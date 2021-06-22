use crate::util::CacheLineAligned;
use crate::wait::SpinYieldWait;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Optimization: use a writer atomic instead of mutexes
pub struct TripleBuffered<T> {
    buffers: [CacheLineAligned<Mutex<T>>; 3],
    read: CacheLineAligned<AtomicUsize>,
}

impl<T: Send> TripleBuffered<T> {
    #[inline]
    pub fn new(state: [T; 3]) -> (TripleBufferedHead<T>, TripleBufferedTail<T>) {
        let [first, second, third] = state;
        let buffer_a = Mutex::new(first);
        let buffer_b = Mutex::new(second);
        let buffer_c = Mutex::new(third);

        Self {
            buffers: [buffer_a.into(), buffer_b.into(), buffer_c.into()],
            read: AtomicUsize::new(0).into(),
        }
        .finalize()
    }

    #[inline]
    pub fn new_fn<F: Fn() -> T>(factory: F) -> (TripleBufferedHead<T>, TripleBufferedTail<T>) {
        Self::new([factory(), factory(), factory()])
    }

    fn finalize(self) -> (TripleBufferedHead<T>, TripleBufferedTail<T>) {
        let buffer = Arc::new(self);

        let receiver = TripleBufferedTail {
            buffer: buffer.clone(),
        };

        let sender = TripleBufferedHead { buffer };

        (sender, receiver)
    }
}

impl<T: Send + Default> TripleBuffered<T> {
    #[inline]
    pub fn new_default() -> (TripleBufferedHead<T>, TripleBufferedTail<T>) {
        Self::new([T::default(), T::default(), T::default()])
    }
}

impl<T: Send + Clone> TripleBuffered<T> {
    #[inline]
    pub fn new_clone(initial: T) -> (TripleBufferedHead<T>, TripleBufferedTail<T>) {
        Self::new([initial.clone(), initial.clone(), initial])
    }
}

pub struct TripleBufferedTail<T> {
    buffer: Arc<TripleBuffered<T>>,
}

impl<T> TripleBufferedTail<T> {
    #[inline]
    pub fn advance(&self) -> MutexGuard<RawMutex, T> {
        let i = self.buffer.read.fetch_add(1, Ordering::Relaxed) % 3;
        self.buffer.buffers[i].lock()
    }
}

pub struct TripleBufferedHead<T> {
    buffer: Arc<TripleBuffered<T>>,
}

impl<T> TripleBufferedHead<T> {
    #[inline]
    pub fn write(&self) -> MutexGuard<RawMutex, T> {
        let mut waiter = SpinYieldWait::default();
        loop {
            let i = (self.buffer.read.load(Ordering::Relaxed) + 2) % 3;
            if let Some(t) = self.buffer.buffers[i].try_lock() {
                return t;
            }

            // in general we shouldn't run into this case
            waiter.wait();
        }
    }
}

impl<T> Clone for TripleBufferedHead<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
        }
    }
}
