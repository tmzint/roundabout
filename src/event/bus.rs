use crate::event::buffer::{EventBuffer, EventHeader};
use crate::event::vec::EventVec;
use crate::event::{EventRegistry, PaddingEvent};
use crate::util::CacheLineAligned;
use crate::wait::WaitingStrategy;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct EventBus {
    buffer: EventBuffer,
    write: CacheLineAligned<AtomicUsize>,
    reads: Vec<Arc<CacheLineAligned<AtomicUsize>>>,
}

impl EventBus {
    #[inline]
    pub fn bounded(
        registry: EventRegistry,
        min_cap: usize,
        receiver_count: usize,
        waiting_strategy: WaitingStrategy,
    ) -> (EventBusSender, Vec<EventBusReceiver>) {
        let mut bus = Self::with_capacity(registry.event_size().inner(), min_cap);
        bus.reads = (0..receiver_count)
            .map(|_| Arc::new(AtomicUsize::new(0).into()))
            .collect();
        let bus = Arc::new(bus);

        let cache_line_padding =
            Self::cache_line_padding(registry.event_size().inner(), bus.buffer.cap());
        let mut receivers = Vec::with_capacity(receiver_count);
        for i in 0..receiver_count {
            let receiver = EventBusReceiver::new(
                bus.reads[i].clone(),
                bus.clone(),
                cache_line_padding,
                waiting_strategy,
            );
            receivers.push(receiver);
        }

        let sender = EventBusSender::new(registry, bus, cache_line_padding, waiting_strategy);

        (sender, receivers)
    }

    fn cache_line_padding(event_size: usize, cap: usize) -> usize {
        const CACHE_LINE: f32 = 64.0 * 4.0;

        // we explicitly reserve distance between read and write to avoid false sharing
        let event_tail_padding = (CACHE_LINE / event_size as f32).ceil().max(1.0);
        let event_padding_ratio = (1.0 - (event_tail_padding as f32 / cap as f32))
            .min(1.0)
            .max(0.0);
        let scaled_event_tail_padding = event_tail_padding as f32 * event_padding_ratio;

        let header_tail_padding = (CACHE_LINE / std::mem::size_of::<EventHeader>() as f32)
            .ceil()
            .max(1.0);
        let header_padding_ratio = (1.0 - (header_tail_padding as f32 / cap as f32))
            .min(1.0)
            .max(0.0);
        let scaled_header_tail_padding = header_tail_padding as f32 * header_padding_ratio;

        scaled_event_tail_padding
            .max(scaled_header_tail_padding)
            .ceil() as usize
    }

    fn with_capacity(event_size: usize, min_cap: usize) -> Self {
        let cap = min_cap.next_power_of_two();

        unsafe {
            let buffer = EventBuffer::with_capacity(event_size, cap);
            // init the buffer with events that can be dropped so that we can unconditional drops
            // before writing and on dropping of the bus.
            for i in 0..cap {
                let header = buffer.get_header(i);
                header.write(EventHeader {
                    e_idx: PaddingEvent::EVENT_INDEX,
                    drop_fn: None,
                });
            }

            Self {
                buffer,
                write: AtomicUsize::new(0).into(),
                reads: Default::default(),
            }
        }
    }

    fn normalize(&self, cursor: usize) -> usize {
        cursor & (self.buffer.cap() - 1)
    }

    fn min_read(&self, write: usize, ordering: Ordering) -> Option<usize> {
        self.reads
            .iter()
            .filter(|r| Arc::strong_count(r) > 1)
            .map(|r| r.load(ordering))
            .max_by_key(|r| write.wrapping_sub(*r))
    }
}

impl Drop for EventBus {
    fn drop(&mut self) {
        unsafe {
            if self.buffer.cap() == 0 {
                return;
            }

            // on creation of the buffer we fill the buffer with no drop events so we can just drop all
            for i in 0..self.buffer.cap() {
                self.buffer.drop_event(i);
            }

            self.buffer.dealloc();
        }
    }
}

pub struct EventBusView<'a> {
    read: &'a AtomicUsize,
    header: &'a EventHeader,
    data: *const u8,
    commit_read: usize,
}

impl<'a> EventBusView<'a> {
    #[inline]
    pub fn event_idx(&self) -> usize {
        self.header.e_idx
    }

    #[inline]
    pub unsafe fn data(&self) -> *const u8 {
        self.data
    }
}

impl<'a> Drop for EventBusView<'a> {
    #[inline]
    fn drop(&mut self) {
        self.read.store(self.commit_read, Ordering::Release);
    }
}

pub struct EventBusReceiver {
    read: Arc<CacheLineAligned<AtomicUsize>>,
    bus: Arc<EventBus>,
    read_cache: usize,
    write_cache: usize,
    cache_line_padding: usize,
    waiting_strategy: WaitingStrategy,
}

impl EventBusReceiver {
    fn new(
        read: Arc<CacheLineAligned<AtomicUsize>>,
        bus: Arc<EventBus>,
        cache_line_padding: usize,
        waiting_strategy: WaitingStrategy,
    ) -> Self {
        Self {
            read,
            bus,
            read_cache: 0,
            write_cache: 0,
            cache_line_padding,
            waiting_strategy,
        }
    }

    #[inline]
    pub fn recv(&mut self) -> EventBusView {
        // TODO: return on drop of sender + all read
        unsafe {
            if self.write_cache.wrapping_sub(self.read_cache) <= self.cache_line_padding {
                let mut waiter = self.waiting_strategy.waiter();
                loop {
                    self.write_cache = self.bus.write.load(Ordering::Relaxed);
                    if self.write_cache.wrapping_sub(self.read_cache) > self.cache_line_padding {
                        break;
                    }

                    waiter.wait();
                }
            }

            self.recv_unchecked()
        }
    }

    #[inline]
    pub fn try_recv(&mut self) -> Option<EventBusView> {
        // TODO: error on drop of sender + all read
        unsafe {
            if self.write_cache.wrapping_sub(self.read_cache) <= self.cache_line_padding {
                self.write_cache = self.bus.write.load(Ordering::Relaxed);
            }

            if self.write_cache.wrapping_sub(self.read_cache) > self.cache_line_padding {
                Some(self.recv_unchecked())
            } else {
                None
            }
        }
    }

    unsafe fn recv_unchecked(&mut self) -> EventBusView {
        let normalized_read_cache = self.bus.normalize(self.read_cache);
        self.read_cache = self.read_cache.wrapping_add(1);
        let header = &*self.bus.buffer.get_header(normalized_read_cache);
        let data = self.bus.buffer.get_event(normalized_read_cache);
        EventBusView {
            read: &self.read,
            header,
            data,
            commit_read: self.read_cache,
        }
    }
}

unsafe impl Send for EventBusReceiver {}

pub struct EventBusSender {
    registry: EventRegistry,
    bus: Arc<EventBus>,
    read_cache: usize,
    write_cache: usize,
    cache_line_padding: usize,
    waiting_strategy: WaitingStrategy,
    padding_events_cache: Option<EventVec>,
}

impl EventBusSender {
    fn new(
        registry: EventRegistry,
        bus: Arc<EventBus>,
        cache_line_padding: usize,
        waiting_strategy: WaitingStrategy,
    ) -> Self {
        let mut padding_events_cache =
            EventVec::with_capacity(registry.clone(), cache_line_padding);
        for _ in 0..cache_line_padding {
            padding_events_cache.push(PaddingEvent);
        }

        Self {
            registry,
            bus,
            read_cache: 0,
            write_cache: 0,
            cache_line_padding,
            waiting_strategy,
            padding_events_cache: Some(padding_events_cache),
        }
    }

    #[inline]
    pub fn buffer(&self) -> EventVec {
        EventVec::new(self.registry.clone())
    }

    #[inline]
    pub fn buffer_with_capacity(&self, cap: usize) -> EventVec {
        EventVec::with_capacity(self.registry.clone(), cap)
    }

    #[inline]
    pub fn send_all(&mut self, vec: &mut EventVec) {
        unsafe {
            assert_eq!(&self.registry, vec.get_registry());
            self.send_all_unchecked(vec);
        }
    }

    #[inline]
    pub unsafe fn send_all_unchecked(&mut self, vec: &mut EventVec) {
        debug_assert_eq!(&self.registry, vec.get_registry());
        if vec.is_empty() {
            return;
        }

        let mut remaining = vec.len();
        loop {
            let normalized_write = self.bus.normalize(self.write_cache);
            let at = vec.len() - remaining;

            let ring_remaining = self.bus.buffer.cap() - normalized_write;
            match ring_remaining {
                _ if remaining == 0 => {
                    break;
                }
                0 => {
                    self.send_batch(vec, at, remaining);
                    break;
                }
                batch_size if remaining <= batch_size => {
                    self.send_batch(vec, at, remaining);
                    break;
                }
                batch_size => {
                    self.send_batch(vec, at, batch_size);
                    remaining -= batch_size;
                }
            }
        }

        vec.set_len(0);
    }

    unsafe fn send_batch(&mut self, vec: &mut EventVec, at: usize, batch_size: usize) {
        let normalized_write = self.bus.normalize(self.write_cache);

        // GC
        self.wait_for_capacity(batch_size + self.cache_line_padding);
        for i in 0..batch_size {
            self.bus
                .buffer
                .drop_event(self.bus.normalize(self.write_cache.wrapping_add(i)));
        }

        let vec_buffer = vec.get_buffer();
        let headers = vec_buffer.get_header(at);
        let events = vec_buffer.get_event(at);
        self.bus
            .buffer
            .copy_nonoverlapping_all(normalized_write, headers, events, batch_size);

        let new_write_cache = self.write_cache.wrapping_add(batch_size);
        self.bus.write.store(new_write_cache, Ordering::Release);
        self.write_cache = new_write_cache;
    }

    #[inline]
    pub fn send<T: 'static + Send + Sync>(&mut self, event: T) {
        unsafe {
            // Optimization: static resolution of e_idx
            match self.registry.get_index_of::<T>() {
                Some(e_idx) => {
                    let event = ManuallyDrop::new(event);
                    let drop_fn: Option<fn(*mut u8)> = if std::mem::needs_drop::<T>() {
                        Some(|ptr| (ptr as *mut T).drop_in_place())
                    } else {
                        None
                    };
                    let data = event.deref() as *const T as *const u8;

                    self.send_nonoverlapping(e_idx, data, std::mem::size_of::<T>(), drop_fn);
                }
                None => {
                    log::warn!(
                        "skipping sending of unhandled event type: {}",
                        std::any::type_name::<T>()
                    );
                }
            }
        }
    }

    #[inline]
    pub unsafe fn send_nonoverlapping(
        &mut self,
        e_idx: usize,
        data: *const u8,
        data_len: usize,
        drop_fn: Option<fn(*mut u8)>,
    ) {
        let header = EventHeader { e_idx, drop_fn };
        let normalized_write = self.bus.normalize(self.write_cache);

        // GC
        self.wait_for_capacity(1 + self.cache_line_padding);
        self.bus.buffer.drop_event(normalized_write);

        self.bus
            .buffer
            .copy_nonoverlapping(normalized_write, header, data, data_len);

        let new_write_cache = self.write_cache.wrapping_add(1);
        self.bus.write.store(new_write_cache, Ordering::Release);
        self.write_cache = new_write_cache;
    }

    #[inline]
    pub fn flush_padding(&mut self) {
        unsafe {
            let mut padding_events_cache = self.padding_events_cache.take().unwrap_unchecked();
            self.send_all_unchecked(&mut padding_events_cache);
            padding_events_cache.set_len(self.cache_line_padding);
            self.padding_events_cache = Some(padding_events_cache);
        }
    }

    fn wait_for_capacity(&mut self, len: usize) {
        if self.bus.buffer.cap() < self.write_cache.wrapping_sub(self.read_cache) + len {
            let mut waiter = self.waiting_strategy.waiter();
            loop {
                self.read_cache = self
                    .bus
                    .min_read(self.write_cache, Ordering::Relaxed)
                    .unwrap_or_else(|| self.write_cache);

                if self.bus.buffer.cap() >= self.write_cache.wrapping_sub(self.read_cache) + len {
                    break;
                }

                waiter.wait();
            }
        }
    }
}

unsafe impl Send for EventBusSender {}

#[cfg(test)]
mod tests {
    use super::EventBus;
    use crate::event::EventRegistry;
    use crate::wait::WaitingStrategy;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    struct FooEvent(Arc<()>, u128);
    struct BarEvent(Arc<()>, bool);

    // TODO: test send all

    #[test]
    fn tail_padding_event_bus_sender() {
        let mut registry = EventRegistry::new();
        registry.register_of::<FooEvent>();
        registry.register_of::<BarEvent>();

        assert_eq!(registry.event_size().inner(), 24);

        let (sender, _) = EventBus::bounded(registry.clone(), 0, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 0);

        let (sender, _) = EventBus::bounded(registry.clone(), 8, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 0);

        let (sender, _) = EventBus::bounded(registry.clone(), 16, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 4);

        let (sender, _) = EventBus::bounded(registry.clone(), 64, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 12);

        let (sender, _) = EventBus::bounded(registry.clone(), 512, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 16);

        let (sender, _) = EventBus::bounded(registry.clone(), 2048, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 16);
    }

    #[test]
    fn empty_event_bus() {
        let mut registry = EventRegistry::new();
        registry.register_of::<FooEvent>();
        registry.register_of::<BarEvent>();

        let (sender, mut receivers) = EventBus::bounded(registry, 4, 1, WaitingStrategy::default());

        assert_eq!(receivers.len(), 1);

        let mut receiver = receivers.remove(0);

        assert_eq!(sender.read_cache, 0);
        assert_eq!(sender.write_cache, 0);

        assert_eq!(sender.bus.reads.len(), 1);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 0);
        assert_eq!(sender.bus.buffer.cap(), 4);

        assert_eq!(receiver.write_cache, 0);
        assert_eq!(receiver.read_cache, 0);
        assert!(receiver.try_recv().is_none());
        assert_eq!(receiver.bus.reads[0].load(Ordering::SeqCst), 0);
    }

    #[test]
    fn sequential_event_bus() {
        let mut registry = EventRegistry::new();
        let foo_idx = registry.register_of::<FooEvent>();
        let bar_idx = registry.register_of::<BarEvent>();

        let (mut sender, mut receivers) =
            EventBus::bounded(registry, 4, 1, WaitingStrategy::default());
        let mut receiver = receivers.remove(0);

        let foo_arc = Arc::new(());
        let bar_arc = Arc::new(());

        sender.send(FooEvent(foo_arc.clone(), 1));
        sender.send(BarEvent(bar_arc.clone(), false));
        sender.send(FooEvent(foo_arc.clone(), 2));

        assert_eq!(sender.write_cache, 3);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 3);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 2);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 0);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let event = recv.unwrap();
            assert_eq!(event.header.e_idx, foo_idx);

            let foo = unsafe { &*(event.data as *const FooEvent) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 1);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 1);

        sender.send(BarEvent(bar_arc.clone(), true));
        assert_eq!(sender.write_cache, 4);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 4);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 1);
            assert_eq!(Arc::strong_count(&bar_arc), 3);

            let event = recv.unwrap();
            assert_eq!(event.header.e_idx, bar_idx);

            let bar = unsafe { &*(event.data as *const BarEvent) };
            assert!(Arc::ptr_eq(&bar.0, &bar_arc));
            assert_eq!(bar.1, false);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 2);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 2);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let event = recv.unwrap();
            assert_eq!(event.header.e_idx, foo_idx);

            let foo = unsafe { &*(event.data as *const FooEvent) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 2);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 3);
            assert_eq!(Arc::strong_count(&bar_arc), 3);

            let event = recv.unwrap();
            assert_eq!(event.header.e_idx, bar_idx);

            let bar = unsafe { &*(event.data as *const BarEvent) };
            assert!(Arc::ptr_eq(&bar.0, &bar_arc));
            assert_eq!(bar.1, true);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 4);

        sender.send(FooEvent(foo_arc.clone(), 3));
        assert_eq!(sender.write_cache, 5);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 5);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 4);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let event = recv.unwrap();
            assert_eq!(event.header.e_idx, foo_idx);

            let foo = unsafe { &*(event.data as *const FooEvent) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 3);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 5);
        assert!(receiver.try_recv().is_none());
    }

    #[test]
    fn concurrent_event_bus() {
        let mut registry = EventRegistry::new();
        let foo_idx = registry.register_of::<FooEvent>();
        let bar_idx = registry.register_of::<BarEvent>();

        let (mut sender, mut receivers) =
            EventBus::bounded(registry, 4, 1, WaitingStrategy::default());
        let mut receiver = receivers.remove(0);

        let message_count = 300usize;

        let foo_arc = Arc::new(());
        let bar_arc = Arc::new(());

        let recv_foo_arc = foo_arc.clone();
        let recv_bar_arc = bar_arc.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..message_count {
                let event = receiver.recv();
                if i % 3 == 0 {
                    assert_eq!(event.header.e_idx, bar_idx);

                    let bar = unsafe { &*(event.data as *const BarEvent) };
                    assert!(Arc::ptr_eq(&bar.0, &recv_bar_arc));
                    assert_eq!(bar.1, i % 2 == 0);
                } else {
                    assert_eq!(event.header.e_idx, foo_idx);

                    let foo = unsafe { &*(event.data as *const FooEvent) };
                    assert!(Arc::ptr_eq(&foo.0, &recv_foo_arc));
                    assert_eq!(foo.1 as usize, i as usize);
                }

                // deterministic semi random waiting
                std::thread::sleep(Duration::from_micros(
                    (i % 2 + i % 3 + i % 5 + i % 7 + i % 9) as u64,
                ))
            }
        });

        for i in 0..message_count {
            if i % 3 == 0 {
                sender.send(BarEvent(bar_arc.clone(), i % 2 == 0));
            } else {
                sender.send(FooEvent(foo_arc.clone(), i as u128));
            }
        }

        sender.flush_padding();
        handle.join().unwrap();

        assert_eq!(Arc::strong_count(&foo_arc), 4);
        assert_eq!(Arc::strong_count(&bar_arc), 2);
    }

    #[test]
    fn drop_event_bus() {
        let mut registry = EventRegistry::new();
        registry.register_of::<FooEvent>();
        registry.register_of::<BarEvent>();

        let (mut sender, mut receivers) =
            EventBus::bounded(registry, 3, 1, WaitingStrategy::default());
        let receiver = receivers.remove(0);

        let arc = Arc::new(());

        sender.send(FooEvent(arc.clone(), 1));
        assert_eq!(Arc::strong_count(&arc), 2);

        std::mem::drop(sender);
        std::mem::drop(receiver);
        assert_eq!(Arc::strong_count(&arc), 1);
    }
}
