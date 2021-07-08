use crate::message::buffer::{MessageBuffer, MessageHeader};
use crate::message::vec::MessageVec;
use crate::message::{MessageRegistry, PaddingMessage};
use crate::util::CacheLineAligned;
use crate::wait::WaitingStrategy;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct MessageBus {
    buffer: MessageBuffer,
    write: CacheLineAligned<AtomicUsize>,
    reads: Vec<Arc<CacheLineAligned<AtomicUsize>>>,
}

impl MessageBus {
    #[inline]
    pub fn bounded(
        registry: MessageRegistry,
        min_cap: usize,
        receiver_count: usize,
        waiting_strategy: WaitingStrategy,
    ) -> (MessageBusSender, Vec<MessageBusReceiver>) {
        let mut bus = Self::with_capacity(registry.message_size().inner(), min_cap);
        bus.reads = (0..receiver_count)
            .map(|_| Arc::new(AtomicUsize::new(0).into()))
            .collect();
        let bus = Arc::new(bus);

        let cache_line_padding =
            Self::cache_line_padding(registry.message_size().inner(), bus.buffer.cap());
        let mut receivers = Vec::with_capacity(receiver_count);
        for i in 0..receiver_count {
            let receiver = MessageBusReceiver::new(
                bus.reads[i].clone(),
                bus.clone(),
                cache_line_padding,
                waiting_strategy,
            );
            receivers.push(receiver);
        }

        let sender = MessageBusSender::new(registry, bus, cache_line_padding, waiting_strategy);

        (sender, receivers)
    }

    fn cache_line_padding(message_size: usize, cap: usize) -> usize {
        const CACHE_LINE: f32 = 64.0 * 4.0;

        // we explicitly reserve distance between read and write to avoid false sharing
        let message_tail_padding = (CACHE_LINE / message_size as f32).ceil().max(1.0);
        let message_padding_ratio = (1.0 - (message_tail_padding as f32 / cap as f32))
            .min(1.0)
            .max(0.0);
        let scaled_message_tail_padding = message_tail_padding as f32 * message_padding_ratio;

        let header_tail_padding = (CACHE_LINE / std::mem::size_of::<MessageHeader>() as f32)
            .ceil()
            .max(1.0);
        let header_padding_ratio = (1.0 - (header_tail_padding as f32 / cap as f32))
            .min(1.0)
            .max(0.0);
        let scaled_header_tail_padding = header_tail_padding as f32 * header_padding_ratio;

        scaled_message_tail_padding
            .max(scaled_header_tail_padding)
            .ceil() as usize
    }

    fn with_capacity(message_size: usize, min_cap: usize) -> Self {
        let cap = min_cap.next_power_of_two();

        unsafe {
            let buffer = MessageBuffer::with_capacity(message_size, cap);
            // init the buffer with messages that can be dropped so that we can unconditional drops
            // before writing and on dropping of the bus.
            for i in 0..cap {
                let header = buffer.get_header(i);
                header.write(MessageHeader {
                    e_idx: PaddingMessage::MESSAGE_INDEX,
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

impl Drop for MessageBus {
    fn drop(&mut self) {
        unsafe {
            if self.buffer.cap() == 0 {
                return;
            }

            // on creation of the buffer we fill the buffer with no drop messages so we can just drop all
            for i in 0..self.buffer.cap() {
                self.buffer.drop_message(i);
            }

            self.buffer.dealloc();
        }
    }
}

pub struct MessageBusView<'a> {
    read: &'a AtomicUsize,
    header: &'a MessageHeader,
    data: *const u8,
    commit_read: usize,
}

impl<'a> MessageBusView<'a> {
    #[inline]
    pub fn message_idx(&self) -> usize {
        self.header.e_idx
    }

    #[inline]
    pub unsafe fn data(&self) -> *const u8 {
        self.data
    }
}

impl<'a> Drop for MessageBusView<'a> {
    #[inline]
    fn drop(&mut self) {
        self.read.store(self.commit_read, Ordering::Release);
    }
}

pub struct MessageBusReceiver {
    read: Arc<CacheLineAligned<AtomicUsize>>,
    bus: Arc<MessageBus>,
    read_cache: usize,
    write_cache: usize,
    cache_line_padding: usize,
    waiting_strategy: WaitingStrategy,
}

impl MessageBusReceiver {
    fn new(
        read: Arc<CacheLineAligned<AtomicUsize>>,
        bus: Arc<MessageBus>,
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
    pub fn recv(&mut self) -> MessageBusView {
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
    pub fn try_recv(&mut self) -> Option<MessageBusView> {
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

    unsafe fn recv_unchecked(&mut self) -> MessageBusView {
        let normalized_read_cache = self.bus.normalize(self.read_cache);
        self.read_cache = self.read_cache.wrapping_add(1);
        let header = &*self.bus.buffer.get_header(normalized_read_cache);
        let data = self.bus.buffer.get_message(normalized_read_cache);
        MessageBusView {
            read: &self.read,
            header,
            data,
            commit_read: self.read_cache,
        }
    }
}

unsafe impl Send for MessageBusReceiver {}

pub struct MessageBusSender {
    registry: MessageRegistry,
    bus: Arc<MessageBus>,
    read_cache: usize,
    write_cache: usize,
    cache_line_padding: usize,
    waiting_strategy: WaitingStrategy,
    padding_messages_cache: Option<MessageVec>,
}

impl MessageBusSender {
    fn new(
        registry: MessageRegistry,
        bus: Arc<MessageBus>,
        cache_line_padding: usize,
        waiting_strategy: WaitingStrategy,
    ) -> Self {
        let mut padding_messages_cache =
            MessageVec::with_capacity(registry.clone(), cache_line_padding);
        for _ in 0..cache_line_padding {
            padding_messages_cache.push(PaddingMessage);
        }

        Self {
            registry,
            bus,
            read_cache: 0,
            write_cache: 0,
            cache_line_padding,
            waiting_strategy,
            padding_messages_cache: Some(padding_messages_cache),
        }
    }

    #[inline]
    pub fn buffer(&self) -> MessageVec {
        MessageVec::new(self.registry.clone())
    }

    #[inline]
    pub fn buffer_with_capacity(&self, cap: usize) -> MessageVec {
        MessageVec::with_capacity(self.registry.clone(), cap)
    }

    #[inline]
    pub fn send_all(&mut self, vec: &mut MessageVec) {
        unsafe {
            assert_eq!(&self.registry, vec.get_registry());
            self.send_all_unchecked(vec);
        }
    }

    #[inline]
    pub unsafe fn send_all_unchecked(&mut self, vec: &mut MessageVec) {
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

    unsafe fn send_batch(&mut self, vec: &mut MessageVec, at: usize, batch_size: usize) {
        let normalized_write = self.bus.normalize(self.write_cache);

        // GC
        self.wait_for_capacity(batch_size + self.cache_line_padding);
        for i in 0..batch_size {
            self.bus
                .buffer
                .drop_message(self.bus.normalize(self.write_cache.wrapping_add(i)));
        }

        let vec_buffer = vec.get_buffer();
        let headers = vec_buffer.get_header(at);
        let messages = vec_buffer.get_message(at);
        self.bus
            .buffer
            .copy_nonoverlapping_all(normalized_write, headers, messages, batch_size);

        let new_write_cache = self.write_cache.wrapping_add(batch_size);
        self.bus.write.store(new_write_cache, Ordering::Release);
        self.write_cache = new_write_cache;
    }

    #[inline]
    pub fn send<T: 'static + Send + Sync>(&mut self, message: T) {
        unsafe {
            // Optimization: static resolution of e_idx
            match self.registry.get_index_of::<T>() {
                Some(e_idx) => {
                    let message = ManuallyDrop::new(message);
                    let drop_fn: Option<fn(*mut u8)> = if std::mem::needs_drop::<T>() {
                        Some(|ptr| (ptr as *mut T).drop_in_place())
                    } else {
                        None
                    };
                    let data = message.deref() as *const T as *const u8;

                    self.send_nonoverlapping(e_idx, data, std::mem::size_of::<T>(), drop_fn);
                }
                None => {
                    log::debug!(
                        "skipping sending of unhandled message type: {}",
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
        let header = MessageHeader { e_idx, drop_fn };
        let normalized_write = self.bus.normalize(self.write_cache);

        // GC
        self.wait_for_capacity(1 + self.cache_line_padding);
        self.bus.buffer.drop_message(normalized_write);

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
            // Optimization: use unwrap_unchecked (micro)
            let mut padding_messages_cache = self.padding_messages_cache.take().unwrap();
            self.send_all_unchecked(&mut padding_messages_cache);
            padding_messages_cache.set_len(self.cache_line_padding);
            self.padding_messages_cache = Some(padding_messages_cache);
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

unsafe impl Send for MessageBusSender {}

#[cfg(test)]
mod tests {
    use super::MessageBus;
    use crate::message::MessageRegistry;
    use crate::wait::WaitingStrategy;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    struct FooMessage(Arc<()>, u128);
    struct BarMessage(Arc<()>, bool);

    // TODO: test send all

    #[test]
    fn tail_padding_message_bus_sender() {
        let mut registry = MessageRegistry::new();
        registry.register_of::<FooMessage>();
        registry.register_of::<BarMessage>();

        assert_eq!(registry.message_size().inner(), 24);

        let (sender, _) = MessageBus::bounded(registry.clone(), 0, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 0);

        let (sender, _) = MessageBus::bounded(registry.clone(), 8, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 0);

        let (sender, _) = MessageBus::bounded(registry.clone(), 16, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 4);

        let (sender, _) = MessageBus::bounded(registry.clone(), 64, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 12);

        let (sender, _) = MessageBus::bounded(registry.clone(), 512, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 16);

        let (sender, _) =
            MessageBus::bounded(registry.clone(), 2048, 0, WaitingStrategy::default());
        assert_eq!(sender.cache_line_padding, 16);
    }

    #[test]
    fn empty_message_bus() {
        let mut registry = MessageRegistry::new();
        registry.register_of::<FooMessage>();
        registry.register_of::<BarMessage>();

        let (sender, mut receivers) =
            MessageBus::bounded(registry, 4, 1, WaitingStrategy::default());

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
    fn sequential_message_bus() {
        let mut registry = MessageRegistry::new();
        let foo_idx = registry.register_of::<FooMessage>();
        let bar_idx = registry.register_of::<BarMessage>();

        let (mut sender, mut receivers) =
            MessageBus::bounded(registry, 4, 1, WaitingStrategy::default());
        let mut receiver = receivers.remove(0);

        let foo_arc = Arc::new(());
        let bar_arc = Arc::new(());

        sender.send(FooMessage(foo_arc.clone(), 1));
        sender.send(BarMessage(bar_arc.clone(), false));
        sender.send(FooMessage(foo_arc.clone(), 2));

        assert_eq!(sender.write_cache, 3);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 3);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 2);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 0);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let message = recv.unwrap();
            assert_eq!(message.header.e_idx, foo_idx);

            let foo = unsafe { &*(message.data as *const FooMessage) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 1);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 1);

        sender.send(BarMessage(bar_arc.clone(), true));
        assert_eq!(sender.write_cache, 4);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 4);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 1);
            assert_eq!(Arc::strong_count(&bar_arc), 3);

            let message = recv.unwrap();
            assert_eq!(message.header.e_idx, bar_idx);

            let bar = unsafe { &*(message.data as *const BarMessage) };
            assert!(Arc::ptr_eq(&bar.0, &bar_arc));
            assert_eq!(bar.1, false);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 2);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 2);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let message = recv.unwrap();
            assert_eq!(message.header.e_idx, foo_idx);

            let foo = unsafe { &*(message.data as *const FooMessage) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 2);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 3);
            assert_eq!(Arc::strong_count(&bar_arc), 3);

            let message = recv.unwrap();
            assert_eq!(message.header.e_idx, bar_idx);

            let bar = unsafe { &*(message.data as *const BarMessage) };
            assert!(Arc::ptr_eq(&bar.0, &bar_arc));
            assert_eq!(bar.1, true);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 4);

        sender.send(FooMessage(foo_arc.clone(), 3));
        assert_eq!(sender.write_cache, 5);
        assert_eq!(sender.bus.write.load(Ordering::SeqCst), 5);
        assert_eq!(Arc::strong_count(&foo_arc), 3);
        assert_eq!(Arc::strong_count(&bar_arc), 3);

        {
            let recv = receiver.try_recv();
            assert!(recv.is_some());
            assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 4);
            assert_eq!(Arc::strong_count(&foo_arc), 3);

            let message = recv.unwrap();
            assert_eq!(message.header.e_idx, foo_idx);

            let foo = unsafe { &*(message.data as *const FooMessage) };
            assert!(Arc::ptr_eq(&foo.0, &foo_arc));
            assert_eq!(foo.1, 3);
        }
        assert_eq!(sender.bus.reads[0].load(Ordering::SeqCst), 5);
        assert!(receiver.try_recv().is_none());
    }

    #[test]
    fn concurrent_message_bus() {
        let mut registry = MessageRegistry::new();
        let foo_idx = registry.register_of::<FooMessage>();
        let bar_idx = registry.register_of::<BarMessage>();

        let (mut sender, mut receivers) =
            MessageBus::bounded(registry, 4, 1, WaitingStrategy::default());
        let mut receiver = receivers.remove(0);

        let message_count = 300usize;

        let foo_arc = Arc::new(());
        let bar_arc = Arc::new(());

        let recv_foo_arc = foo_arc.clone();
        let recv_bar_arc = bar_arc.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..message_count {
                let message = receiver.recv();
                if i % 3 == 0 {
                    assert_eq!(message.header.e_idx, bar_idx);

                    let bar = unsafe { &*(message.data as *const BarMessage) };
                    assert!(Arc::ptr_eq(&bar.0, &recv_bar_arc));
                    assert_eq!(bar.1, i % 2 == 0);
                } else {
                    assert_eq!(message.header.e_idx, foo_idx);

                    let foo = unsafe { &*(message.data as *const FooMessage) };
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
                sender.send(BarMessage(bar_arc.clone(), i % 2 == 0));
            } else {
                sender.send(FooMessage(foo_arc.clone(), i as u128));
            }
        }

        sender.flush_padding();
        handle.join().unwrap();

        assert_eq!(Arc::strong_count(&foo_arc), 4);
        assert_eq!(Arc::strong_count(&bar_arc), 2);
    }

    #[test]
    fn drop_message_bus() {
        let mut registry = MessageRegistry::new();
        registry.register_of::<FooMessage>();
        registry.register_of::<BarMessage>();

        let (mut sender, mut receivers) =
            MessageBus::bounded(registry, 3, 1, WaitingStrategy::default());
        let receiver = receivers.remove(0);

        let arc = Arc::new(());

        sender.send(FooMessage(arc.clone(), 1));
        assert_eq!(Arc::strong_count(&arc), 2);

        std::mem::drop(sender);
        std::mem::drop(receiver);
        assert_eq!(Arc::strong_count(&arc), 1);
    }
}
