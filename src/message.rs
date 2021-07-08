mod buffer;
pub mod bus;
pub mod vec;

use crate::message::vec::MessageVec;
use crate::util::triple::TripleBufferedHead;
use crate::util::IndexSet;
use std::alloc::Layout;
use std::any::TypeId;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Deref;

// TODO:
//  const message index POC
//  should also work with once cell and lazy, would be slower but more safe and support more OS (e.g. Mac, Non Unixes/windows)
// #[dynamic(10)]
// static GLOBAL_MESSAGE_COUNTER: AtomicUsize = AtomicUsize::new(0);
//
// ------------------------------------------------------------------------------
// GENERATED:
// ------------------------------------------------------------------------------
// // Unix and Windows support only, best performance
// #[dynamic(0)]
// static mut ID_MESSAGE_INDEX: usize = unsafe { GLOBAL_MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed) };
//
// pub trait Message: Sized + Send + Sync + 'static {
//     #[inline]
//     unsafe fn index() -> usize {
//         *ID_MESSAGE_INDEX
//     }
// }

pub struct PaddingMessage;

impl PaddingMessage {
    pub(crate) const MESSAGE_INDEX: usize = 0;
}
static_assertions::assert_eq_size!(PaddingMessage, ());

pub struct ShutdownCommand(PhantomData<()>);

impl ShutdownCommand {
    pub(crate) const MESSAGE_INDEX: usize = 1;

    pub(crate) fn new() -> Self {
        // non public constructable shutdown message
        Self(Default::default())
    }
}

static_assertions::assert_eq_size!(ShutdownCommand, ());

pub struct ShutdownRequestedEvent(PhantomData<()>);

impl ShutdownRequestedEvent {
    pub(crate) fn new() -> Self {
        // non public constructable shutdown requested message
        Self(Default::default())
    }
}

static_assertions::assert_eq_size!(ShutdownRequestedEvent, ());

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MessageRegistry {
    // TODO: const message lookup
    //  don't use TypeId as it is unsound and may collide
    //  -> use a proc macro to generated an associated uuid, maybe it is possible to
    //  even get better performance with the capabilities of proc macros during dispatching?
    //  see https://github.com/rust-lang/rust/issues/10389
    message_index_set: IndexSet<TypeId>,

    // TODO: const message lookup
    // message_lkp_tbl: Vec<Option<usize>>,
    message_size: MessageSize,
}

impl MessageRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn get_index_of<E: 'static + Send + Sync>(&self) -> Option<usize> {
        self.get_index(TypeId::of::<E>())
    }

    #[inline]
    pub fn get_index(&self, tid: TypeId) -> Option<usize> {
        self.message_index_set.get_index_of(&tid)
    }

    #[inline]
    pub fn register_of<E: 'static + Send + Sync>(&mut self) -> usize {
        self.message_size = self.message_size.max(MessageSize::of::<E>());
        self.message_index_set.insert_full(TypeId::of::<E>()).0
    }

    pub(crate) fn register_all<I: Iterator<Item = TypeId>>(
        &mut self,
        tids: I,
        max_message_size: MessageSize,
    ) -> &mut Self {
        for tid in tids {
            self.message_index_set.insert(tid);
        }
        self.message_size = self.message_size.max(max_message_size);

        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.message_index_set.len()
    }

    #[inline]
    pub fn message_size(&self) -> MessageSize {
        self.message_size
    }
}

impl Default for MessageRegistry {
    fn default() -> Self {
        let mut registry = Self {
            message_index_set: Default::default(),
            message_size: Default::default(),
        };

        let e_idx = registry.register_of::<PaddingMessage>();
        assert_eq!(e_idx, PaddingMessage::MESSAGE_INDEX);

        let e_idx = registry.register_of::<ShutdownCommand>();
        assert_eq!(e_idx, ShutdownCommand::MESSAGE_INDEX);

        registry
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct MessageSize(usize);

impl MessageSize {
    pub const MESSAGE_ALIGN: usize = std::mem::align_of::<usize>();

    #[inline(always)]
    pub(crate) fn of<T: 'static + Sized + Send + Sync>() -> Self {
        // TODO:
        //  Make this a compile time check,
        //  once static assertions can support generic parameters from the outer function
        //  see:
        //      https://github.com/rust-lang/rfcs/issues/2790,
        //      https://github.com/nvzqz/static-assertions-rs/issues/21
        assert_eq!(std::mem::align_of::<usize>() % std::mem::align_of::<T>(), 0);
        Self(Self::for_size(std::mem::size_of::<T>()))
    }

    #[inline(always)]
    pub(crate) fn for_size(message_size: usize) -> usize {
        let alignments = (message_size / Self::MESSAGE_ALIGN)
            + if message_size % Self::MESSAGE_ALIGN == 0 {
                0
            } else {
                1
            };

        alignments * Self::MESSAGE_ALIGN
    }

    #[inline]
    pub fn inner(self) -> usize {
        self.0
    }
}

impl Deref for MessageSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Into<usize> for MessageSize {
    fn into(self) -> usize {
        self.0
    }
}

impl Default for MessageSize {
    fn default() -> Self {
        Self(Self::MESSAGE_ALIGN)
    }
}

pub struct UntypedMessage {
    e_idx: usize,
    tid: TypeId,
    data: *mut u8,
    data_size: usize,
    drop_fn: Option<fn(*mut u8)>,
}

impl UntypedMessage {
    pub(crate) unsafe fn new<E: 'static + Send + Sync>(e_idx: usize, message: E) -> Self {
        let data_len = MessageSize::of::<E>().inner();
        let layout = Self::layout(data_len);
        let ptr = std::alloc::alloc(layout) as *mut E;
        ptr.write(message);
        let drop_fn: Option<fn(*mut u8)> = if std::mem::needs_drop::<E>() {
            Some(|ptr| (ptr as *mut E).drop_in_place())
        } else {
            None
        };

        Self {
            e_idx,
            tid: TypeId::of::<E>(),
            data: ptr as *mut u8,
            data_size: data_len,
            drop_fn,
        }
    }

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, MessageSize::MESSAGE_ALIGN).unwrap()
    }
}

impl Drop for UntypedMessage {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if let Some(drop) = self.drop_fn {
                (drop)(self.data as *mut u8);
            }
            std::alloc::dealloc(self.data as *mut u8, Self::layout(self.data_size));
        }
    }
}

unsafe impl Send for UntypedMessage {}
unsafe impl Sync for UntypedMessage {}

pub struct MessageSender {
    head: TripleBufferedHead<MessageVec>,
    // Don't auto impl send and sync as TripleBufferedHead should be thread specific
    _pd: PhantomData<*mut u8>,
}

static_assertions::assert_not_impl_any!(MessageSender: Send, Sync);

impl MessageSender {
    pub fn new(head: TripleBufferedHead<MessageVec>) -> Self {
        Self {
            head,
            _pd: Default::default(),
        }
    }

    #[inline]
    pub fn send<E: 'static + Send + Sync>(&self, message: E) -> bool {
        let send = self.head.write().push(message);
        if !send {
            log::debug!(
                "skipping sending of unhandled message type: {}",
                std::any::type_name::<E>()
            );
        }

        send
    }

    #[inline]
    pub fn send_iter<I: IntoIterator<Item = E>, E: 'static + Send + Sync>(
        &self,
        messages: I,
    ) -> bool {
        let send = self.head.write().extend(messages);
        if !send {
            log::debug!(
                "skipping sending of unhandled message type: {}",
                std::any::type_name::<E>()
            );
        }

        send
    }

    #[inline]
    pub fn send_all(&self, messages: &mut MessageVec) {
        self.head.write().extend_vec(messages);
    }

    #[inline]
    pub fn buffer(&self) -> MessageVec {
        let registry = self.head.write().get_registry().clone();

        MessageVec::new(registry)
    }

    #[inline]
    pub fn prepare<E: 'static + Send + Sync>(&self, message: E) -> Option<UntypedMessage> {
        unsafe {
            // Optimization: static resolution of e_idx
            self.head
                .write()
                .get_registry()
                .get_index_of::<E>()
                .map(|e_idx| UntypedMessage::new(e_idx, message))
        }
    }

    #[inline]
    pub fn send_untyped(&self, mut message: UntypedMessage) {
        unsafe {
            let mut head = self.head.write();
            match head.get_registry().get_index(message.tid) {
                Some(e_idx) if e_idx == message.e_idx => {
                    head.push_untyped(
                        e_idx,
                        message.data,
                        message.data_size,
                        message.drop_fn.take(),
                    );
                }
                _ => {
                    panic!("untyped message is incompatible with message registry");
                }
            }
        }
    }
}

impl std::fmt::Debug for MessageSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("MessageSender")
    }
}

impl Clone for MessageSender {
    fn clone(&self) -> Self {
        Self {
            head: self.head.clone(),
            _pd: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::MessageSize;

    #[test]
    fn aligned_size_of() {
        assert_eq!(
            MessageSize::of::<[u8; 1]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
        assert_eq!(
            MessageSize::of::<[u8; 3]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
        assert_eq!(
            MessageSize::of::<[u8; 7]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
        assert_eq!(
            MessageSize::of::<[u8; 15]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
        assert_eq!(
            MessageSize::of::<[u8; 31]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
        assert_eq!(
            MessageSize::of::<[u8; 63]>().inner() % MessageSize::MESSAGE_ALIGN,
            0
        );
    }
}
