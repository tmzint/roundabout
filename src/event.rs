mod buffer;
pub mod bus;
pub mod vec;

use crate::event::vec::EventVec;
use crate::util::triple::TripleBufferedHead;
use crate::util::IndexSet;
use std::alloc::Layout;
use std::any::TypeId;
use std::marker::PhantomData;
use std::ops::Deref;

// TODO:
//  const event index POC
//  should also work with once cell and lazy, would be slower but more safe and support more OS (e.g. Mac, Non Unixes/windows)
// #[dynamic(10)]
// static GLOBAL_EVENT_COUNTER: AtomicUsize = AtomicUsize::new(0);
//
// ------------------------------------------------------------------------------
// GENERATED:
// ------------------------------------------------------------------------------
// // Unix and Windows support only, best performance
// #[dynamic(0)]
// static mut ID_EVENT_INDEX: usize = unsafe { GLOBAL_EVENT_COUNTER.fetch_add(1, Ordering::Relaxed) };
//
// pub trait Event: Sized + Send + Sync + 'static {
//     #[inline]
//     unsafe fn index() -> usize {
//         *ID_EVENT_INDEX
//     }
// }

pub struct PaddingEvent;

impl PaddingEvent {
    pub(crate) const EVENT_INDEX: usize = 0;
}
static_assertions::assert_eq_size!(PaddingEvent, ());

pub struct ShutdownEvent;

impl ShutdownEvent {
    pub(crate) const EVENT_INDEX: usize = 1;
}

static_assertions::assert_eq_size!(ShutdownEvent, ());

pub struct ShutdownRequestedEvent(PhantomData<()>);

impl ShutdownRequestedEvent {
    pub(crate) fn new() -> Self {
        // non public constructable shutdown requested event
        Self(Default::default())
    }
}

static_assertions::assert_eq_size!(ShutdownRequestedEvent, ());

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EventRegistry {
    // TODO: const event lookup
    //  don't use TypeId as it is unsound and may collide
    //  -> use a proc macro to generated an associated uuid, maybe it is possible to
    //  even get better performance with the capabilities of proc macros during dispatching?
    //  see https://github.com/rust-lang/rust/issues/10389
    event_index_set: IndexSet<TypeId>,

    // TODO: const event lookup
    // event_lkp_tbl: Vec<Option<usize>>,
    event_size: EventSize,
}

impl EventRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn get_index_of<E: 'static + Send + Sync>(&self) -> Option<usize> {
        self.get_index(TypeId::of::<E>())
    }

    #[inline]
    pub fn get_index(&self, tid: TypeId) -> Option<usize> {
        self.event_index_set.get_index_of(&tid)
    }

    #[inline]
    pub fn register_of<E: 'static + Send + Sync>(&mut self) -> usize {
        self.event_size = self.event_size.max(EventSize::of::<E>());
        self.event_index_set.insert_full(TypeId::of::<E>()).0
    }

    pub(crate) fn register_all<I: Iterator<Item = TypeId>>(
        &mut self,
        tids: I,
        max_event_size: EventSize,
    ) -> &mut Self {
        for tid in tids {
            self.event_index_set.insert(tid);
        }
        self.event_size = self.event_size.max(max_event_size);

        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.event_index_set.len()
    }

    #[inline]
    pub fn event_size(&self) -> EventSize {
        self.event_size
    }
}

impl Default for EventRegistry {
    fn default() -> Self {
        let mut registry = Self {
            event_index_set: Default::default(),
            event_size: Default::default(),
        };

        let e_idx = registry.register_of::<PaddingEvent>();
        assert_eq!(e_idx, PaddingEvent::EVENT_INDEX);

        let e_idx = registry.register_of::<ShutdownEvent>();
        assert_eq!(e_idx, ShutdownEvent::EVENT_INDEX);

        registry
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct EventSize(usize);

impl EventSize {
    pub const EVENT_ALIGN: usize = std::mem::align_of::<usize>();

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
    pub(crate) fn for_size(event_size: usize) -> usize {
        let alignments = (event_size / Self::EVENT_ALIGN)
            + if event_size % Self::EVENT_ALIGN == 0 {
                0
            } else {
                1
            };

        alignments * Self::EVENT_ALIGN
    }

    #[inline]
    pub fn inner(self) -> usize {
        self.0
    }
}

impl Deref for EventSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Into<usize> for EventSize {
    fn into(self) -> usize {
        self.0
    }
}

impl Default for EventSize {
    fn default() -> Self {
        Self(Self::EVENT_ALIGN)
    }
}

pub struct UntypedEvent {
    e_idx: usize,
    tid: TypeId,
    data: *mut u8,
    data_size: usize,
    drop_fn: Option<fn(*mut u8)>,
}

impl UntypedEvent {
    pub(crate) unsafe fn new<E: 'static + Send + Sync>(e_idx: usize, event: E) -> Self {
        let data_len = EventSize::of::<E>().inner();
        let layout = Self::layout(data_len);
        let ptr = std::alloc::alloc(layout) as *mut E;
        ptr.write(event);
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
        Layout::from_size_align(size, EventSize::EVENT_ALIGN).unwrap()
    }
}

impl Drop for UntypedEvent {
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

unsafe impl Send for UntypedEvent {}
unsafe impl Sync for UntypedEvent {}

pub struct EventSender {
    head: TripleBufferedHead<EventVec>,
    // Don't auto impl send and sync for EventSender for future compatibility,
    // as with an ring buffer the implementation might use thread local storage.
    _pd: PhantomData<*mut u8>,
}

impl EventSender {
    pub fn new(head: TripleBufferedHead<EventVec>) -> Self {
        Self {
            head,
            _pd: Default::default(),
        }
    }

    #[inline]
    pub fn send<E: 'static + Send + Sync>(&mut self, event: E) {
        if !self.head.write().push(event) {
            log::warn!(
                "skipping sending of unhandled event type: {}",
                std::any::type_name::<E>()
            );
        }
    }

    #[inline]
    pub fn send_iter<I: IntoIterator<Item = E>, E: 'static + Send + Sync>(&mut self, events: I) {
        if !self.head.write().extend(events) {
            log::warn!(
                "skipping sending of unhandled event type: {}",
                std::any::type_name::<E>()
            );
        }
    }

    #[inline]
    pub fn send_all(&mut self, events: &mut EventVec) {
        self.head.write().extend_vec(events);
    }

    #[inline]
    pub fn buffer(&self) -> EventVec {
        let registry = self.head
            .write()
            .get_registry().clone();

        EventVec::new(registry)
    }

    #[inline]
    pub fn prepare<E: 'static + Send + Sync>(&mut self, event: E) -> Option<UntypedEvent> {
        unsafe {
            // Optimization: static resolution of e_idx
            self.head
                .write()
                .get_registry()
                .get_index_of::<E>()
                .map(|e_idx| UntypedEvent::new(e_idx, event))
        }
    }

    #[inline]
    pub fn send_untyped(&mut self, mut event: UntypedEvent) {
        unsafe {
            let mut head = self.head.write();
            match head.get_registry().get_index(event.tid) {
                Some(e_idx) if e_idx == event.e_idx => {
                    head.push_untyped(e_idx, event.data, event.data_size, event.drop_fn.take());
                }
                _ => {
                    panic!("untyped event is incompatible with event registry");
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::EventSize;

    #[test]
    fn aligned_size_of() {
        assert_eq!(
            EventSize::of::<[u8; 1]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
        assert_eq!(
            EventSize::of::<[u8; 3]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
        assert_eq!(
            EventSize::of::<[u8; 7]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
        assert_eq!(
            EventSize::of::<[u8; 15]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
        assert_eq!(
            EventSize::of::<[u8; 31]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
        assert_eq!(
            EventSize::of::<[u8; 63]>().inner() % EventSize::EVENT_ALIGN,
            0
        );
    }
}
