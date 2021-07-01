use crate::event::{EventRegistry, EventSender, EventSize};
use crate::prelude::EventPipeline;
use crate::schedule::{EventPipelineSingle, ShutdownSwitch};
use crate::util::HashMap;
use core::mem;
use std::alloc::Layout;
use std::any::TypeId;
use std::marker::PhantomData;

// TODO: use TypeStates to minimize temporary structures

pub struct RuntimeContext {
    sender: EventSender,
    shutdown_switch: ShutdownSwitch,
}

impl RuntimeContext {
    pub(crate) fn new(sender: EventSender, shutdown_switch: ShutdownSwitch) -> Self {
        Self {
            sender,
            shutdown_switch,
        }
    }

    #[inline]
    pub fn sender(&self) -> &EventSender {
        &self.sender
    }

    #[inline]
    pub fn shutdown_switch(&mut self) -> &mut ShutdownSwitch {
        &mut self.shutdown_switch
    }
}

pub struct EventGroupBuilder<'a> {
    registry: &'a mut EventRegistry,
    handlers: Vec<EventHandler>,
    blocking: Option<Box<dyn FnOnce(EventPipelineSingle) + 'static + Send>>,
}

impl<'a> EventGroupBuilder<'a> {
    pub fn new(registry: &'a mut EventRegistry) -> Self {
        Self {
            registry,
            handlers: vec![],
            blocking: None,
        }
    }

    pub fn register<T: 'static, H>(mut self, handler: H) -> Self
    where
        H: Fn(EventHandlerBuilder<T>) -> EventHandlerBlueprint<T>,
    {
        assert!(self.blocking.is_none());
        let builder = EventHandlerBuilder::new();
        let blueprint = handler(builder);
        self.registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_event_size,
        );
        self.handlers.push(blueprint.finish(&self.registry));
        self
    }

    pub(crate) fn register_blocking<T: 'static, H>(mut self, blocking_handler: H) -> Self
    where
        H: Fn(EventHandlerBuilder<T>) -> BlockingEventHandlerBlueprint<T>,
    {
        assert!(self.blocking.is_none());
        assert!(self.handlers.is_empty());

        let builder = EventHandlerBuilder::new();
        let BlockingEventHandlerBlueprint {
            blueprint,
            blocking,
        } = blocking_handler(builder);

        self.registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_event_size,
        );
        self.handlers.push(blueprint.finish(&self.registry));

        self.blocking = Some(Box::new(|sep| unsafe { blocking(EventPipeline::new(sep)) }));

        self
    }

    pub(crate) fn finish(self) -> EventHandlerGroup {
        EventHandlerGroup {
            handlers: self.handlers,
            blocking: self.blocking,
        }
    }
}

pub struct EventHandlerGroup {
    handlers: Vec<EventHandler>,
    blocking: Option<Box<dyn FnOnce(EventPipelineSingle) + 'static + Send>>,
}

impl EventHandlerGroup {
    pub(crate) fn fill_jmp_tbl(&mut self, length: usize) {
        for handler in &mut self.handlers {
            handler.fill_jmp_tbl(length);
        }
    }

    pub(crate) fn take_blocking(
        &mut self,
    ) -> Option<Box<dyn FnOnce(EventPipelineSingle) + 'static + Send>> {
        self.blocking.take()
    }

    pub(crate) fn initialize(self, context: &RuntimeContext) -> Vec<InitializedEventHandler> {
        self.handlers
            .into_iter()
            .map(|h| h.initialize(context))
            .collect()
    }
}

pub struct EventHandlerBuilder<T> {
    jmp_map: HashMap<TypeId, fn(*mut u8, &mut RuntimeContext, *const u8)>,
    max_event_size: EventSize,
    pd: PhantomData<T>,
}

impl<T: 'static> EventHandlerBuilder<T> {
    pub(crate) fn new() -> Self {
        Self {
            jmp_map: Default::default(),
            max_event_size: Default::default(),
            pd: Default::default(),
        }
    }

    pub fn on<E: 'static + Send + Sync>(
        mut self,
        f: fn(&mut T, &mut RuntimeContext, e: &E),
    ) -> Self {
        let tid = TypeId::of::<E>();
        self.max_event_size = self.max_event_size.max(EventSize::of::<E>());
        let prev = self.jmp_map.insert(tid, unsafe { std::mem::transmute(f) });
        if prev.is_some() {
            panic!(
                "override of event handler branch for: {}",
                std::any::type_name::<E>()
            )
        }

        self
    }

    pub fn with_factory<F: FnOnce(&RuntimeContext) -> T + 'static + Send>(
        self,
        state_init: F,
    ) -> EventHandlerBlueprint<T> {
        EventHandlerBlueprint {
            builder: self,
            state_init: Box::new(state_init),
        }
    }
}

impl<T: 'static + Send> EventHandlerBuilder<T> {
    pub fn with(self, state: T) -> EventHandlerBlueprint<T> {
        EventHandlerBlueprint {
            builder: self,
            state_init: Box::new(|_| state),
        }
    }
}

impl<T: 'static + Default> EventHandlerBuilder<T> {
    pub fn with_default(self) -> EventHandlerBlueprint<T> {
        EventHandlerBlueprint {
            builder: self,
            state_init: Box::new(|_| T::default()),
        }
    }
}

unsafe impl<T: 'static> Send for EventHandlerBuilder<T> {}

pub struct EventHandlerBlueprint<T> {
    builder: EventHandlerBuilder<T>,
    state_init: Box<dyn FnOnce(&RuntimeContext) -> T + 'static + Send>,
}

impl<T: 'static> EventHandlerBlueprint<T> {
    pub fn block<F>(self, blocking: F) -> BlockingEventHandlerBlueprint<T>
    where
        F: FnOnce(EventPipeline<T>) + 'static + Send,
    {
        BlockingEventHandlerBlueprint {
            blueprint: self,
            blocking: Box::new(blocking),
        }
    }

    pub(crate) fn finish(self, registry: &EventRegistry) -> EventHandler {
        unsafe {
            let mut jmp_tbl = Vec::with_capacity(registry.len());
            for _ in 0..registry.len() {
                jmp_tbl.push(None)
            }

            for (k, f) in self.builder.jmp_map {
                let idx = registry.get_index(k).expect("registered event");
                jmp_tbl[idx] = Some(f);
            }

            EventHandler::new(self.state_init, jmp_tbl)
        }
    }
}

unsafe impl<T: 'static> Send for EventHandlerBlueprint<T> {}

pub struct BlockingEventHandlerBlueprint<T> {
    pub(crate) blueprint: EventHandlerBlueprint<T>,
    pub(crate) blocking: Box<dyn FnOnce(EventPipeline<T>) + 'static + Send>,
}

pub struct EventHandler {
    state_init: Box<dyn for<'a> FnOnce(&'a RuntimeContext) -> *mut u8>,
    jmp_tbl: Vec<Option<fn(*mut u8, &mut RuntimeContext, *const u8)>>,
    destructor: fn(*mut u8),
}

impl EventHandler {
    unsafe fn new<T: 'static>(
        state_init: Box<dyn for<'a> FnOnce(&'a RuntimeContext) -> T + 'static + Send>,
        jmp_tbl: Vec<Option<fn(*mut u8, &mut RuntimeContext, *const u8)>>,
    ) -> Self {
        let destructor = |state_ptr: *mut u8| {
            (state_ptr as *mut T).drop_in_place();
            std::alloc::dealloc(state_ptr, Layout::new::<mem::MaybeUninit<T>>());
        };

        let state_init: Box<dyn for<'a> FnOnce(&'a RuntimeContext) -> *mut u8> =
            Box::new(|context| {
                let initial = (state_init)(context);
                let layout = Layout::new::<mem::MaybeUninit<T>>();
                if usize::BITS < 64 && layout.size() > isize::MAX as usize {
                    panic!("state capacity overflow");
                }
                let ptr = std::alloc::alloc(layout) as *mut T;
                ptr.write(initial);
                ptr as *mut u8
            });

        Self {
            state_init,
            jmp_tbl,
            destructor,
        }
    }

    pub(crate) fn fill_jmp_tbl(&mut self, length: usize) {
        for _ in self.jmp_tbl.len()..length {
            self.jmp_tbl.push(None);
        }
    }

    pub(crate) fn initialize(self, context: &RuntimeContext) -> InitializedEventHandler {
        InitializedEventHandler {
            state: (self.state_init)(context),
            jmp_tbl: self.jmp_tbl,
            destructor: self.destructor,
        }
    }
}

unsafe impl Send for EventHandler {}

pub struct InitializedEventHandler {
    state: *mut u8,
    // Optimization: jmp_tbl size
    //  don't fully represent all types but use (max_event_idx - min_event_idx + 1) length of a jump table.
    //  offsets can then be applied to the given event_idx to map it to the sub section
    //  this then can be further optimized by rearranging the event indexes e.g. via hill climbing.
    //  The initial order should already be partially ordered as they are based on handler registrations.
    //  see: https://stackoverflow.com/questions/18570427/how-to-optimize-the-size-of-jump-tables
    //
    // Optimization:
    //  branch table index for the position in the jump table or
    //  leading zeroes to compute position in jump table directly
    //
    // Optimization: use arrayvec instead
    jmp_tbl: Vec<Option<fn(*mut u8, &mut RuntimeContext, *const u8)>>,
    destructor: fn(*mut u8),
}

impl InitializedEventHandler {
    /**
    Safety:
        * the payload needs to correspond to the event type associated with the event index
    */
    pub(crate) unsafe fn handle(
        &mut self,
        context: &mut RuntimeContext,
        event_index: usize,
        event_payload: *const u8,
    ) {
        // Optimization: if vs noop fn
        // Optimization: use unchecked get with full jump table
        if let Some(Some(f)) = self.jmp_tbl.get(event_index) {
            f(self.state, context, event_payload);
        }
    }

    pub(crate) unsafe fn state(&self) -> *mut u8 {
        self.state
    }
}

impl Drop for InitializedEventHandler {
    fn drop(&mut self) {
        (self.destructor)(self.state);
    }
}

#[cfg(test)]
mod tests {
    use super::{EventHandlerBuilder, EventRegistry, RuntimeContext};
    use crate::event::vec::EventVec;
    use crate::event::EventSender;
    use crate::schedule::ShutdownSwitch;
    use crate::util::triple::TripleBuffered;
    use std::sync::Arc;

    #[test]
    fn on_event_handler() {
        let blueprint = EventHandlerBuilder::new()
            .on::<usize>(|u, _s, event| *u = *u + *event)
            .with_factory(|_| 1usize);

        let mut registry = EventRegistry::default();
        registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_event_size,
        );

        let event_handler = blueprint.finish(&registry);

        let e: usize = usize::MAX - 1;
        let idx = registry.get_index_of::<usize>().unwrap();

        let (head, _) = TripleBuffered::new([
            EventVec::new(registry.clone()),
            EventVec::new(registry.clone()),
            EventVec::new(registry),
        ]);
        let mut context = RuntimeContext::new(EventSender::new(head), ShutdownSwitch::noop());

        unsafe {
            let event_payload = (&e as *const usize) as *const u8;
            let mut initialized_event_handler = event_handler.initialize(&context);
            initialized_event_handler.handle(&mut context, idx, event_payload);
            assert_eq!(
                *(&*(initialized_event_handler.state as *const usize)),
                usize::MAX
            );
        }
    }

    #[test]
    fn drop_event_handler_blueprint() {
        let builder: EventHandlerBuilder<Arc<()>> = EventHandlerBuilder::new();
        let arc: Arc<()> = Arc::new(());
        let event_handler = builder.with(arc.clone());
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(event_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }

    #[test]
    fn drop_event_handler() {
        let builder: EventHandlerBuilder<Arc<()>> = EventHandlerBuilder::new();
        let registry = EventRegistry::default();
        let arc: Arc<()> = Arc::new(());
        let event_handler = builder.with(arc.clone()).finish(&registry);
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(event_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }

    #[test]
    fn drop_initialized_event_handler() {
        let builder: EventHandlerBuilder<Arc<()>> = EventHandlerBuilder::new();
        let registry = EventRegistry::default();
        let arc: Arc<()> = Arc::new(());
        let context = RuntimeContext::new(
            EventSender::new(TripleBuffered::new_fn(|| EventVec::new(registry.clone())).0),
            ShutdownSwitch::noop(),
        );
        let initialized_event_handler = builder
            .with(arc.clone())
            .finish(&registry)
            .initialize(&context);
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(initialized_event_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }
}
