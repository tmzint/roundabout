use crate::message::{MessageRegistry, MessageSender, MessageSize};
use crate::prelude::MessagePipeline;
use crate::schedule::{MessagePipelineSingle, ShutdownSwitch};
use crate::util::HashMap;
use core::mem;
use std::alloc::Layout;
use std::any::TypeId;
use std::marker::PhantomData;

// TODO: use TypeStates to minimize temporary structures

pub struct RuntimeContext {
    sender: MessageSender,
    shutdown_switch: ShutdownSwitch,
}

impl RuntimeContext {
    pub(crate) fn new(sender: MessageSender, shutdown_switch: ShutdownSwitch) -> Self {
        Self {
            sender,
            shutdown_switch,
        }
    }

    #[inline]
    pub fn sender(&self) -> &MessageSender {
        &self.sender
    }

    #[inline]
    pub fn shutdown_switch(&mut self) -> &mut ShutdownSwitch {
        &mut self.shutdown_switch
    }
}

pub struct MessageGroupBuilder<'a> {
    registry: &'a mut MessageRegistry,
    handlers: Vec<MessageHandler>,
    blocking: Option<Box<dyn FnOnce(MessagePipelineSingle) + 'static + Send>>,
}

impl<'a> MessageGroupBuilder<'a> {
    pub fn new(registry: &'a mut MessageRegistry) -> Self {
        Self {
            registry,
            handlers: vec![],
            blocking: None,
        }
    }

    pub fn register<T: 'static, H>(mut self, handler: H) -> Self
    where
        H: Fn(MessageHandlerBuilder<T>) -> MessageHandlerBlueprint<T>,
    {
        assert!(self.blocking.is_none());
        let builder = MessageHandlerBuilder::new();
        let blueprint = handler(builder);
        self.registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_message_size,
        );
        self.handlers.push(blueprint.finish(&self.registry));
        self
    }

    pub(crate) fn register_blocking<T: 'static, H>(mut self, blocking_handler: H) -> Self
    where
        H: Fn(MessageHandlerBuilder<T>) -> BlockingMessageHandlerBlueprint<T>,
    {
        assert!(self.blocking.is_none());
        assert!(self.handlers.is_empty());

        let builder = MessageHandlerBuilder::new();
        let BlockingMessageHandlerBlueprint {
            blueprint,
            blocking,
        } = blocking_handler(builder);

        self.registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_message_size,
        );
        self.handlers.push(blueprint.finish(&self.registry));

        self.blocking = Some(Box::new(|sep| unsafe {
            blocking(MessagePipeline::new(sep))
        }));

        self
    }

    pub(crate) fn finish(self) -> MessageHandlerGroup {
        MessageHandlerGroup {
            handlers: self.handlers,
            blocking: self.blocking,
        }
    }
}

pub struct MessageHandlerGroup {
    handlers: Vec<MessageHandler>,
    blocking: Option<Box<dyn FnOnce(MessagePipelineSingle) + 'static + Send>>,
}

impl MessageHandlerGroup {
    pub(crate) fn fill_jmp_tbl(&mut self, length: usize) {
        for handler in &mut self.handlers {
            handler.fill_jmp_tbl(length);
        }
    }

    pub(crate) fn take_blocking(
        &mut self,
    ) -> Option<Box<dyn FnOnce(MessagePipelineSingle) + 'static + Send>> {
        self.blocking.take()
    }

    pub(crate) fn initialize(self, context: &RuntimeContext) -> Vec<InitializedMessageHandler> {
        self.handlers
            .into_iter()
            .map(|h| h.initialize(context))
            .collect()
    }
}

pub struct MessageHandlerBuilder<T> {
    jmp_map: HashMap<TypeId, fn(*mut u8, &mut RuntimeContext, *const u8)>,
    max_message_size: MessageSize,
    pd: PhantomData<T>,
}

impl<T: 'static> MessageHandlerBuilder<T> {
    pub(crate) fn new() -> Self {
        Self {
            jmp_map: Default::default(),
            max_message_size: Default::default(),
            pd: Default::default(),
        }
    }

    pub fn on<E: 'static + Send + Sync>(
        mut self,
        f: fn(&mut T, &mut RuntimeContext, e: &E),
    ) -> Self {
        let tid = TypeId::of::<E>();
        self.max_message_size = self.max_message_size.max(MessageSize::of::<E>());
        let prev = self.jmp_map.insert(tid, unsafe { std::mem::transmute(f) });
        if prev.is_some() {
            panic!(
                "override of message handler branch for: {}",
                std::any::type_name::<E>()
            )
        }

        self
    }

    pub fn with_factory<F: FnOnce(&RuntimeContext) -> T + 'static + Send>(
        self,
        state_init: F,
    ) -> MessageHandlerBlueprint<T> {
        MessageHandlerBlueprint {
            builder: self,
            state_init: Box::new(state_init),
        }
    }
}

impl<T: 'static + Send> MessageHandlerBuilder<T> {
    pub fn with(self, state: T) -> MessageHandlerBlueprint<T> {
        MessageHandlerBlueprint {
            builder: self,
            state_init: Box::new(|_| state),
        }
    }
}

impl<T: 'static + Default> MessageHandlerBuilder<T> {
    pub fn with_default(self) -> MessageHandlerBlueprint<T> {
        MessageHandlerBlueprint {
            builder: self,
            state_init: Box::new(|_| T::default()),
        }
    }
}

unsafe impl<T: 'static> Send for MessageHandlerBuilder<T> {}

pub struct MessageHandlerBlueprint<T> {
    builder: MessageHandlerBuilder<T>,
    state_init: Box<dyn FnOnce(&RuntimeContext) -> T + 'static + Send>,
}

impl<T: 'static> MessageHandlerBlueprint<T> {
    pub fn block<F>(self, blocking: F) -> BlockingMessageHandlerBlueprint<T>
    where
        F: FnOnce(MessagePipeline<T>) + 'static + Send,
    {
        BlockingMessageHandlerBlueprint {
            blueprint: self,
            blocking: Box::new(blocking),
        }
    }

    pub(crate) fn finish(self, registry: &MessageRegistry) -> MessageHandler {
        unsafe {
            let mut jmp_tbl = Vec::with_capacity(registry.len());
            for _ in 0..registry.len() {
                jmp_tbl.push(None)
            }

            for (k, f) in self.builder.jmp_map {
                let idx = registry.get_index(k).expect("registered message");
                jmp_tbl[idx] = Some(f);
            }

            MessageHandler::new(self.state_init, jmp_tbl)
        }
    }
}

unsafe impl<T: 'static> Send for MessageHandlerBlueprint<T> {}

pub struct BlockingMessageHandlerBlueprint<T> {
    pub(crate) blueprint: MessageHandlerBlueprint<T>,
    pub(crate) blocking: Box<dyn FnOnce(MessagePipeline<T>) + 'static + Send>,
}

pub struct MessageHandler {
    state_init: Box<dyn for<'a> FnOnce(&'a RuntimeContext) -> *mut u8>,
    jmp_tbl: Vec<Option<fn(*mut u8, &mut RuntimeContext, *const u8)>>,
    destructor: fn(*mut u8),
}

impl MessageHandler {
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

    pub(crate) fn initialize(self, context: &RuntimeContext) -> InitializedMessageHandler {
        InitializedMessageHandler {
            state: (self.state_init)(context),
            jmp_tbl: self.jmp_tbl,
            destructor: self.destructor,
        }
    }
}

unsafe impl Send for MessageHandler {}

pub struct InitializedMessageHandler {
    state: *mut u8,
    // Optimization: jmp_tbl size
    //  don't fully represent all types but use (max_message_idx - min_message_idx + 1) length of a jump table.
    //  offsets can then be applied to the given message_idx to map it to the sub section
    //  this then can be further optimized by rearranging the message indexes e.g. via hill climbing.
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

impl InitializedMessageHandler {
    /**
    Safety:
        * the payload needs to correspond to the message type associated with the message index
    */
    pub(crate) unsafe fn handle(
        &mut self,
        context: &mut RuntimeContext,
        message_index: usize,
        message_payload: *const u8,
    ) {
        // Optimization: if vs noop fn
        // Optimization: use unchecked get with full jump table
        if let Some(Some(f)) = self.jmp_tbl.get(message_index) {
            f(self.state, context, message_payload);
        }
    }

    pub(crate) unsafe fn state(&self) -> *mut u8 {
        self.state
    }
}

impl Drop for InitializedMessageHandler {
    fn drop(&mut self) {
        (self.destructor)(self.state);
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageHandlerBuilder, MessageRegistry, RuntimeContext};
    use crate::message::vec::MessageVec;
    use crate::message::MessageSender;
    use crate::schedule::ShutdownSwitch;
    use crate::util::triple::TripleBuffered;
    use std::sync::Arc;

    #[test]
    fn on_message_handler() {
        let blueprint = MessageHandlerBuilder::new()
            .on::<usize>(|u, _s, message| *u = *u + *message)
            .with_factory(|_| 1usize);

        let mut registry = MessageRegistry::default();
        registry.register_all(
            blueprint.builder.jmp_map.keys().copied(),
            blueprint.builder.max_message_size,
        );

        let message_handler = blueprint.finish(&registry);

        let e: usize = usize::MAX - 1;
        let idx = registry.get_index_of::<usize>().unwrap();

        let (head, _) = TripleBuffered::new([
            MessageVec::new(registry.clone()),
            MessageVec::new(registry.clone()),
            MessageVec::new(registry),
        ]);
        let mut context = RuntimeContext::new(MessageSender::new(head), ShutdownSwitch::noop());

        unsafe {
            let message_payload = (&e as *const usize) as *const u8;
            let mut initialized_message_handler = message_handler.initialize(&context);
            initialized_message_handler.handle(&mut context, idx, message_payload);
            assert_eq!(
                *(&*(initialized_message_handler.state as *const usize)),
                usize::MAX
            );
        }
    }

    #[test]
    fn drop_message_handler_blueprint() {
        let builder: MessageHandlerBuilder<Arc<()>> = MessageHandlerBuilder::new();
        let arc: Arc<()> = Arc::new(());
        let message_handler = builder.with(arc.clone());
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(message_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }

    #[test]
    fn drop_message_handler() {
        let builder: MessageHandlerBuilder<Arc<()>> = MessageHandlerBuilder::new();
        let registry = MessageRegistry::default();
        let arc: Arc<()> = Arc::new(());
        let message_handler = builder.with(arc.clone()).finish(&registry);
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(message_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }

    #[test]
    fn drop_initialized_message_handler() {
        let builder: MessageHandlerBuilder<Arc<()>> = MessageHandlerBuilder::new();
        let registry = MessageRegistry::default();
        let arc: Arc<()> = Arc::new(());
        let context = RuntimeContext::new(
            MessageSender::new(TripleBuffered::new_fn(|| MessageVec::new(registry.clone())).0),
            ShutdownSwitch::noop(),
        );
        let initialized_message_handler = builder
            .with(arc.clone())
            .finish(&registry)
            .initialize(&context);
        assert_eq!(Arc::strong_count(&arc), 2);
        std::mem::drop(initialized_message_handler);
        assert_eq!(Arc::strong_count(&arc), 1);
    }
}
