use crate::message::{MessageRegistry, MessageSender, MessageSize};
use crate::prelude::MessageBusView;
use crate::schedule::{MessageReceiver, ShutdownSwitch};
use crate::util::HashMap;
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct RuntimeContext {
    // Optimization: clone vs arc vs reference
    registry: MessageRegistry,
    sender: MessageSender,
    shutdown_switch: ShutdownSwitch,
}

impl RuntimeContext {
    pub(crate) fn new(
        registry: MessageRegistry,
        sender: MessageSender,
        shutdown_switch: ShutdownSwitch,
    ) -> Self {
        Self {
            registry,
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

    #[inline]
    pub fn registry(&self) -> &MessageRegistry {
        &self.registry
    }
}

impl AsRef<RuntimeContext> for RuntimeContext {
    #[inline]
    fn as_ref(&self) -> &RuntimeContext {
        self
    }
}

pub struct MessageGroupBuilder<'a> {
    registry: &'a mut MessageRegistry,
}

impl<'a> MessageGroupBuilder<'a> {
    pub(crate) fn new(registry: &'a mut MessageRegistry) -> Self {
        Self { registry }
    }

    pub fn register<TS2, T: 'static, A: 'static, B: 'static, F>(
        &mut self,
        f: F,
    ) -> MessageHandlerBuilder<Closed, TS2, T, A, B>
    where
        F: FnOnce(OpenMessageHandlerBuilder<T, A, B>) -> MessageHandlerBuilder<Open, TS2, T, A, B>,
    {
        f(OpenMessageHandlerBuilder::new()).close(&mut self.registry)
    }

    // TODO: use result as a return value for the group_init?
    pub fn init<F>(self, group_init: F) -> MessageGroup
    where
        F: FnOnce(MessageReceiver, RuntimeContext) + 'static + Send,
    {
        MessageGroup {
            group_init: Box::new(group_init),
        }
    }
}

pub struct MessageGroup {
    group_init: Box<dyn FnOnce(MessageReceiver, RuntimeContext) + 'static + Send>,
}

impl MessageGroup {
    pub(crate) fn start(self, recv: MessageReceiver, context: RuntimeContext) {
        (self.group_init)(recv, context);
    }
}

pub struct Open;
pub struct Closed;

pub struct Empty;
pub struct Init;

pub type OpenMessageHandlerBuilder<T, A = RuntimeContext, B = ()> =
    MessageHandlerBuilder<Open, Empty, T, A, B>;
pub type ClosedMessageHandlerBuilder<T, A = RuntimeContext, B = ()> =
    MessageHandlerBuilder<Closed, Empty, T, A, B>;
pub type InitMessageHandlerBuilder<T, A = RuntimeContext, B = ()> =
    MessageHandlerBuilder<Open, Init, T, A, B>;
pub type FinMessageHandlerBuilder<T, A = RuntimeContext, B = ()> =
    MessageHandlerBuilder<Closed, Init, T, A, B>;

pub struct MessageHandlerBuilder<TS1, TS2, T, A, B> {
    jmp_map: Arc<HashMap<TypeId, fn(&mut T, &mut A, *const u8) -> B>>,
    max_message_size: MessageSize,
    state_init: Option<Box<dyn FnOnce(&A) -> T + Send + 'static>>,
    _pd_ts1: PhantomData<TS1>,
    _pd_ts2: PhantomData<TS2>,
}

impl<T: 'static, A: 'static, B: 'static> MessageHandlerBuilder<Open, Empty, T, A, B> {
    pub(crate) fn new() -> Self {
        Self {
            jmp_map: Default::default(),
            max_message_size: Default::default(),
            state_init: None,
            _pd_ts1: Default::default(),
            _pd_ts2: Default::default(),
        }
    }
}

impl<TS2, T: 'static, A: 'static, B: 'static> MessageHandlerBuilder<Open, TS2, T, A, B> {
    pub fn on<E: 'static + Send + Sync>(mut self, f: fn(&mut T, &mut A, e: &E) -> B) -> Self {
        let tid = TypeId::of::<E>();
        self.max_message_size = self.max_message_size.max(MessageSize::of::<E>());

        let prev = Arc::get_mut(&mut self.jmp_map)
            .expect("unique jmp_map when open")
            .insert(tid, unsafe { std::mem::transmute(f) });
        if prev.is_some() {
            panic!(
                "override of message handler branch for: {}",
                std::any::type_name::<E>()
            )
        }

        self
    }

    pub(crate) fn close(
        self,
        registry: &mut MessageRegistry,
    ) -> MessageHandlerBuilder<Closed, TS2, T, A, B> {
        registry.register_all(self.jmp_map.keys().copied(), self.max_message_size);

        MessageHandlerBuilder {
            jmp_map: self.jmp_map,
            max_message_size: self.max_message_size,
            state_init: self.state_init,
            _pd_ts1: Default::default(),
            _pd_ts2: Default::default(),
        }
    }
}

impl<T: 'static, A: 'static, B: 'static> Clone for MessageHandlerBuilder<Closed, Empty, T, A, B> {
    fn clone(&self) -> Self {
        Self {
            jmp_map: self.jmp_map.clone(),
            max_message_size: self.max_message_size,
            // is none as we are not in TS2 of Init but in Empty
            state_init: None,
            _pd_ts1: Default::default(),
            _pd_ts2: Default::default(),
        }
    }
}

impl<TS1, T: 'static, A: 'static, B: 'static> MessageHandlerBuilder<TS1, Empty, T, A, B> {
    pub fn init_fn<F>(self, sate_init: F) -> MessageHandlerBuilder<TS1, Init, T, A, B>
    where
        F: FnOnce(&A) -> T + Send + 'static,
    {
        MessageHandlerBuilder {
            jmp_map: self.jmp_map,
            max_message_size: self.max_message_size,
            state_init: Some(Box::new(sate_init)),
            _pd_ts1: Default::default(),
            _pd_ts2: Default::default(),
        }
    }
}

impl<TS1, T: Default + 'static, A: 'static, B: 'static> MessageHandlerBuilder<TS1, Empty, T, A, B> {
    pub fn init_default(self) -> MessageHandlerBuilder<TS1, Init, T, A, B> {
        self.init_fn(|_| T::default())
    }
}

impl<TS1, T: Send + 'static, A: 'static, B: 'static> MessageHandlerBuilder<TS1, Empty, T, A, B> {
    pub fn init(self, state: T) -> MessageHandlerBuilder<TS1, Init, T, A, B> {
        self.init_fn(|_| state)
    }
}

impl<T: 'static, A: AsRef<RuntimeContext> + 'static, B: 'static>
    MessageHandlerBuilder<Closed, Init, T, A, B>
{
    pub fn finish(self, context: &A) -> anyhow::Result<MessageHandler<T, A, B>> {
        let registry = context.as_ref().registry();
        let mut jmp_tbl = Vec::with_capacity(registry.len());
        for _ in 0..registry.len() {
            jmp_tbl.push(None)
        }

        for (k, f) in self.jmp_map.iter() {
            let idx = registry
                .get_index(*k)
                .ok_or_else(|| anyhow::anyhow!("message type not found: {:?}", k))?;

            jmp_tbl[idx] = Some(*f);
        }

        let state = (self.state_init.expect("state_init when init"))(context);

        Ok(MessageHandler {
            state,
            jmp_tbl,
            _pd: Default::default(),
        })
    }
}

pub struct MessageHandler<T, A = RuntimeContext, B = ()> {
    pub state: T,
    jmp_tbl: Vec<Option<fn(&mut T, &mut A, *const u8) -> B>>,
    // make non send
    _pd: PhantomData<*mut u8>,
}

impl<T: 'static, A: 'static, B: 'static> MessageHandler<T, A, B> {
    #[inline]
    pub fn handle<'a>(&mut self, context: &mut A, message: &MessageBusView<'a>) -> Option<B> {
        // Optimization: if (option) vs noop fn

        // This requires that the message uses the same message registry,
        // which is guaranteed by making Message Handler and Runtime Context non send.
        unsafe {
            self.jmp_tbl
                .get_unchecked(message.message_idx())
                .map(|f| f(&mut self.state, context, message.data()))
        }
    }
}
