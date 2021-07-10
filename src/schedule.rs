use crate::handler::{InitializedMessageHandler, MessageHandlerGroup, RuntimeContext};
use crate::message::bus::{MessageBus, MessageBusReceiver, MessageBusSender};
use crate::message::{MessageRegistry, MessageSender, ShutdownCommand, ShutdownRequestedEvent};
use crate::prelude::MessageVec;
use crate::util::cpu::CpuAffinity;
use crate::util::triple::{TripleBuffered, TripleBufferedHead, TripleBufferedTail};
use crate::wait::WaitingStrategy;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const SHUTDOWN_NONE: usize = 0;
const SHUTDOWN_ORDERLY: usize = 1;
const SHUTDOWN_FINISH: usize = 2;
const SHUTDOWN_ABORT: usize = 3;

pub struct ShutdownSwitch {
    shutdown: Arc<AtomicUsize>,
}

impl ShutdownSwitch {
    /** test only */
    #[allow(dead_code)]
    pub(crate) fn noop() -> Self {
        Self {
            shutdown: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn request_shutdown(&mut self) -> bool {
        self.shutdown
            .compare_exchange(
                SHUTDOWN_NONE,
                SHUTDOWN_ORDERLY,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    #[inline]
    pub fn finish_shutdown(&mut self) -> bool {
        self.shutdown
            .compare_exchange(
                SHUTDOWN_ORDERLY,
                SHUTDOWN_FINISH,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

pub struct MessageScheduler {
    registry: Arc<MessageRegistry>,
    groups: Vec<MessageHandlerGroup>,
    primary: Option<MessageHandlerGroup>,
    cpu_affinity: bool,
    min_bus_capacity: usize,
    shutdown_timeout: Duration,
    waiting_strategy: WaitingStrategy,
    shutdown: Arc<AtomicUsize>,
}

impl MessageScheduler {
    pub fn new<ER: Into<Arc<MessageRegistry>>>(
        registry: ER,
        groups: Vec<MessageHandlerGroup>,
        primary: Option<MessageHandlerGroup>,
        cpu_affinity: bool,
        min_bus_capacity: usize,
        shutdown_timeout: Duration,
        waiting_strategy: WaitingStrategy,
    ) -> Self {
        Self {
            registry: registry.into(),
            groups,
            primary,
            cpu_affinity,
            min_bus_capacity,
            shutdown_timeout,
            waiting_strategy,
            shutdown: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_shutdown_switch(&self) -> ShutdownSwitch {
        ShutdownSwitch {
            shutdown: self.shutdown.clone(),
        }
    }

    pub fn schedule<E: 'static + Send + Sync>(self, init: E) {
        unsafe {
            let pipeline_count = self.groups.len() + self.primary.iter().len();
            let mut tails = Vec::with_capacity(pipeline_count);
            let mut handles = Vec::with_capacity(pipeline_count);

            let (bus_sender, mut bus_receivers) = MessageBus::bounded(
                self.registry.deref().to_owned(),
                self.min_bus_capacity,
                pipeline_count,
                self.waiting_strategy,
            );

            // Optimization: support NUMA (Threadripper) / heterogeneous computing architecture (big.LITTLE)
            // Optimization: avoid scheduling on the same physical core (SMT), this currently assumes siblings are interleaved
            let mut cpu_affinities = if self.cpu_affinity {
                let mut v = CpuAffinity::for_cores();
                v.reverse();
                v
            } else {
                Vec::default()
            };

            let primary_cpu_affinity = cpu_affinities.pop().unwrap_or_default();

            for group in self.groups {
                let receiver = bus_receivers.pop().unwrap();

                let (head, tail) = TripleBuffered::new([
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                ]);
                tails.push(tail);

                let cpu = cpu_affinities.pop().unwrap_or_default();
                let shutdown = self.shutdown.clone();
                let handle =
                    thread::spawn(move || run_pipeline(group, head, receiver, cpu, shutdown));
                handles.push(handle);
            }

            if let Some(primary) = self.primary {
                let receiver = bus_receivers.pop().unwrap();

                let (head, tail) = TripleBuffered::new([
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                ]);
                tails.push(tail);

                let cpu = cpu_affinities.pop().unwrap_or_default();
                let shutdown = self.shutdown.clone();
                let shutdown_timeout = self.shutdown_timeout;
                let waiting_strategy = self.waiting_strategy;
                let router_handle = thread::spawn(move || {
                    run_router(
                        init,
                        tails,
                        bus_sender,
                        cpu,
                        shutdown_timeout,
                        &shutdown,
                        waiting_strategy,
                    );
                });
                handles.push(router_handle);

                run_pipeline(primary, head, receiver, primary_cpu_affinity, self.shutdown);
            } else {
                run_router(
                    init,
                    tails,
                    bus_sender,
                    primary_cpu_affinity,
                    self.shutdown_timeout,
                    &self.shutdown,
                    self.waiting_strategy,
                );
            }

            for handle in handles {
                handle.join().unwrap();
            }
        }
    }
}

unsafe fn run_router<E: 'static + Send + Sync>(
    init: E,
    tails: Vec<TripleBufferedTail<MessageVec>>,
    mut bus_sender: MessageBusSender,
    cpu: CpuAffinity,
    shutdown_timeout: Duration,
    shutdown: &AtomicUsize,
    waiting_strategy: WaitingStrategy,
) {
    cpu.apply_for_current();

    // Optimization: different waiting strategy for router / pipelines
    let mut waiter = waiting_strategy.waiter();
    let mut buffer = bus_sender.buffer();
    buffer.push(init);
    let mut shutdown_timeout_at: Option<Instant> = None;

    loop {
        for tail in &tails {
            let mut messages = tail.advance();
            buffer.extend_vec_unchecked(&mut messages);
        }

        match shutdown.load(Ordering::Relaxed) {
            SHUTDOWN_NONE => {}
            SHUTDOWN_ORDERLY => match shutdown_timeout_at {
                None => {
                    if !buffer.push(ShutdownRequestedEvent::new()) {
                        log::info!(
                            "instant shutdown as no message handler for the request was registered"
                        );
                        let _ = shutdown.compare_exchange(
                            SHUTDOWN_ORDERLY,
                            SHUTDOWN_FINISH,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                    } else {
                        log::info!("shutdown was requested");
                        shutdown_timeout_at = Some(Instant::now() + shutdown_timeout);
                    }
                }
                Some(at) => {
                    if at <= Instant::now() {
                        log::info!(
                            "timeout of shutdown after {:.2}s",
                            shutdown_timeout.as_secs_f64()
                        );
                    }
                }
            },
            SHUTDOWN_FINISH => {
                log::info!("finishing shutdown");
                buffer.push(ShutdownCommand::new());
                bus_sender.send_all(&mut buffer);
                bus_sender.flush_padding();
                return;
            }
            SHUTDOWN_ABORT => {
                log::error!("scheduler run into an unrecoverable error");
                std::process::abort();
            }
            _ => unreachable!(),
        };

        if buffer.len() > 0 {
            bus_sender.send_all(&mut buffer);
            bus_sender.flush_padding();
            waiter.reset();
        } else {
            waiter.wait()
        }
    }
}

fn run_pipeline(
    group: MessageHandlerGroup,
    head: TripleBufferedHead<MessageVec>,
    receiver: MessageBusReceiver,
    cpu: CpuAffinity,
    shutdown: Arc<AtomicUsize>,
) {
    let panic_shutdown = shutdown.clone();
    let panic_bomb = PipelinePanicBomb(&panic_shutdown);
    cpu.apply_for_current();

    let message_sender = MessageSender::new(head);
    let shutdown_switch = ShutdownSwitch { shutdown };
    let context = RuntimeContext::new(message_sender, shutdown_switch);

    let (mut handlers, blocking) = group.initialize(&context);

    match (handlers.len(), blocking) {
        (1, None) => MessagePipelineSingle {
            receiver,
            context,
            handler: handlers.pop().unwrap(),
        }
        .trim_horizon_blocking(),

        (1, Some(blocking)) => {
            let pipeline = MessagePipelineSingle {
                receiver,
                context,
                handler: handlers.pop().unwrap(),
            };
            (blocking)(pipeline);
        }

        (_, None) => MessagePipelineMulti {
            receiver,
            context,
            handlers,
        }
        .trim_horizon_blocking(),

        (i, Some(_)) => {
            panic!(
                "blocking handlers are only supported for a group with a length of one, \
                            group has a length of: {}",
                i
            );
        }
    }

    log::info!("pipeline shutdown");
    panic_bomb.disarm();
}

pub struct PipelinePanicBomb<'a>(&'a AtomicUsize);

impl<'a> PipelinePanicBomb<'a> {
    pub fn disarm(self) {
        std::mem::forget(self);
    }
}

impl<'a> Drop for PipelinePanicBomb<'a> {
    fn drop(&mut self) {
        self.0.store(SHUTDOWN_ABORT, Ordering::Release);
    }
}

pub struct MessagePipelineMulti {
    receiver: MessageBusReceiver,
    context: RuntimeContext,
    handlers: Vec<InitializedMessageHandler>,
}

impl MessagePipelineMulti {
    pub fn trim_horizon_blocking(mut self) {
        unsafe {
            loop {
                let message = self.receiver.recv();
                for handler in &mut self.handlers {
                    handler.handle(&mut self.context, message.message_idx(), message.data());
                }

                if message.message_idx() == ShutdownCommand::MESSAGE_INDEX {
                    return;
                }
            }
        }
    }
}

pub struct MessagePipelineSingle {
    receiver: MessageBusReceiver,
    context: RuntimeContext,
    handler: InitializedMessageHandler,
}

impl MessagePipelineSingle {
    #[inline]
    pub fn trim_horizon(&mut self) {
        unsafe {
            while let Some(message) = self.receiver.try_recv() {
                self.handler
                    .handle(&mut self.context, message.message_idx(), message.data());
            }
        }
    }

    #[inline]
    pub fn trim_horizon_blocking(mut self) {
        unsafe {
            loop {
                let message = self.receiver.recv();
                self.handler
                    .handle(&mut self.context, message.message_idx(), message.data());

                if message.message_idx() == ShutdownCommand::MESSAGE_INDEX {
                    return;
                }
            }
        }
    }
}

pub struct MessagePipeline<T> {
    pipeline: MessagePipelineSingle,
    _pd: PhantomData<T>,
}

impl<T> MessagePipeline<T> {
    /**
    Safety:
        * the type has to match the type of the state of the enclosed InitializedMessageHandler
    */
    pub(crate) unsafe fn new(pipeline: MessagePipelineSingle) -> Self {
        Self {
            pipeline,
            _pd: Default::default(),
        }
    }

    #[inline]
    pub fn trim_horizon(&mut self) {
        self.pipeline.trim_horizon();
    }

    #[inline]
    pub fn trim_horizon_while<F: Fn(&T) -> bool>(&mut self, condition: F) {
        self.trim_horizon_until(|t| !condition(t));
    }

    #[inline]
    pub fn trim_horizon_until<F: Fn(&T) -> bool>(&mut self, condition: F) {
        unsafe {
            if condition(&*(self.pipeline.handler.state() as *const T)) {
                return;
            }

            loop {
                let message = self.pipeline.receiver.recv();
                self.pipeline.handler.handle(
                    &mut self.pipeline.context,
                    message.message_idx(),
                    message.data(),
                );

                // Optimization: only check condition when handler actually handles message
                let state = &*(self.pipeline.handler.state() as *const T);
                if condition(state) || message.message_idx() == ShutdownCommand::MESSAGE_INDEX {
                    return;
                }
            }
        }
    }

    #[inline]
    pub fn trim_horizon_blocking(self) {
        self.pipeline.trim_horizon_blocking();
    }

    #[inline]
    pub fn state(&self) -> &T {
        unsafe { &*(self.pipeline.handler.state() as *const T) }
    }

    #[inline]
    pub fn state_mut(&mut self) -> &mut T {
        unsafe { &mut *(self.pipeline.handler.state() as *mut T) }
    }

    #[inline]
    pub fn context(&mut self) -> &mut RuntimeContext {
        &mut self.pipeline.context
    }

    #[inline]
    pub fn explode(&mut self) -> (&mut T, &mut RuntimeContext) {
        let state = unsafe { &mut *(self.pipeline.handler.state() as *mut T) };
        (state, &mut self.pipeline.context)
    }
}
