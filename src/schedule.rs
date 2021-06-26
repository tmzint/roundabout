use crate::event::bus::{EventBus, EventBusReceiver, EventBusSender};
use crate::event::{EventRegistry, EventSender, ShutdownEvent, ShutdownRequestedEvent};
use crate::handler::{EventHandlerGroup, InitializedEventHandler, RuntimeContext};
use crate::prelude::EventVec;
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

pub struct EventScheduler {
    registry: Arc<EventRegistry>,
    groups: Vec<EventHandlerGroup>,
    primary: Option<EventHandlerGroup>,
    cpu_affinity: bool,
    min_bus_capacity: usize,
    shutdown_timeout: Duration,
    waiting_strategy: WaitingStrategy,
    shutdown: Arc<AtomicUsize>,
}

impl EventScheduler {
    pub fn new<ER: Into<Arc<EventRegistry>>>(
        registry: ER,
        groups: Vec<EventHandlerGroup>,
        primary: Option<EventHandlerGroup>,
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

            let (bus_sender, mut bus_receivers) = EventBus::bounded(
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
                    EventVec::new(self.registry.deref().to_owned()),
                    EventVec::new(self.registry.deref().to_owned()),
                    EventVec::new(self.registry.deref().to_owned()),
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
                    EventVec::new(self.registry.deref().to_owned()),
                    EventVec::new(self.registry.deref().to_owned()),
                    EventVec::new(self.registry.deref().to_owned()),
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
    tails: Vec<TripleBufferedTail<EventVec>>,
    mut bus_sender: EventBusSender,
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
            let mut events = tail.advance();
            buffer.extend_vec_unchecked(&mut events);
        }

        match shutdown.load(Ordering::Relaxed) {
            SHUTDOWN_NONE => {}
            SHUTDOWN_ORDERLY => match shutdown_timeout_at {
                None => {
                    if !buffer.push(ShutdownRequestedEvent::new()) {
                        log::info!(
                            "instant shutdown as no event handler for the request was registered"
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
                buffer.push(ShutdownEvent::new());
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
    mut group: EventHandlerGroup,
    head: TripleBufferedHead<EventVec>,
    receiver: EventBusReceiver,
    cpu: CpuAffinity,
    shutdown: Arc<AtomicUsize>,
) {
    let panic_shutdown = shutdown.clone();
    let panic_bomb = PipelinePanicBomb(&panic_shutdown);
    cpu.apply_for_current();

    let event_sender = EventSender::new(head);
    let shutdown_switch = ShutdownSwitch { shutdown };
    let context = RuntimeContext::new(event_sender, shutdown_switch);

    let blocking = group.take_blocking();
    let mut handlers = group.initialize();

    match (handlers.len(), blocking) {
        (1, None) => EventPipelineSingle {
            receiver,
            context,
            handler: handlers.pop().unwrap(),
        }
        .trim_horizon_blocking(),

        (1, Some(blocking)) => {
            let pipeline = EventPipelineSingle {
                receiver,
                context,
                handler: handlers.pop().unwrap(),
            };
            (blocking)(pipeline);
        }

        (_, None) => EventPipelineMulti {
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

pub struct EventPipelineMulti {
    receiver: EventBusReceiver,
    context: RuntimeContext,
    handlers: Vec<InitializedEventHandler>,
}

impl EventPipelineMulti {
    pub fn trim_horizon_blocking(mut self) {
        unsafe {
            loop {
                let event = self.receiver.recv();
                for handler in &mut self.handlers {
                    handler.handle(&mut self.context, event.event_idx(), event.data());
                }

                if event.event_idx() == ShutdownEvent::EVENT_INDEX {
                    return;
                }
            }
        }
    }
}

pub struct EventPipelineSingle {
    receiver: EventBusReceiver,
    context: RuntimeContext,
    handler: InitializedEventHandler,
}

impl EventPipelineSingle {
    #[inline]
    pub fn trim_horizon(&mut self) {
        unsafe {
            while let Some(event) = self.receiver.try_recv() {
                self.handler
                    .handle(&mut self.context, event.event_idx(), event.data());
            }
        }
    }

    #[inline]
    pub fn trim_horizon_blocking(mut self) {
        unsafe {
            loop {
                let event = self.receiver.recv();
                self.handler
                    .handle(&mut self.context, event.event_idx(), event.data());

                if event.event_idx() == ShutdownEvent::EVENT_INDEX {
                    return;
                }
            }
        }
    }
}

pub struct EventPipeline<T> {
    pipeline: EventPipelineSingle,
    _pd: PhantomData<T>,
}

impl<T> EventPipeline<T> {
    /**
    Safety:
        * the type has to match the type of the state of the enclosed InitializedEventHandler
    */
    pub(crate) unsafe fn new(pipeline: EventPipelineSingle) -> Self {
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
                let event = self.pipeline.receiver.recv();
                self.pipeline.handler.handle(
                    &mut self.pipeline.context,
                    event.event_idx(),
                    event.data(),
                );

                // Optimization: only check condition when handler actually handles event
                let state = &*(self.pipeline.handler.state() as *const T);
                if condition(state) || event.event_idx() == ShutdownEvent::EVENT_INDEX {
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
