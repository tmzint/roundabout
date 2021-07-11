use crate::handler::{MessageGroup, RuntimeContext};
use crate::message::bus::{MessageBus, MessageBusReceiver, MessageBusSender};
use crate::message::{MessageRegistry, MessageSender, ShutdownCommand, ShutdownRequestedEvent};
use crate::prelude::{MessageBusView, MessageVec};
use crate::util::cpu::CpuAffinity;
use crate::util::triple::{TripleBuffered, TripleBufferedHead, TripleBufferedTail};
use crate::wait::WaitingStrategy;
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
    // last group runs on the main thread
    groups: Vec<MessageGroup>,
    cpu_affinity: bool,
    min_bus_capacity: usize,
    shutdown_timeout: Duration,
    waiting_strategy: WaitingStrategy,
    shutdown: Arc<AtomicUsize>,
}

impl MessageScheduler {
    pub fn new<ER: Into<Arc<MessageRegistry>>>(
        registry: ER,
        groups: Vec<MessageGroup>,
        cpu_affinity: bool,
        min_bus_capacity: usize,
        shutdown_timeout: Duration,
        waiting_strategy: WaitingStrategy,
    ) -> Self {
        Self {
            registry: registry.into(),
            groups,
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

    pub fn schedule<E: 'static + Send + Sync>(mut self, init: E) {
        unsafe {
            let mut tails = Vec::with_capacity(self.groups.len());
            let mut handles = Vec::with_capacity(self.groups.len());

            let (bus_sender, mut bus_recvs) = MessageBus::bounded(
                self.registry.deref().to_owned(),
                self.min_bus_capacity,
                self.groups.len(),
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

            let main_cpu_affinity = cpu_affinities.pop().unwrap_or_default();
            let main_group = self.groups.pop();

            for group in self.groups {
                let bus_recv = bus_recvs.pop().unwrap();

                let (head, tail) = TripleBuffered::new([
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                    MessageVec::new(self.registry.deref().to_owned()),
                ]);
                tails.push(tail);

                let cpu = cpu_affinities.pop().unwrap_or_default();
                let shutdown = self.shutdown.clone();
                let registry = self.registry.deref().to_owned();
                let handle = thread::spawn(move || {
                    run_pipeline(group, registry, head, bus_recv, cpu, shutdown)
                });
                handles.push(handle);
            }

            if let Some(main_group) = main_group {
                let bus_recv = bus_recvs.pop().unwrap();

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

                let registry = self.registry.deref().to_owned();
                run_pipeline(
                    main_group,
                    registry,
                    head,
                    bus_recv,
                    main_cpu_affinity,
                    self.shutdown,
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
    group: MessageGroup,
    registry: MessageRegistry,
    head: TripleBufferedHead<MessageVec>,
    bus_recv: MessageBusReceiver,
    cpu: CpuAffinity,
    shutdown: Arc<AtomicUsize>,
) {
    let panic_shutdown = shutdown.clone();
    let panic_bomb = ShutdownPanicBomb(&panic_shutdown);
    cpu.apply_for_current();

    let message_sender = MessageSender::new(head);
    let shutdown_switch = ShutdownSwitch { shutdown };
    let context = RuntimeContext::new(registry, message_sender, shutdown_switch);
    let recv = MessageReceiver::new(bus_recv);

    group.start(recv, context);

    log::info!("pipeline shutdown");
    panic_bomb.disarm();
}

struct ShutdownPanicBomb<'a>(&'a AtomicUsize);

impl<'a> ShutdownPanicBomb<'a> {
    pub fn disarm(self) {
        std::mem::forget(self);
    }
}

impl<'a> Drop for ShutdownPanicBomb<'a> {
    fn drop(&mut self) {
        self.0.store(SHUTDOWN_ABORT, Ordering::Release);
    }
}

pub struct MessageReceiver {
    bus_recv: MessageBusReceiver,
    shutdown: bool,
}

impl MessageReceiver {
    fn new(bus_recv: MessageBusReceiver) -> Self {
        Self {
            bus_recv,
            shutdown: false,
        }
    }

    #[inline]
    pub fn stream<F: FnMut(&MessageBusView)>(mut self, mut f: F) {
        if self.shutdown {
            return;
        }

        loop {
            let message = self.bus_recv.recv();
            f(&message);

            if message.message_idx() == ShutdownCommand::MESSAGE_INDEX {
                return;
            }
        }
    }

    #[inline]
    pub fn recv_while<F: FnMut(&MessageBusView) -> bool>(
        &mut self,
        mut f: F,
    ) -> anyhow::Result<()> {
        loop {
            if self.shutdown {
                return Err(anyhow::anyhow!("message bus shutdown"));
            }

            let message = self.bus_recv.recv();
            if !f(&message) {
                return Ok(());
            }
            self.shutdown = message.message_idx() == ShutdownCommand::MESSAGE_INDEX;
        }
    }

    #[inline]
    pub fn recv(&mut self) -> anyhow::Result<MessageBusView> {
        if self.shutdown {
            return Err(anyhow::anyhow!("message bus shutdown"));
        }

        let message = self.bus_recv.recv();
        self.shutdown = message.message_idx() == ShutdownCommand::MESSAGE_INDEX;

        Ok(message)
    }

    #[inline]
    pub fn try_recv(&mut self) -> anyhow::Result<Option<MessageBusView>> {
        if self.shutdown {
            return Err(anyhow::anyhow!("message bus shutdown"));
        }

        if let Some(message) = self.bus_recv.try_recv() {
            self.shutdown = message.message_idx() == ShutdownCommand::MESSAGE_INDEX;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}
