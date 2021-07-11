pub mod prelude;

mod handler;
mod message;
mod schedule;
mod util;
mod wait;

use crate::handler::{MessageGroup, MessageGroupBuilder, OpenMessageHandlerBuilder};
use crate::message::MessageRegistry;
use crate::schedule::{MessageScheduler, ShutdownSwitch};
use crate::wait::WaitingStrategy;
use std::sync::Arc;
use std::time::Duration;

pub struct RuntimeBuilder {
    cpu_affinity: bool,
    min_bus_capacity: usize,
    shutdown_timeout: Duration,
    waiting_strategy: WaitingStrategy,
    registry: MessageRegistry,
    // last group runs on the main thread
    groups: Vec<MessageGroup>,
}

impl RuntimeBuilder {
    pub fn with_cpu_affinity(mut self, cpu_affinity: bool) -> Self {
        self.cpu_affinity = cpu_affinity;
        self
    }

    pub fn with_waiting_strategy(mut self, waiting_strategy: WaitingStrategy) -> Self {
        self.waiting_strategy = waiting_strategy;
        self
    }

    // TODO: better ux // custom group builder type that allows init of single handler -> SingleGroupBuilder
    //  and modularization (allow creation of init orthogonal to closed?)
    pub fn add<T: 'static, FH, FT>(self, handler: FH, init: FT) -> Self
    where
        FH: FnOnce(OpenMessageHandlerBuilder<T>) -> OpenMessageHandlerBuilder<T>,
        FT: FnOnce() -> T + Send + 'static,
    {
        self.add_group(|mut group| {
            let hb = group.register(handler);

            group.init(move |recv, mut context| {
                let mut h = hb.init(&context, init()).unwrap();
                recv.stream(move |message| {
                    h.handle(&mut context, message);
                });
            })
        })
    }

    pub fn add_group<F>(mut self, group: F) -> Self
    where
        F: FnOnce(MessageGroupBuilder) -> MessageGroup,
    {
        let group = group(MessageGroupBuilder::new(&mut self.registry));
        self.groups.push(group);
        self
    }

    pub fn finish_main<T: 'static, FH, FT>(self, handler: FH, init: FT) -> Runtime
    where
        FH: FnOnce(OpenMessageHandlerBuilder<T>) -> OpenMessageHandlerBuilder<T>,
        FT: FnOnce() -> T + Send + 'static,
    {
        self.add(handler, init).finish()
    }

    pub fn finish_main_group<F>(self, group: F) -> Runtime
    where
        F: FnOnce(MessageGroupBuilder) -> MessageGroup,
    {
        self.add_group(group).finish()
    }

    pub fn finish(self) -> Runtime {
        let registry = Arc::new(self.registry);
        let scheduler = MessageScheduler::new(
            registry,
            self.groups,
            self.cpu_affinity,
            self.min_bus_capacity,
            self.shutdown_timeout,
            self.waiting_strategy,
        );

        Runtime { scheduler }
    }
}

pub struct Runtime {
    scheduler: MessageScheduler,
}

impl Runtime {
    pub fn builder(min_bus_capacity: usize) -> RuntimeBuilder {
        RuntimeBuilder {
            cpu_affinity: false,
            min_bus_capacity,
            shutdown_timeout: Duration::from_secs(10),
            waiting_strategy: Default::default(),
            registry: Default::default(),
            groups: Default::default(),
        }
    }

    pub fn get_shutdown_switch(&self) -> ShutdownSwitch {
        self.scheduler.get_shutdown_switch()
    }

    pub fn start<E: 'static + Send + Sync>(self, init: E) {
        log::info!("start runtime");
        self.scheduler.schedule(init);
    }
}
