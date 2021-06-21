#![feature(option_result_unwrap_unchecked)]

use crate::event::EventRegistry;
use crate::handler::{
    BlockingEventHandlerBlueprint, EventGroupBuilder, EventHandlerBlueprint, EventHandlerBuilder,
    EventHandlerGroup,
};
use crate::schedule::{EventScheduler, ShutdownSwitch};
use crate::wait::WaitingStrategy;
use std::sync::Arc;
use std::time::Duration;

mod event;
mod handler;
pub mod prelude;
mod schedule;
mod util;
mod wait;

pub struct RuntimeBuilder {
    cpu_affinity: bool,
    min_bus_capacity: usize,
    shutdown_timeout: Duration,
    waiting_strategy: WaitingStrategy,
    registry: EventRegistry,
    groups: Vec<EventHandlerGroup>,
    primary: Option<EventHandlerGroup>,
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

    pub fn register_group<F>(mut self, group: F) -> Self
    where
        F: FnOnce(EventGroupBuilder) -> EventGroupBuilder,
    {
        let builder = group(EventGroupBuilder::new(&mut self.registry));
        self.groups.push(builder.finish());
        self
    }

    pub fn register<T: 'static, H>(self, handler: H) -> Self
    where
        H: Fn(EventHandlerBuilder<T>) -> EventHandlerBlueprint<T>,
    {
        self.register_group(move |g| g.register(handler))
    }

    pub fn register_blocking<T: 'static, H>(self, handler: H) -> Self
    where
        H: Fn(EventHandlerBuilder<T>) -> BlockingEventHandlerBlueprint<T>,
    {
        self.register_group(move |g| g.register_blocking(handler))
    }

    pub fn finish_group_primary<T: 'static, F>(mut self, group: F) -> Runtime
    where
        F: FnOnce(EventGroupBuilder) -> EventGroupBuilder,
    {
        assert!(self.primary.is_none());
        let builder = group(EventGroupBuilder::new(&mut self.registry));
        self.primary = Some(builder.finish());
        self.finish()
    }

    pub fn finish_primary<T: 'static, H>(mut self, handler: H) -> Runtime
    where
        H: Fn(EventHandlerBuilder<T>) -> EventHandlerBlueprint<T>,
    {
        assert!(self.primary.is_none());
        let builder = EventGroupBuilder::new(&mut self.registry).register(handler);
        self.primary = Some(builder.finish());
        self.finish()
    }

    // TODO: order of primary / blocking / register can be made more dynamic with type state pattern
    pub fn finish_primary_blocking<T: 'static, H>(mut self, handler: H) -> Runtime
    where
        H: Fn(EventHandlerBuilder<T>) -> BlockingEventHandlerBlueprint<T>,
    {
        assert!(self.primary.is_none());
        let builder = EventGroupBuilder::new(&mut self.registry).register_blocking(handler);
        self.primary = Some(builder.finish());
        self.finish()
    }

    pub fn finish(mut self) -> Runtime {
        for group in self.groups.iter_mut().chain(self.primary.iter_mut()) {
            group.fill_jmp_tbl(self.registry.len());
        }

        let registry = Arc::new(self.registry);
        let scheduler = EventScheduler::new(
            registry,
            self.groups,
            self.primary,
            self.cpu_affinity,
            self.min_bus_capacity,
            self.shutdown_timeout,
            self.waiting_strategy,
        );

        Runtime { scheduler }
    }
}

pub struct Runtime {
    scheduler: EventScheduler,
}

impl Runtime {
    pub fn builder(min_bus_capacity: usize) -> RuntimeBuilder {
        RuntimeBuilder {
            cpu_affinity: false,
            min_bus_capacity,
            shutdown_timeout: Duration::from_secs(10),
            waiting_strategy: Default::default(),
            registry: Default::default(),
            groups: vec![],
            primary: None,
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