use criterion::measurement::WallTime;
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion, Throughput};
use roundabout::prelude::*;
use std::time::{Duration, Instant};

#[derive(Copy, Clone)]
pub struct PingEvent(usize);

pub struct PingState {
    counter: usize,
    i: usize,
    n: usize,
    t: usize,
}

pub fn ping_sequential(b: &mut Bencher<WallTime>, n: usize, t: usize) {
    b.iter_custom(|iters| {
        let n = iters as usize * n;
        let mut runtime_builder = Runtime::builder(512);
        for i in 0..t {
            runtime_builder = runtime_builder.add(
                |b: OpenMessageHandlerBuilder<PingState>| {
                    b.on::<PingEvent>(|state, context, ping| {
                        if state.counter == state.n {
                            context.shutdown_switch().request_shutdown();
                        }

                        if ping.0 % state.t == state.i {
                            context.sender().send(PingEvent(ping.0 + 1));
                        }

                        state.counter += 1;
                    })
                },
                move || PingState {
                    counter: 0,
                    i,
                    n,
                    t,
                },
            );
        }

        let runtime = runtime_builder.finish();
        // TODO: this also measures startup / shutdown -> requires to get state from runtime // they are not send
        let start = Instant::now();
        runtime.start(PingEvent(0));
        start.elapsed()
    })
}

pub struct InitEvent(usize);

pub fn ping_concurrent(b: &mut Bencher<WallTime>, n: usize, t: usize, in_flight: usize) {
    assert_eq!(in_flight % t, 0);

    b.iter_custom(|iters| {
        let n = iters as usize * n;
        let mut runtime_builder = Runtime::builder(1024);
        for i in 0..t {
            runtime_builder = runtime_builder.add(
                |b: OpenMessageHandlerBuilder<PingState>| {
                    b.on::<InitEvent>(|state, context, init| {
                        for _ in 0..init.0 {
                            context.sender().send(PingEvent(state.i));
                        }
                    })
                    .on::<PingEvent>(|state, context, ping| {
                        if state.counter == state.n {
                            context.shutdown_switch().request_shutdown();
                        }

                        if ping.0 % state.t == state.i {
                            context.sender().send(PingEvent(ping.0 + 1));
                        }

                        state.counter += 1;
                    })
                },
                move || PingState {
                    counter: 0,
                    i,
                    n,
                    t,
                },
            );
        }

        let runtime = runtime_builder.finish();
        // TODO: this also measures startup / shutdown -> requires to get state from runtime // they are not send
        let start = Instant::now();
        runtime.start(InitEvent(in_flight / t));
        start.elapsed()
    })
}

pub fn runtime_ping(c: &mut Criterion) {
    const N: usize = 50000;

    c.benchmark_group("Runtime: sequential")
        .throughput(Throughput::Elements(N as u64))
        .bench_function(format!("sequential (1 recv) - {}", N), |b| {
            ping_sequential(b, black_box(N), 1)
        })
        .bench_function(format!("sequential (3 recv) - {}", N), |b| {
            ping_sequential(b, black_box(N), 3)
        })
        .bench_function(format!("sequential (5 recv) - {}", N), |b| {
            ping_sequential(b, black_box(N), 5)
        })
        .bench_function(format!("sequential (7 recv) - {}", N), |b| {
            ping_sequential(b, black_box(N), 7)
        });

    c.benchmark_group("Runtime: concurrent (210 messages in flight)")
        .throughput(Throughput::Elements(N as u64))
        .bench_function(format!("concurrent (1 recv) - {}", N), |b| {
            ping_concurrent(b, black_box(N), 1, 210)
        })
        .bench_function(format!("concurrent (3 recv) - {}", N), |b| {
            ping_concurrent(b, black_box(N), 3, 210)
        })
        .bench_function(format!("concurrent (5 recv) - {}", N), |b| {
            ping_concurrent(b, black_box(N), 5, 210)
        })
        .bench_function(format!("concurrent (7 recv) - {}", N), |b| {
            ping_concurrent(b, black_box(N), 7, 210)
        });
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(20));
    targets = runtime_ping
}
criterion_main!(benches);
