use criterion::measurement::WallTime;
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion, Throughput};
use roundabout::prelude::*;
use std::time::{Duration, Instant};

struct Ping(u64);
struct Stop(bool);

pub fn ping_send(b: &mut Bencher<WallTime>, n: usize, t: usize, ws: WaitingStrategy) {
    let mut registry = EventRegistry::new();
    let _ping_id = registry.register_of::<Ping>();
    let stop_id = registry.register_of::<Stop>();

    b.iter_custom(|iters| {
        let (mut sender, receivers) = EventBus::bounded(registry.clone(), 1024, t, ws);
        let mut handles = vec![];

        for mut receiver in receivers {
            handles.push(std::thread::spawn(move || loop {
                let event = receiver.recv();
                if event.event_idx() == stop_id {
                    break;
                }
            }));
        }

        let n = n * iters as usize;
        let start = Instant::now();
        for i in 0..n {
            sender.send(Ping(i as u64));
        }

        let elapsed = start.elapsed();
        sender.send(Stop(false));
        sender.flush_padding();

        for handle in handles {
            handle.join().unwrap();
        }
        elapsed
    });
}

pub fn ping_send_all(b: &mut Bencher<WallTime>, n: usize, t: usize, ws: WaitingStrategy) {
    let mut registry = EventRegistry::new();
    let _ping_id = registry.register_of::<Ping>();
    let stop_id = registry.register_of::<Stop>();

    b.iter_custom(|iters| {
        let mut buffer = EventVec::with_capacity(registry.clone(), 10);
        let (mut sender, receivers) = EventBus::bounded(registry.clone(), 1024, t, ws);
        let mut handles = vec![];

        for mut receiver in receivers {
            handles.push(std::thread::spawn(move || loop {
                let event = receiver.recv();
                if event.event_idx() == stop_id {
                    break;
                }
            }));
        }

        let n = n * iters as usize;
        let start = Instant::now();
        for i in 0..n {
            buffer.extend([
                Ping(i as u64),
                Ping((i + 1) as u64),
                Ping((i + 2) as u64),
                Ping((i + 3) as u64),
                Ping((i + 4) as u64),
                Ping((i + 5) as u64),
                Ping((i + 6) as u64),
                Ping((i + 7) as u64),
                Ping((i + 8) as u64),
                Ping((i + 9) as u64),
            ]);
            sender.send_all(&mut buffer);
        }

        let elapsed = start.elapsed();
        sender.send(Stop(false));
        sender.flush_padding();

        for handle in handles {
            handle.join().unwrap();
        }
        elapsed
    });
}

pub fn event_bus_ping(c: &mut Criterion) {
    const N: usize = 50000;

    c.benchmark_group("EventBus: send")
        .throughput(Throughput::Elements(N as u64))
        .bench_function(format!("0 recv - {}", N), |b| {
            ping_send(b, black_box(N), 0, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("1 recv - {}", N), |b| {
            ping_send(b, black_box(N), 1, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("3 recv - {}", N), |b| {
            ping_send(b, black_box(N), 32, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("5 recv - {}", N), |b| {
            ping_send(b, black_box(N), 5, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("7 recv - {}", N), |b| {
            ping_send(b, black_box(N), 7, WaitingStrategy::SpinYield)
        });

    const BUFFER_N: usize = N / 10;
    assert_eq!(N % 10, 0);
    c.benchmark_group("EventBus: send_all (10 buffered)")
        .throughput(Throughput::Elements(N as u64))
        .bench_function(format!("0 recv - {}", N), |b| {
            ping_send_all(b, black_box(BUFFER_N), 0, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("1 recv - {}", N), |b| {
            ping_send_all(b, black_box(BUFFER_N), 1, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("3 recv - {}", N), |b| {
            ping_send_all(b, black_box(BUFFER_N), 3, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("5 recv - {}", N), |b| {
            ping_send_all(b, black_box(BUFFER_N), 5, WaitingStrategy::SpinYield)
        })
        .bench_function(format!("7 recv - {}", N), |b| {
            ping_send_all(b, black_box(BUFFER_N), 7, WaitingStrategy::SpinYield)
        });
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(20));
    targets = event_bus_ping
}
criterion_main!(benches);
