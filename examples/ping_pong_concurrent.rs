use roundabout::prelude::*;
use std::time::Instant;

pub struct InitEvent;
pub struct PingEvent(u64);
pub struct PongEvent(u64);

const INIT_N: usize = 64;

pub struct PingState {
    start: Instant,
    count: u64,
}

fn ping_handler(
    builder: OpenMessageHandlerBuilder<PingState>,
) -> OpenMessageHandlerBuilder<PingState> {
    builder
        .on::<InitEvent>(|_state, context, _init| {
            println!("init ping");
            for _ in 0..INIT_N {
                context.sender().send(PingEvent(1));
            }
        })
        .on::<PingEvent>(|state, context, ping| {
            state.count += ping.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Ping: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PongEvent(1));
        })
}

pub struct PongState {
    start: Instant,
    count: u64,
}

fn pong_handler(
    builder: OpenMessageHandlerBuilder<PongState>,
) -> OpenMessageHandlerBuilder<PongState> {
    builder
        .on::<InitEvent>(|_state, context, _init| {
            println!("init pong");
            for _ in 0..INIT_N {
                context.sender().send(PongEvent(1));
            }
        })
        .on::<PongEvent>(|state, context, pong| {
            state.count += pong.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Pong: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PingEvent(1));
        })
}

fn main() {
    let start = Instant::now();
    let runtime = Runtime::builder(512)
        .add(ping_handler, move || PingState { start, count: 0 })
        .add(pong_handler, move || PongState { start, count: 0 })
        .finish();

    let mut shutdown_switch = runtime.get_shutdown_switch();
    ctrlc::set_handler(move || {
        shutdown_switch.request_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    runtime.start(InitEvent);
}
