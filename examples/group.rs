use roundabout::prelude::*;
use std::time::Instant;

pub struct InitMessage;
pub struct PingMessage(u64);
pub struct PongMessage(u64);

const INIT_N: usize = 3;

pub struct PingState {
    start: Instant,
    count: u64,
}

fn ping_handler(
    builder: MessageHandlerBuilder<PingState>,
    start: Instant,
) -> MessageHandlerBlueprint<PingState> {
    builder
        .on::<InitMessage>(|_state, context, _init| {
            println!("init ping");
            for _ in 0..INIT_N {
                context.sender().send(PingMessage(1));
            }
        })
        .on::<PingMessage>(|state, context, ping| {
            state.count += ping.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Ping: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PongMessage(1));
        })
        .with(PingState { start, count: 0 })
}

pub struct PongState {
    start: Instant,
    count: u64,
}

fn pong_handler(
    builder: MessageHandlerBuilder<PongState>,
    start: Instant,
) -> MessageHandlerBlueprint<PongState> {
    builder
        .on::<InitMessage>(|_state, context, _init| {
            println!("init pong");
            for _ in 0..INIT_N {
                context.sender().send(PongMessage(1));
            }
        })
        .on::<PongMessage>(|state, context, pong| {
            state.count += pong.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Pong: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PingMessage(1));
        })
        .with(PongState { start, count: 0 })
}

fn main() {
    let start = Instant::now();
    let runtime = Runtime::builder(128)
        .register_group(|group| {
            group
                .register(|b| ping_handler(b, start))
                .register(|b| pong_handler(b, start))
        })
        .finish();

    let mut shutdown_switch = runtime.get_shutdown_switch();
    ctrlc::set_handler(move || {
        shutdown_switch.request_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    runtime.start(InitMessage);
}
