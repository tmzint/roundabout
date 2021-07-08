use roundabout::prelude::*;
use std::time::Instant;

pub struct PingMessage(u64);

pub struct BlockingState {
    start: Instant,
    count: u64,
}

fn blocking_handler(
    builder: MessageHandlerBuilder<BlockingState>,
    start: Instant,
) -> BlockingMessageHandlerBlueprint<BlockingState> {
    builder
        .on::<PingMessage>(|state, context, foo| {
            state.count += foo.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Foo: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PingMessage(1));
        })
        .with(BlockingState { start, count: 0 })
        .block(|pipeline| {
            // This is equivalent to a non blocking handler
            pipeline.trim_horizon_blocking();
        })
}

fn main() {
    let start = Instant::now();
    let runtime = Runtime::builder(128)
        .register_blocking(|b| blocking_handler(b, start))
        .finish();

    let mut shutdown_switch = runtime.get_shutdown_switch();
    ctrlc::set_handler(move || {
        shutdown_switch.request_shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    runtime.start(PingMessage(0));
}
