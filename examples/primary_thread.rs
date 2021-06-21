use roundabout::prelude::*;
use std::time::Instant;

pub struct PingEvent(u64);

pub struct BlockingState {
    start: Instant,
    count: u64,
}

fn foo_handler(
    builder: EventHandlerBuilder<BlockingState>,
    start: Instant,
) -> EventHandlerBlueprint<BlockingState> {
    builder
        .on::<PingEvent>(|state, context, foo| {
            state.count += foo.0;
            if state.count % 10000 == 0 {
                let elapsed = state.start.elapsed();
                println!(
                    "Ping: {} @ {:.2}/s, ",
                    state.count,
                    state.count as f64 / elapsed.as_secs_f64()
                );
            }

            context.sender().send(PingEvent(1));
        })
        .with(BlockingState { start, count: 0 })
}

fn main() {
    let start = Instant::now();
    let runtime = Runtime::builder(128).finish_primary(|b| foo_handler(b, start));

    let mut shutdown_switch = runtime.get_shutdown_switch();
    ctrlc::set_handler(move || {
        shutdown_switch.shutdown();
    })
    .expect("Error setting Ctrl-C handler");

    runtime.start(PingEvent(0));
}
