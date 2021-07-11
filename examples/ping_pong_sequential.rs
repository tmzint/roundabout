use roundabout::prelude::*;

#[derive(Debug)]
pub struct PingEvent(u64);

#[derive(Debug)]
pub struct PongEvent(u64);

#[derive(Default)]
pub struct PingState {
    count: u64,
}

fn ping_handler(
    builder: OpenMessageHandlerBuilder<PingState>,
) -> InitMessageHandlerBuilder<PingState> {
    builder
        .on::<PingEvent>(|state, context, ping| {
            println!("Ping: {:?}", ping);
            context.sender().send(PongEvent(state.count));
            state.count += 1;
        })
        .init_default()
}

#[derive(Default)]
pub struct PongState {
    count: u64,
}

fn pong_handler(
    builder: OpenMessageHandlerBuilder<PongState>,
) -> InitMessageHandlerBuilder<PongState> {
    builder
        .on::<PongEvent>(|state, context, pong| {
            println!("Pong: {:?}", pong);
            state.count += 1;
            context.sender().send(PingEvent(state.count));
        })
        .init_default()
}

fn main() {
    Runtime::builder(512)
        .add(ping_handler)
        .add(pong_handler)
        .finish()
        .start(PingEvent(0));
}
