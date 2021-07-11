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
) -> OpenMessageHandlerBuilder<PingState> {
    builder.on::<PingEvent>(|state, context, ping| {
        println!("Ping: {:?}", ping);
        context.sender().send(PongEvent(state.count));
        state.count += 1;
    })
}

#[derive(Default)]
pub struct PongState {
    count: u64,
}

fn pong_handler(
    builder: OpenMessageHandlerBuilder<PongState>,
) -> OpenMessageHandlerBuilder<PongState> {
    builder.on::<PongEvent>(|state, context, pong| {
        println!("Pong: {:?}", pong);
        state.count += 1;
        context.sender().send(PingEvent(state.count));
    })
}

fn main() {
    Runtime::builder(512)
        .add(ping_handler, Default::default)
        .add(pong_handler, Default::default)
        .finish()
        .start(PingEvent(0));
}
