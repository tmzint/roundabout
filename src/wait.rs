use std::time::Duration;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum WaitingStrategy {
    Spin,
    Yield,
    SpinYield,
    Sleep,
    // TODO: parking lot park & unpark_all
    // see:
    //  https://github.com/Amanieu/parking_lot/blob/master/core/src/parking_lot.rs
    //  https://github.com/Amanieu/parking_lot/blob/master/src/condvar.rs
    //  https://github.com/Amanieu/parking_lot/blob/master/src/once.rs
    //  https://github.com/Amanieu/parking_lot/blob/master/lock_api/src/mutex.rs
    //  https://github.com/Amanieu/parking_lot/blob/master/core/src/spinwait.rs
}

impl WaitingStrategy {
    // TODO: move condition into wait? => park will need this?
    #[inline]
    pub fn waiter(&self) -> Waiter {
        match self {
            WaitingStrategy::Spin => Waiter::Spin(SpinWait::default()),
            WaitingStrategy::Yield => Waiter::Yield,
            WaitingStrategy::SpinYield => Waiter::SpinYield(SpinYieldWait::default()),
            WaitingStrategy::Sleep => Waiter::Sleep,
        }
    }
}

impl Default for WaitingStrategy {
    #[inline]
    fn default() -> Self {
        Self::Yield
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Waiter {
    Spin(SpinWait),
    Yield,
    SpinYield(SpinYieldWait),
    Sleep,
}

impl Waiter {
    #[inline]
    pub fn wait(&mut self) {
        match self {
            Waiter::Spin(s) => s.wait(),
            Waiter::Yield => std::thread::yield_now(),
            Waiter::SpinYield(sy) => sy.wait(),
            Waiter::Sleep => std::thread::sleep(Duration::from_nanos(1)),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        match self {
            Waiter::Spin(s) => {
                s.counter = 0;
            }
            Waiter::Yield => {}
            Waiter::SpinYield(sy) => {
                sy.counter = 0;
            }
            Waiter::Sleep => {}
        }
    }
}

#[derive(Default, Copy, Clone, Eq, PartialEq)]
pub struct SpinWait {
    counter: usize,
}

impl SpinWait {
    #[inline]
    pub fn wait(&mut self) {
        if self.counter <= 3 {
            self.counter += 1;
        }

        let iterations = 1 << self.counter;
        for _ in 0..iterations {
            std::hint::spin_loop();
        }
    }
}

#[derive(Default, Copy, Clone, Eq, PartialEq)]
pub struct SpinYieldWait {
    counter: usize,
}

impl SpinYieldWait {
    #[inline]
    pub fn wait(&mut self) {
        self.counter += 1;
        if self.counter <= 3 {
            let iterations = 1 << self.counter;
            for _ in 0..iterations {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
    }
}
