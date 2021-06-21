use crate::event::EventSize;
use std::alloc::Layout;

// Optimization: smaller size, e.g u16 instead of usize
pub(crate) struct EventHeader {
    pub(crate) e_idx: usize,
    // Optimization: remove inline drop function and move to registry?
    pub(crate) drop_fn: Option<fn(*mut u8)>,
}

static_assertions::const_assert!(!std::mem::needs_drop::<EventHeader>());

pub(crate) struct EventBuffer {
    headers_start: *mut EventHeader,
    events_start: *mut u8,
    cap: usize,
    event_size: usize,
}

impl EventBuffer {
    pub(crate) unsafe fn new(event_size: usize) -> Self {
        Self::dangling(event_size)
    }

    pub(crate) unsafe fn with_capacity(event_size: usize, cap: usize) -> Self {
        let mut buffer = Self::dangling(event_size);
        if cap > 0 {
            buffer.grow(cap);
        }
        buffer
    }

    unsafe fn dangling(event_size: usize) -> Self {
        let headers_start = Self::header_layout(0).align() as *mut EventHeader;
        let events_start = Self::event_layout(event_size, 0).align() as *mut u8;
        Self {
            headers_start,
            events_start,
            cap: 0,
            event_size,
        }
    }

    fn header_layout(cap: usize) -> Layout {
        // same layout as event buss enables direct copying
        Layout::array::<EventHeader>(cap).unwrap()
    }

    fn event_layout(event_size: usize, cap: usize) -> Layout {
        // same layout as event buss enables direct copying
        Layout::from_size_align(event_size * cap, EventSize::EVENT_ALIGN).unwrap()
    }

    pub(crate) fn grow(&mut self, min: usize) {
        unsafe {
            let new_cap = (self.cap * 2).max(self.cap + min);

            let headers_layout = Self::header_layout(new_cap);
            let events_layout = Self::event_layout(self.event_size, new_cap);

            if usize::BITS < 64
                && (headers_layout.size() > isize::MAX as usize
                    || events_layout.size() > isize::MAX as usize)
            {
                panic!("event buffer capacity overflow");
            }

            let headers_start = std::alloc::alloc(headers_layout) as *mut EventHeader;
            headers_start.copy_from_nonoverlapping(self.headers_start, self.cap);

            let events_start = std::alloc::alloc(events_layout);
            events_start
                .copy_from_nonoverlapping(self.events_start as *mut u8, self.event_size * self.cap);

            if self.cap > 0 {
                std::alloc::dealloc(self.headers_start as *mut u8, Self::header_layout(self.cap));
                std::alloc::dealloc(
                    self.events_start,
                    Self::event_layout(self.event_size, self.cap),
                );
            }

            self.headers_start = headers_start;
            self.events_start = events_start;
            self.cap = new_cap;
        }
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    pub(crate) unsafe fn get_header(&self, idx: usize) -> *mut EventHeader {
        self.headers_start.add(idx)
    }

    pub(crate) unsafe fn get_event(&self, idx: usize) -> *mut u8 {
        self.events_start.add(self.event_size * idx)
    }

    pub(crate) unsafe fn copy_nonoverlapping_all(
        &self,
        at: usize,
        headers: *mut EventHeader,
        events: *mut u8,
        len: usize,
    ) {
        debug_assert!(len <= self.cap - at);
        self.get_header(at).copy_from_nonoverlapping(headers, len);
        self.get_event(at)
            .copy_from_nonoverlapping(events, self.event_size * len);
    }

    pub(crate) unsafe fn copy_nonoverlapping(
        &self,
        idx: usize,
        header: EventHeader,
        event: *const u8,
        event_size: usize,
    ) {
        debug_assert!(event_size <= self.event_size);
        self.get_header(idx).write(header);
        self.get_event(idx)
            .copy_from_nonoverlapping(event, event_size);
    }

    pub(crate) unsafe fn drop_event(&self, idx: usize) {
        let header = &*self.get_header(idx);
        let event = self.get_event(idx);
        if let Some(drop) = header.drop_fn {
            drop(event);
        }
    }

    pub(crate) unsafe fn dealloc(&mut self) {
        std::alloc::dealloc(self.headers_start as *mut u8, Self::header_layout(self.cap));
        std::alloc::dealloc(
            self.events_start,
            Self::event_layout(self.event_size, self.cap),
        );
    }
}
