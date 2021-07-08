use crate::message::MessageSize;
use std::alloc::Layout;

// Optimization: smaller size, e.g u16 instead of usize
pub(crate) struct MessageHeader {
    pub(crate) e_idx: usize,
    // Optimization: remove inline drop function and move to registry?
    pub(crate) drop_fn: Option<fn(*mut u8)>,
}

static_assertions::const_assert!(!std::mem::needs_drop::<MessageHeader>());

pub(crate) struct MessageBuffer {
    headers_start: *mut MessageHeader,
    messages_start: *mut u8,
    cap: usize,
    message_size: usize,
}

impl MessageBuffer {
    pub(crate) unsafe fn new(message_size: usize) -> Self {
        Self::dangling(message_size)
    }

    pub(crate) unsafe fn with_capacity(message_size: usize, cap: usize) -> Self {
        let mut buffer = Self::dangling(message_size);
        if cap > 0 {
            buffer.grow(cap);
        }
        buffer
    }

    unsafe fn dangling(message_size: usize) -> Self {
        let headers_start = Self::header_layout(0).align() as *mut MessageHeader;
        let messages_start = Self::message_layout(message_size, 0).align() as *mut u8;
        Self {
            headers_start,
            messages_start,
            cap: 0,
            message_size,
        }
    }

    fn header_layout(cap: usize) -> Layout {
        // same layout as message buss enables direct copying
        Layout::array::<MessageHeader>(cap).unwrap()
    }

    fn message_layout(message_size: usize, cap: usize) -> Layout {
        // same layout as message buss enables direct copying
        Layout::from_size_align(message_size * cap, MessageSize::MESSAGE_ALIGN).unwrap()
    }

    pub(crate) fn grow(&mut self, min: usize) {
        unsafe {
            let new_cap = (self.cap * 2).max(self.cap + min);

            let headers_layout = Self::header_layout(new_cap);
            let messages_layout = Self::message_layout(self.message_size, new_cap);

            if usize::BITS < 64
                && (headers_layout.size() > isize::MAX as usize
                    || messages_layout.size() > isize::MAX as usize)
            {
                panic!("message buffer capacity overflow");
            }

            let headers_start = std::alloc::alloc(headers_layout) as *mut MessageHeader;
            headers_start.copy_from_nonoverlapping(self.headers_start, self.cap);

            let messages_start = std::alloc::alloc(messages_layout);
            messages_start.copy_from_nonoverlapping(
                self.messages_start as *mut u8,
                self.message_size * self.cap,
            );

            if self.cap > 0 {
                std::alloc::dealloc(self.headers_start as *mut u8, Self::header_layout(self.cap));
                std::alloc::dealloc(
                    self.messages_start,
                    Self::message_layout(self.message_size, self.cap),
                );
            }

            self.headers_start = headers_start;
            self.messages_start = messages_start;
            self.cap = new_cap;
        }
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    pub(crate) unsafe fn get_header(&self, idx: usize) -> *mut MessageHeader {
        self.headers_start.add(idx)
    }

    pub(crate) unsafe fn get_message(&self, idx: usize) -> *mut u8 {
        self.messages_start.add(self.message_size * idx)
    }

    pub(crate) unsafe fn copy_nonoverlapping_all(
        &self,
        at: usize,
        headers: *mut MessageHeader,
        messages: *mut u8,
        len: usize,
    ) {
        debug_assert!(len <= self.cap - at);
        self.get_header(at).copy_from_nonoverlapping(headers, len);
        self.get_message(at)
            .copy_from_nonoverlapping(messages, self.message_size * len);
    }

    pub(crate) unsafe fn copy_nonoverlapping(
        &self,
        idx: usize,
        header: MessageHeader,
        message: *const u8,
        message_size: usize,
    ) {
        debug_assert!(message_size <= self.message_size);
        self.get_header(idx).write(header);
        self.get_message(idx)
            .copy_from_nonoverlapping(message, message_size);
    }

    pub(crate) unsafe fn drop_message(&self, idx: usize) {
        let header = &*self.get_header(idx);
        let message = self.get_message(idx);
        if let Some(drop) = header.drop_fn {
            drop(message);
        }
    }

    pub(crate) unsafe fn dealloc(&mut self) {
        std::alloc::dealloc(self.headers_start as *mut u8, Self::header_layout(self.cap));
        std::alloc::dealloc(
            self.messages_start,
            Self::message_layout(self.message_size, self.cap),
        );
    }
}
