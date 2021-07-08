use crate::message::buffer::{MessageBuffer, MessageHeader};
use crate::message::MessageRegistry;
use std::mem::ManuallyDrop;
use std::ops::Deref;

pub struct MessageVec {
    // Optimization: clone vs arc vs reference
    registry: MessageRegistry,
    buffer: MessageBuffer,
    len: usize,
}

impl MessageVec {
    #[inline]
    pub fn new(registry: MessageRegistry) -> Self {
        unsafe {
            let buffer = MessageBuffer::new(registry.message_size().inner());
            Self {
                registry,
                buffer,
                len: 0,
            }
        }
    }

    #[inline]
    pub fn with_capacity(registry: MessageRegistry, cap: usize) -> Self {
        unsafe {
            let buffer = MessageBuffer::with_capacity(registry.message_size().inner(), cap);
            Self {
                registry,
                buffer,
                len: 0,
            }
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn iter(&self) -> MessageVecIter {
        MessageVecIter { vec: self, i: 0 }
    }

    #[inline]
    pub fn push<T: 'static + Send + Sync>(&mut self, message: T) -> bool {
        unsafe {
            // Optimization: static resolution of e_idx
            match self.registry.get_index_of::<T>() {
                Some(e_idx) => {
                    let message = ManuallyDrop::new(message);
                    let data = message.deref() as *const T as *const u8;
                    let drop_fn: Option<fn(*mut u8)> = if std::mem::needs_drop::<T>() {
                        Some(|ptr| (ptr as *mut T).drop_in_place())
                    } else {
                        None
                    };

                    self.push_untyped(e_idx, data, std::mem::size_of::<T>(), drop_fn);
                    true
                }
                None => {
                    log::debug!(
                        "skipping storing of unhandled message type: {}",
                        std::any::type_name::<T>()
                    );
                    false
                }
            }
        }
    }

    #[inline]
    pub fn extend<I: IntoIterator<Item = T>, T: 'static + Send + Sync>(
        &mut self,
        messages: I,
    ) -> bool {
        unsafe {
            match self.registry.get_index_of::<T>() {
                Some(e_idx) => {
                    for message in messages.into_iter() {
                        let message: ManuallyDrop<T> = ManuallyDrop::new(message);
                        let data = message.deref() as *const T as *const u8;
                        let drop_fn: Option<fn(*mut u8)> = if std::mem::needs_drop::<T>() {
                            Some(|ptr| (ptr as *mut T).drop_in_place())
                        } else {
                            None
                        };

                        self.push_untyped(e_idx, data, std::mem::size_of::<T>(), drop_fn);
                    }

                    true
                }
                None => {
                    log::debug!(
                        "skipping storing of unhandled message type: {}",
                        std::any::type_name::<T>()
                    );
                    false
                }
            }
        }
    }

    #[inline]
    pub fn extend_vec(&mut self, other: &mut Self) {
        unsafe {
            assert_eq!(self.registry, other.registry);
            self.extend_vec_unchecked(other);
        }
    }

    #[inline]
    pub unsafe fn extend_vec_unchecked(&mut self, other: &mut Self) {
        debug_assert_eq!(self.registry, other.registry);
        if other.is_empty() {
            return;
        }

        let remaining = self.buffer.cap() - self.len;
        if remaining < other.len {
            self.buffer.grow(other.len - remaining);
        }

        let headers = other.buffer.get_header(0);
        let messages = other.buffer.get_message(0);
        self.buffer
            .copy_nonoverlapping_all(self.len, headers, messages, other.len);

        self.len += other.len;
        other.len = 0;
    }

    pub(crate) fn get_registry(&self) -> &MessageRegistry {
        &self.registry
    }

    pub(crate) unsafe fn get_buffer(&self) -> &MessageBuffer {
        &self.buffer
    }

    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        self.len = len;
    }

    pub(crate) unsafe fn push_untyped(
        &mut self,
        e_idx: usize,
        data: *const u8,
        data_size: usize,
        drop_fn: Option<fn(*mut u8)>,
    ) {
        if self.buffer.cap() - self.len == 0 {
            self.buffer.grow(1);
        }

        let header = MessageHeader { e_idx, drop_fn };
        self.buffer
            .copy_nonoverlapping(self.len, header, data, data_size);
        self.len += 1;
    }
}

impl Drop for MessageVec {
    fn drop(&mut self) {
        unsafe {
            if self.buffer.cap() == 0 {
                return;
            }

            for i in 0..self.len {
                self.buffer.drop_message(i);
            }

            self.buffer.dealloc();
        }
    }
}

unsafe impl Send for MessageVec {}
unsafe impl Sync for MessageVec {}

pub struct MessageVecIter<'a> {
    vec: &'a MessageVec,
    i: usize,
}

impl<'a> Iterator for MessageVecIter<'a> {
    type Item = MessageVecView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.i >= self.vec.len {
                return None;
            }

            let header = &*self.vec.buffer.get_header(self.i);
            let data = self.vec.buffer.get_message(self.i);
            self.i += 1;
            Some(MessageVecView { header, data })
        }
    }
}

pub struct MessageVecView<'a> {
    header: &'a MessageHeader,
    data: *const u8,
}

impl<'a> MessageVecView<'a> {
    #[inline]
    pub fn message_idx(&self) -> usize {
        self.header.e_idx
    }

    #[inline]
    pub unsafe fn data(&self) -> *const u8 {
        self.data
    }
}
