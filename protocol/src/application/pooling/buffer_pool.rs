use parking_lot::Mutex;

use crate::application::constants::MAX_PACKET_BUFFER_SIZE;

/// Simple buffer pool for reusing packet serialization buffers
pub struct BufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    max_buffers: usize,
}

impl BufferPool {
    pub fn new(max_buffers: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::with_capacity(max_buffers)),
            max_buffers,
        }
    }

    pub fn get_buffer(&self) -> Vec<u8> {
        let mut buffers = self.buffers.lock();
        buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(MAX_PACKET_BUFFER_SIZE))
    }

    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        // Only keep reasonable-sized buffers to prevent memory bloat
        if buffer.capacity() <= MAX_PACKET_BUFFER_SIZE * 2 {
            let mut buffers = self.buffers.lock();
            if buffers.len() < self.max_buffers {
                buffers.push(buffer);
            }
        }
    }
}
