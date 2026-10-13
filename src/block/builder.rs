// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

use super::SIZEOF_U16;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.entry_may_be_added(key, value) {
            return false;
        }

        // offset for this new element is the current length of the data section
        let offset: u16 = self
            .data
            .len()
            .try_into()
            .expect("Offset cannot be over 65535 bytes.");

        self.offsets.push(offset);

        if self.is_empty() {
            // set first_key if this is the first entry of this block
            let mut keyvec = KeyVec::new();
            keyvec.set_from_slice(key);
            self.first_key = keyvec;
        }

        let key_len: u16 = key
            .len()
            .try_into()
            .expect("Key length may not be more than 65535 bytes.");

        let val_len: u16 = value
            .len()
            .try_into()
            .expect("Data length may not be more than 65535 bytes.");

        // add key_len (u16), then key bytes, then val_len (u16), then val_bytes
        self.data.extend(key_len.to_be_bytes());
        self.data.extend(key.raw_ref());
        self.data.extend(val_len.to_be_bytes());
        self.data.extend(value);

        true
    }

    fn entry_may_be_added(&self, key: KeySlice, value: &[u8]) -> bool {
        // Ensure the block won't exceed block_size after addition of this entry.
        // Only exception is if this block is empty and the single entry is larger than
        // block_size; in that case we allow the overflow.

        if self.is_empty() {
            return true;
        }

        let cur_size = self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16;

        // add u16 for the fields key_len, val_len, offset
        let additional_size = key.len() + SIZEOF_U16 + value.len() + SIZEOF_U16 + SIZEOF_U16;

        cur_size + additional_size <= self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
