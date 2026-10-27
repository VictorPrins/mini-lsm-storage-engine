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
    /// Binary format of the data section of blocks:
    /// - len of first key (u16)
    /// - first key (varlen)
    /// - key-value pairs, with the following format:
    ///     - prefix overlap with the first key (u16)
    ///     - len of the rest of the key (u16)
    ///     - rest of the key (varlen)
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len: u16 = key
            .len()
            .try_into()
            .expect("Key length may not be more than 65535 bytes.");

        if self.is_empty() {
            // This is the first entry of this block
            self.first_key = key.to_key_vec();
            self.data.extend(key_len.to_be_bytes());
            self.data.extend(key.raw_ref());
        }

        let prefix_len = self.find_prefix_len(key);

        if !self.entry_may_be_added(prefix_len, key, value) {
            return false;
        }

        // offset for this new element is the current length of the data section
        let offset: u16 = self
            .data
            .len()
            .try_into()
            .expect("Offset cannot be over 65535 bytes.");

        self.offsets.push(offset);

        let val_len: u16 = value
            .len()
            .try_into()
            .expect("Data length may not be more than 65535 bytes.");

        let prefix_len_u16: u16 = prefix_len.try_into().unwrap();
        let rest_of_key_len: u16 = key_len - prefix_len_u16;

        // add key_prefix_len (u16), then key_remaining_len (u16), then remaining key bytes, then val_len (u16), then val_bytes
        self.data.extend(prefix_len_u16.to_be_bytes());
        self.data.extend(rest_of_key_len.to_be_bytes());
        self.data.extend(&key.raw_ref()[prefix_len..]);
        self.data.extend(val_len.to_be_bytes());
        self.data.extend(value);

        true
    }

    /// Computes the number of prefix bytes that the given key and self.first_key
    /// have in common
    fn find_prefix_len(&self, key: KeySlice) -> usize {
        key.raw_ref()
            .iter()
            .zip(self.first_key.raw_ref())
            .take_while(|(a, b)| a == b)
            .count()
    }

    fn entry_may_be_added(&self, prefix_len: usize, key: KeySlice, value: &[u8]) -> bool {
        // Ensure the block won't exceed block_size after addition of this entry.
        // Only exception is if this block is empty and the single entry is larger than
        // block_size; in that case we allow the overflow.

        if self.offsets.is_empty() {
            return true;
        }

        // add u16 at the end for the field meta_block_offset
        let cur_size = self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16;

        // add u16 for the fields key_prefix_len, rest_of_key_len, val_len, offset
        let additional_size = key.len() - prefix_len + value.len() + SIZEOF_U16 * 4;

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
