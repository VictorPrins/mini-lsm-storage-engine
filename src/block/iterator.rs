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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let num_elements = self.block.offsets.len();
        if !self.is_valid() || self.idx == num_elements - 1 {
            // Iterator is not valid or last element of this block has been reached.
            // Set key to empty value to signify this iterator has been exhausted
            self.key = KeyVec::new();
            return;
        }

        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {

        // binary search to the first entry that has a key that is greater than or equal to the provided key
        let idx = self
            .block
            .offsets
            .partition_point(|&offset| self.key_at_offset(offset) < key);

        if idx == self.block.offsets.len() {
            // all keys in this block are less than key.
            // hence this iterator is exhausted, which we signify by setting key to empty value.
            self.key = KeyVec::new();
            return;
        }

        self.seek_to_idx(idx);
    }

    fn seek_to_idx(&mut self, idx: usize) {
        let offset = self.block.offsets[idx] as usize;
        let mut buf = &self.block.data[offset..];

        // key_len is first 2 bytes
        let key_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        buf = &buf[2..];

        // key is next (key_len) bytes
        let key = &buf[..key_len];
        buf = &buf[key_len..];

        // val_len is next 2 bytes
        let val_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;

        // Set all values on self
        self.idx = idx;
        self.key = KeySlice::from_slice(key).to_key_vec();

        let val_start = offset + 2 + key_len + 2;
        self.value_range = (val_start, val_start + val_len)
    }

    fn key_at_offset(&self, offset: u16) -> KeySlice<'_> {
        let data = &self.block.data[offset as usize..];

        // key_len is first 2 bytes
        let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;

        // key is next (key_len) bytes
        let key = &data[2..2 + key_len];

        KeySlice::from_slice(key)
    }
}
