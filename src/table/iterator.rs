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

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_idx = 0;
        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block(blk_idx)?);
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block(self.blk_idx)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = 0;
        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block(blk_idx)?);
        let mut iter = Self {
            table,
            blk_iter,
            blk_idx,
        };

        iter.seek_to_key(key)?;

        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // The first block that has last_key >= key is the block that we need
        let blk_idx = self
            .table
            .block_meta
            // partition_point requires the predicate be TRUE for the first partition and FALSE for the second partition. Hence invert the condition.
            .partition_point(|blockmeta| blockmeta.last_key.as_key_slice() < key);
        self.blk_idx = blk_idx;

        if blk_idx == self.table.block_meta.len() {
            // This SST doesn't contain the key
            // set blk_idx is an out-of-bounds index. This makes the iterator invalid as per is_valid.
            return Ok(());
        }

        self.blk_iter =
            BlockIterator::create_and_seek_to_key(self.table.read_block(self.blk_idx)?, key);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_idx < self.table.block_meta.len() && self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() - 1 {
            // if we have exhausted the current block iterator, move on to the next.
            // But first check if we have reached the very last block. If so, simply return Ok.
            // is_valid on Self will then indicate that the SsTableIterator is exhausted.
            self.blk_idx += 1;
        } else {
            return Ok(());
        }

        let next_block_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block(self.blk_idx)?);

        // put the iterator of the next block in place
        self.blk_iter = next_block_iter;
        Ok(())
    }
}
