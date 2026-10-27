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
use std::{path::Path, vec};

use super::{BlockMeta, SsTable};
use crate::key::KeyVec;
use crate::table::FileObject;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    // builder contains the block that is currently being built
    builder: BlockBuilder,
    // the first and last key of the block that is currently being built. These are temporary values that are needed when building BlockMeta when completing the block.
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    // data contains the blocks that are completed
    data: Vec<u8>,
    // contains the meta entries for all completed blocks
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Keys MUST be supplied in a strictly increasing order! This upholds the core invariant that SSTs (and its constituent blocks) are sorted.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // add the block. If it returns false, that means the block is full.
        if !self.builder.add(key, value) {
            self.complete_current_block();
            // recursively call add. This should never lead to infinite recursion since we have just created a fresh block
            self.add(key, value);
            return;
        }

        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        // Always set the last key to the most recently added key
        self.last_key = key.raw_ref().to_vec();

        // add the hash of the key, to be used for the Bloom filter
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
    }

    fn complete_current_block(&mut self) {
        // Init a new fields on the SsTableBuilder, and take the completed ones out
        let completed_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let first_key = std::mem::replace(&mut self.first_key, vec![]);

        let last_key = std::mem::replace(&mut self.last_key, vec![]);

        // add the completed block to the SST data buffer
        let block_bytes = completed_builder.build().encode();

        let offset = self.data.len();

        self.data.extend(block_bytes);

        // add a metadata entry for the completed block
        self.meta.push(BlockMeta {
            offset,
            first_key: KeyVec::from_vec(first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(last_key).into_key_bytes(),
        });
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.complete_current_block();

        // SST file format: [[block data] [block meta] [meta block offset (u32)] [bloom filter] [bloom filter offset (u32)]]
        let mut file_data = self.data;
        let meta_block_offset = u32::try_from(file_data.len()).unwrap();
        BlockMeta::encode_block_meta(&self.meta, &mut file_data);
        file_data.extend(meta_block_offset.to_be_bytes());

        let bloom_filter_offset = u32::try_from(file_data.len()).unwrap();

        const FALSE_POSITIVE_RATE: f64 = 0.01;

        let bloom_filter = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), FALSE_POSITIVE_RATE),
        );

        bloom_filter.encode(&mut file_data);
        file_data.extend(bloom_filter_offset.to_be_bytes());

        let file = FileObject::create(path.as_ref(), file_data)?;

        Ok(SsTable::open(id, block_cache, file)?)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
