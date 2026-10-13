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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // add 2 bytes at the end for the num_entries field (u16)
        let len = self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16;

        let mut buffer = BytesMut::with_capacity(len);

        // add data section to buffer
        buffer.put_slice(&self.data);

        // add offset section to buffer
        for offset in &self.offsets {
            buffer.put_u16(*offset);
        }

        // add final u16 to the buffer that specifies the number of entries in this block
        buffer.put_u16(
            self.offsets
                .len()
                .try_into()
                .expect("Number of entries should never be larger than u16::MAX 65535"),
        );

        buffer.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let idx_n_entries = data.len() - SIZEOF_U16;

        // extract the final field that specifies the number of entries in this block
        let n_entries: [u8; 2] = data[idx_n_entries..].try_into().unwrap();

        let n_entries: usize = u16::from_be_bytes(n_entries) as usize;

        // compute the start idx of the offset section
        let idx_offset_section = idx_n_entries - n_entries * SIZEOF_U16;

        let offsets_section = &data[idx_offset_section..idx_n_entries];
        let data_section = &data[..idx_offset_section];

        let mut offsets_vec: Vec<u16> = Vec::with_capacity(n_entries);

        for i in 0..n_entries {
            let o: [u8; 2] = offsets_section[i * SIZEOF_U16..i * SIZEOF_U16 + SIZEOF_U16]
                .try_into()
                .unwrap();
            let o: u16 = u16::from_be_bytes(o);
            offsets_vec.push(o);
        }

        Block {
            data: Vec::from(data_section),
            offsets: offsets_vec,
        }
    }
}
