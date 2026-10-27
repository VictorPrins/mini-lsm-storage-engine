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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let filter = &buf[..buf.len() - 1];
        let k = buf[buf.len() - 1];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.filter);
        buf.put_u8(self.k);
    }

    /// Get bloom filter bits per key from entries count and FPR
    /// This function computes the optimal number of bits of the filter, given this desired fpr and expected number of entries
    /// see https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions for the formulas
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size = -(entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        // size is the optimal number of bits of the filter
        // This function returns the number of 'bits per entry' (also called 'bits per key') so it divides again by entries
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        // 'keys' is a list of h_1 of all the keys. So this is actually a misnomer;
        // keys is not the keys themselves, but rather the first hash of each key
        // from this first hash we can compute all other hashes of this key

        // 0.69 == ln(2)
        // k is the optimal number of hash functions, given the number of bits per key (= filter size / entries)
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);

        // compute the optimal filter size
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;

        // initialize the filter with zeros
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // Build the Bloom filter for all keys
        // See the inline comments in may_contain() for an explanation of how we hash keys

        for key in keys {
            let mut h = *key;
            let delta = h.rotate_left(15);
            
            for _ in 0..k {
                let bit_idx = (h as usize) % nbits;

                filter.set_bit(bit_idx, true);
                h = h.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    /// h is the hash of a key
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();

            // We use the following paper to compute the k hash functions
            // https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
            // g_i(x) = h_1(x) + i * h_2(x)
            // where g_i(x) is the ith hash used for the bloom filter for key x.
            // in short: we need only two hashes (h_1 and h_2) and then add h_2 repeatedly to arrive at a new hash.
            // In this code, we compute h_2 (named 'delta') by rotating the bits of h_1. This is a simple way to construct a new hash
            // from another hash.
            let delta = h.rotate_left(15);

            for _ in 0..self.k {
                let bit_idx = (h as usize) % nbits;

                // if we encounter any bit that is not true in the filter, the bloom filter has returned a negative result
                // hence return false
                if !self.filter.get_bit(bit_idx) {
                    return false;
                }

                // compute the next hash
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}
