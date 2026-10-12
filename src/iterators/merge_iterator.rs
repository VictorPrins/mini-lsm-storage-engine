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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter))
            }
        }

        let current = heap.pop();

        MergeIterator {
            iters: heap,
            current: current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current
            .as_ref()
            .expect("This method may only be called if iterator is valid.")
            .1
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("This method may only be called if iterator is valid.")
            .1
            .value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {

        // Core invariant: everything in the heap and in current is always valid before and after this function

        let cur_iter = &mut self.current.as_mut().unwrap().1;
        let cur_key = cur_iter.key();

        // advance the heap iterators to the point where the top key is not equal to the current key
        while let Some(mut inner) = self.iters.peek_mut() {
            let top_iter = &mut inner.1;

            if top_iter.key() != cur_key {
                // exit the loop because the heap no longer contains any keys that are equal to cur_key
                break;
            }

            if let Err(e) = top_iter.next() {
                // propagate up any errors. First pop, otherwise the code may panic when PeekMut is dropped at the end of the loop scope, because Drop accesses the top iterator which has just errored.
                PeekMut::pop(inner);
                return Err(e);
            }

            if !top_iter.is_valid() {
                // maintain core invariant: all iterators in our data structure must be valid at all times. So pop off exhausted iterators.
                PeekMut::pop(inner);
            }

            // at the end of every iteration, PeekMut is dropped and the heap automatically rebalanced
        }

        // advance the current iterator
        cur_iter.next()?;

        if !cur_iter.is_valid() {
            // if the current iterator is exhausted, then replace it with the top of the heap.
            // if the heap is also exhausted, then Pop returns None which is the appropriate value to put in self.current in this case.
            self.current = self.iters.pop();
            return Ok(());
        }

        // finally check which is less: the cur_iter or the top of the heap.
        // Swap accordingly, because the least of the two must be in self.current.
        // Note: Because the heap uses reverse ordering we compare with >
        if let Some(inner) = self.iters.peek()
            && inner > self.current.as_mut().unwrap()
        {
            let top_iter = self.iters.pop();
            self.iters.push(self.current.take().unwrap());
            self.current = top_iter;
        }

        Ok(())
    }
}
