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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    current: Current,
}

enum Current {
    A,
    B,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        // a takes precedence over b in case of equal keys
        let current = if !b.is_valid() || a.is_valid() && a.key() <= b.key() {
            Current::A
        } else {
            Current::B
        };

        Ok(Self { a, b, current })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.current {
            Current::A => self.a.key(),
            Current::B => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current {
            Current::A => self.a.value(),
            Current::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.current {
            Current::A => self.a.is_valid(),
            Current::B => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.current {
            Current::A => {
                if self.b.is_valid() && self.b.key() == self.a.key() {
                    // Only advance b if the keys are currently equal.
                    // If not, that means that b is already greater than a. In this case we don't need to advance b
                    // if b is invalid, then we only advance a
                    self.b.next()?;
                }

                // Always advance the current. Current must be valid, since the contract requires that next() only be called if self.is_valid() == true.
                self.a.next()?;
            }
            Current::B => {
                // we never have to advance a in this case, because we already know that
                // a > b (or a is invalid), because in the equality case a would have taken precedence
                // and we wouldn't be in this arm.
                self.b.next()?;
            }
        }

        // Set the current
        if !self.a.is_valid() {
            // Invalid iter always loses
            self.current = Current::B;
        } else if self.b.is_valid() && self.b.key() < self.a.key() {
            // set current to the iter with the lowest key. a takes precedence in case of key equality.
            self.current = Current::B;
        } else {
            self.current = Current::A;
        }

        Ok(())
    }
}
