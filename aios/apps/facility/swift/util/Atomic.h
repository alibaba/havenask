/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>

#include "swift/common/Common.h"

namespace swift {
namespace util {

template <typename T>
class Atomic {
public:
    Atomic() : _value(0) {}

public:
    T get() { return __sync_val_compare_and_swap(&_value, 0, 0); }

    T getAndAdd(T x) { return __sync_fetch_and_add(&_value, x); }

    T addAndGet(T x) { return getAndAdd(x) + x; }

    bool testAndDecreament() {
        T val;
        while ((val = get()) > 0) {
            bool flag = __sync_bool_compare_and_swap(&_value, val, val - 1);
            if (flag) {
                return true;
            }
        }
        return false;
    }

    T incrementAndGet() { return addAndGet(1); }

    void add(T x) { addAndGet(x); }

    void increment() { incrementAndGet(); }

    T decrementAndGet() { return addAndGet(-1); }

    void decrement() { getAndAdd(-1); }

    T getAndSet(T x) { return __sync_lock_test_and_set(&_value, x); }

private:
    volatile T _value;
};

typedef Atomic<int16_t> Atomic16;
typedef Atomic<int32_t> Atomic32;
typedef Atomic<int64_t> Atomic64;
SWIFT_TYPEDEF_PTR(Atomic16);
SWIFT_TYPEDEF_PTR(Atomic32);
SWIFT_TYPEDEF_PTR(Atomic64);

} // namespace util
} // namespace swift
