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

#include "autil/Log.h"

namespace swift {
namespace util {
template <typename T>
class Atomic;
} // namespace util
} // namespace swift

namespace swift {
namespace util {

template <typename T>
class ScopedAtomic {
public:
    ScopedAtomic(Atomic<T> *atomic) : _atomic(atomic) { _atomic->increment(); }
    ~ScopedAtomic() { _atomic->decrement(); };

private:
    ScopedAtomic(const ScopedAtomic &);
    ScopedAtomic &operator=(const ScopedAtomic &);

public:
private:
    Atomic<T> *_atomic;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
