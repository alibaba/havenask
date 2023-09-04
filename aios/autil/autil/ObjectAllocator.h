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

#include <new>
#include <stddef.h>

#include "autil/FixedSizeAllocator.h"

namespace autil {

template <typename T>
class ObjectAllocator {
public:
    ObjectAllocator() : allocator(sizeof(T)) {}
    ~ObjectAllocator() {}

public:
    T *allocate() { return new (allocator.allocate()) T(); }
    void free(T *t) {
        t->~T();
        allocator.free((void *)t);
    }
    size_t getCount() const { return allocator.getCount(); }

private:
    FixedSizeAllocator allocator;

private:
    friend class ObjectAllocatorTest;
};

} // namespace autil
