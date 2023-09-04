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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {

class ThreadSafeSizeStatistics {
public:
    ThreadSafeSizeStatistics();
    ~ThreadSafeSizeStatistics();

private:
    ThreadSafeSizeStatistics(const ThreadSafeSizeStatistics &);
    ThreadSafeSizeStatistics &operator=(const ThreadSafeSizeStatistics &);

public:
    void addSize(int64_t size) {
        autil::ScopedWriteLock lock(_lock);
        _size += size;
    }

    void subSize(int64_t size) {
        autil::ScopedWriteLock lock(_lock);
        if (_size >= size) {
            _size -= size;
        } else {
            _size = 0;
        }
    }

    int64_t getSize() {
        autil::ScopedReadLock lock(_lock);
        return _size;
    }

private:
    mutable autil::ReadWriteLock _lock;
    int64_t _size;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ThreadSafeSizeStatistics);

} // namespace client
} // namespace swift
