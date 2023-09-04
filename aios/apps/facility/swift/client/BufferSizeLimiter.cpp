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
#include "swift/client/BufferSizeLimiter.h"

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, BufferSizeLimiter);

BufferSizeLimiter::BufferSizeLimiter(int64_t maxSize) : _maxSize(maxSize), _curSize(0) {}

BufferSizeLimiter::~BufferSizeLimiter() { AUTIL_LOG(INFO, "destory limiter, current size [%ld]", _curSize); }

bool BufferSizeLimiter::addSize(int64_t size) {
    autil::ScopedWriteLock lock(_lock);
    if (_curSize + size > _maxSize) {
        return false;
    }
    _curSize += size;
    return true;
}

void BufferSizeLimiter::subSize(int64_t size) {
    autil::ScopedWriteLock lock(_lock);
    if (_curSize >= size) {
        _curSize -= size;
    } else {
        AUTIL_LOG(WARN, "cur size [%ld], sub size [%ld]", _curSize, size);
        _curSize = 0;
    }
}

int64_t BufferSizeLimiter::getCurSize() {
    autil::ScopedReadLock lock(_lock);
    return _curSize;
}
int64_t BufferSizeLimiter::getMaxSize() { return _maxSize; }

} // namespace client
} // namespace swift
