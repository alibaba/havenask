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
#include "fslib/cache/FileBlockPool.h"

#include <errno.h>
#include <unistd.h>

FSLIB_BEGIN_NAMESPACE(cache);
AUTIL_DECLARE_AND_SETUP_LOGGER(cache, FileBlockPool);

FileBlockPool::FileBlockPool(int64_t bufferSize, size_t blockSize, bool forceAllocate) {
    AUTIL_LOG(INFO, "buffer size [%ld], block size [%ld]", bufferSize, blockSize);
    assert(bufferSize > 0);
    assert(blockSize > 0);
    _forceAllocate = forceAllocate;
    _blockSize = blockSize;
    _maxBlockCount = (bufferSize + blockSize - 1) / blockSize;
    _blockCount = 0;
    AUTIL_LOG(INFO, "create blockPool, blockcount %ld, maxBlockCount %ld", _blockCount, _maxBlockCount);
    _bufferVec.reserve(_maxBlockCount);
}

FileBlockPool::~FileBlockPool() {
    assert(_blockCount == 0);
    for (size_t i = 0; i < _bufferVec.size(); ++i) {
        free(_bufferVec[i]);
    }
    _bufferVec.clear();
}

int32_t FileBlockPool::allocate(void *&buf) {
    buf = NULL;
    bool canAllocate = false;
    {
        autil::ScopedLock lock(_mutex);
        if (_blockCount < _maxBlockCount || _forceAllocate) {
            ++_blockCount;
            canAllocate = true;
        }
    }

    if (!canAllocate) {
        AUTIL_LOG(DEBUG, "allocate block failed!exceed max memory pool size.");
        return ENOMEM;
    }

    {
        autil::ScopedLock lock(_mutex);
        if (!_bufferVec.empty()) {
            buf = _bufferVec.back();
            _bufferVec.pop_back();
        }
    }
    if (buf == NULL) {
        size_t alignment = getpagesize();
        int32_t errCode = posix_memalign(&buf, alignment, _blockSize);
        return errCode;
    }
    return 0;
}

void FileBlockPool::deAllocate(void *&buf) {
    autil::ScopedLock lock(_mutex);
    if (buf == NULL) {
        AUTIL_LOG(WARN, "try to free NULL pointer");
        return;
    }
    _blockCount--;
    _bufferVec.push_back(buf);
    buf = NULL;
}

FSLIB_END_NAMESPACE(cache);
