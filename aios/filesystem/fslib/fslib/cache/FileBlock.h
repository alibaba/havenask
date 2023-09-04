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
#ifndef FSLIB_FILEBLOCK_H
#define FSLIB_FILEBLOCK_H

#include <cassert>
#include <cstdlib>
#include <unistd.h>

#include "fslib/cache/FileBlockPool.h"
#include "fslib/common/common_define.h"

FSLIB_BEGIN_NAMESPACE(cache);

class FileBlock {
public:
    FileBlock(int64_t offset = 0, size_t blockSize = DEFAULT_FILE_BLOCK_SIZE) {
        _blockSize = blockSize;
        _offset = offset;
        _actualLen = 0;
        _buffer = NULL;
        _pool = NULL;
    }

    ~FileBlock() {
        if (_buffer != NULL) {
            if (_pool != NULL) {
                _pool->deAllocate(_buffer);
            } else {
                free(_buffer);
            }
        }
    }

private:
    FileBlock(const FileBlock &);
    FileBlock &operator=(const FileBlock &);

public:
    int32_t init(FileBlockPool *pool = NULL) {
        assert(!_buffer);
        int32_t errCode = 0;
        size_t alignment = getpagesize();
        if (pool == NULL || pool->getBlockSize() != _blockSize) {
            errCode = posix_memalign(&_buffer, alignment, _blockSize);
        } else {
            errCode = pool->allocate(_buffer);
            _pool = pool;
        }
        return errCode;
    }
    int32_t initWithBuffer(void *buffer) {
        assert(!_buffer);
        _buffer = buffer;
        return 0;
    }
    int64_t getOffset() const { return _offset; }
    size_t getActualLen() const { return _actualLen; }
    void setActualLen(const size_t actualLen) { _actualLen = actualLen; }
    size_t getBlockSize() const { return _blockSize; }
    void *getBuffer() const { return _buffer; }

private:
    void *_buffer;
    int64_t _offset;
    size_t _actualLen;
    size_t _blockSize;
    FileBlockPool *_pool;
};

FSLIB_TYPEDEF_SHARED_PTR(FileBlock);

class GetFileBlockSize {
public:
    uint64_t operator()(const FileBlockPtr &value) { return value->getBlockSize(); }
};

FSLIB_END_NAMESPACE(cache);

#endif // FSLIB_FILEBLOCK_H
