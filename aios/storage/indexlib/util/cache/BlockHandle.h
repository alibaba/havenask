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

#include <memory>

#include "indexlib/util/cache/BlockCache.h"

namespace indexlib { namespace util {

class BlockHandle
{
public:
    BlockHandle() noexcept : _blockCache(NULL), _cacheHandle(NULL), _block(NULL), _blockDataSize(0) {}

    BlockHandle(util::BlockCache* blockCache, autil::CacheBase::Handle* handle, util::Block* block,
                size_t blockDataSize) noexcept
        : _blockCache(blockCache)
        , _cacheHandle(handle)
        , _block(block)
        , _blockDataSize(blockDataSize)
    {
    }

    ~BlockHandle() noexcept { Release(); }

    BlockHandle(BlockHandle&& rhs) noexcept
        : _blockCache(rhs._blockCache)
        , _cacheHandle(rhs._cacheHandle)
        , _block(rhs._block)
        , _blockDataSize(rhs._blockDataSize)
    {
        rhs._blockCache = nullptr;
        rhs._cacheHandle = nullptr;
        rhs._block = nullptr;
        rhs._blockDataSize = 0;
    }

    // TODO(qisa.cb) inline will cause error in coroutine
    __attribute__((noinline)) BlockHandle& operator=(BlockHandle&& other) noexcept
    {
        if (this != &other) {
            std::swap(_blockCache, other._blockCache);
            std::swap(_cacheHandle, other._cacheHandle);
            std::swap(_block, other._block);
            std::swap(_blockDataSize, other._blockDataSize);
        }
        return *this;
    }

public:
    void Reset(util::BlockCache* blockCache, autil::CacheBase::Handle* handle, util::Block* block,
               size_t blockDataSize) noexcept
    {
        Release();
        assert(blockCache);
        assert(block);
        assert(handle);

        _blockCache = blockCache;
        _block = block;
        _cacheHandle = handle;
        _blockDataSize = blockDataSize;
    }

    uint64_t GetOffset() const noexcept
    {
        return _block == NULL ? 0 : (_blockCache->GetBlockSize() * _block->id.inFileIdx);
    }
    uint8_t* GetData() const noexcept { return _block == NULL ? NULL : _block->data; }
    uint32_t GetDataSize() const noexcept { return _blockDataSize; }
    uint64_t GetFileId() const noexcept { return _block == NULL ? 0 : _block->id.fileId; }
    util::Block* GetBlock() const noexcept { return _block; }

    autil::CacheBase::Handle* StealHandle() noexcept
    {
        autil::CacheBase::Handle* ret = _cacheHandle;
        _cacheHandle = NULL;
        return ret;
    }

    void Release() noexcept
    {
        if (!_cacheHandle) {
            return;
        }

        if (_blockCache) {
            _blockCache->ReleaseHandle(_cacheHandle);
        }
        _cacheHandle = NULL;
        _block = NULL;
        _blockDataSize = 0;
        _blockCache = NULL;
    }

private:
    BlockHandle(const BlockHandle&);
    BlockHandle& operator=(const BlockHandle&);

private:
    util::BlockCache* _blockCache;
    autil::CacheBase::Handle* _cacheHandle;
    util::Block* _block;
    uint32_t _blockDataSize;
};

typedef std::shared_ptr<BlockHandle> BlockHandlePtr;
}} // namespace indexlib::util
