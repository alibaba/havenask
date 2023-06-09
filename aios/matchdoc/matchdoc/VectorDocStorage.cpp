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
#include "matchdoc/VectorDocStorage.h"

namespace matchdoc {

void VectorDocStorage::initFromMount(void *base, uint32_t docSize, uint32_t mountSize) {
    size_t chunks = (mountSize + CHUNKSIZE - 1) / CHUNKSIZE;
    _chunks.reserve(chunks);
    _chunkPools.reserve(chunks);
    _mountedChunk = base;
    for (size_t i = 0; i < chunks; ++i) {
        _chunks.push_back(static_cast<char *>(base) + i * docSize * CHUNKSIZE);
        _chunkPools.push_back(nullptr);
    }
}

/**
 * mount tensor构造storage后，默认数据都存在本身的存储上，
 * 当添加新doc时，将最后的不完整一块chunk拷贝，并在此基础上追加。
 * allocator 负责维护 CoW 是否执行过的状态。
 */
void VectorDocStorage::cowFromMount() {
    if (_mountSize % CHUNKSIZE == 0) {
        return;
    }
    uint32_t toCopyCount = _mountSize - CHUNKSIZE * (_mountSize / CHUNKSIZE);
    uint32_t lastChunkIdx = _mountSize / CHUNKSIZE;
    void *lastChunk = _pool->allocate(_docSize * CHUNKSIZE);
    memcpy(lastChunk, _chunks[lastChunkIdx], _docSize * toCopyCount);
    _chunkPools[lastChunkIdx] = _poolPtr;
    _chunks[lastChunkIdx] = lastChunk;
}

// assert(size % CHUNKSIZE == 0)
void VectorDocStorage::growToSize(uint32_t size) {
    auto currentSize = _chunks.size() * CHUNKSIZE;
    if (size <= currentSize) {
        return;
    }
    assert(_docSize > 0);
    uint32_t chunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE - _chunks.size();
    for (uint32_t i = 0; i < chunkCount; i++) {
        void *currentChunk = _pool->allocate(_docSize * CHUNKSIZE);
        _chunks.push_back(currentChunk);
        _chunkPools.push_back(_poolPtr);
    }
}

void VectorDocStorage::addChunk(VectorDocStorage &storage) {
    const std::vector<void *>& chunks = storage._chunks;
    const auto &pools = storage._chunkPools;
    for (size_t i = 0; i < chunks.size(); i++) {
        _chunks.push_back(chunks[i]);
        _chunkPools.push_back(pools[i]);
    }
}

void VectorDocStorage::truncate(uint32_t size) {
    uint32_t newChunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE;
    _chunks.resize(newChunkCount);
    _chunkPools.resize(newChunkCount);
}

}  // namespace matchdoc
