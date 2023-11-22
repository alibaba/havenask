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
#include "matchdoc/VectorStorage.h"

namespace matchdoc {

VectorStorage::VectorStorage(const std::shared_ptr<autil::mem_pool::Pool> &pool, uint32_t docSize)
    : _pool(pool), _docSize(docSize) {}

VectorStorage::~VectorStorage() = default;

void VectorStorage::initFromBuffer(char *base, uint32_t docCount) {
    size_t chunkCount = (docCount + CHUNKSIZE - 1) / CHUNKSIZE;
    size_t alignedDocCount = chunkCount * CHUNKSIZE;
    char *begin = base;
    char *end = begin + docCount * _docSize;
    char *alignedEnd = begin + alignedDocCount * _docSize;
    _chunks.reserve(chunkCount);

    for (char *addr = begin; addr < alignedEnd;) {
        char *next = addr + bytesPerChunk();
        if (next <= end) {
            _chunks.emplace_back(addr, nullptr);
        } else {
            void *chunk = _pool->allocate(bytesPerChunk());
            if (addr < end) {
                memcpy(chunk, addr, end - addr);
            }
            _chunks.emplace_back(chunk, _pool);
        }
        addr = next;
    }
}

// assert(size % CHUNKSIZE == 0)
void VectorStorage::growToSize(uint32_t size) {
    auto currentSize = _chunks.size() * CHUNKSIZE;
    if (size <= currentSize) {
        return;
    }
    assert(_docSize > 0);
    uint32_t chunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE - _chunks.size();
    for (uint32_t i = 0; i < chunkCount; i++) {
        void *currentChunk = _pool->allocate(_docSize * CHUNKSIZE);
        _chunks.emplace_back(currentChunk, _pool);
    }
}

void VectorStorage::reset() { _chunks.clear(); }

void VectorStorage::append(VectorStorage &storage) {
    for (size_t i = 0; i < storage._chunks.size(); i++) {
        _chunks.emplace_back(std::move(storage._chunks[i]));
    }
    storage._chunks.clear();
}

void VectorStorage::truncate(uint32_t size) {
    uint32_t newChunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE;
    _chunks.resize(newChunkCount);
}

std::shared_ptr<VectorStorage> VectorStorage::fromBuffer(char *data,
                                                         uint32_t docSize,
                                                         uint32_t count,
                                                         const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    auto storage = std::make_shared<VectorStorage>(pool, docSize);
    storage->initFromBuffer(data, count);
    return storage;
}

} // namespace matchdoc
