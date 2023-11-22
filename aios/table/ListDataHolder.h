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

#include <algorithm>
#include <vector>

#include "autil/mem_pool/Pool.h"

namespace table {

template <typename T>
class ListDataHolder {
    struct Chunk {
        T *data = nullptr;
        size_t chunkSize = 0;
        std::shared_ptr<autil::mem_pool::Pool> pool;

        Chunk() = default;
        Chunk(const Chunk &other) : data(other.data), chunkSize(other.chunkSize), pool(other.pool) {}
        Chunk(Chunk &&other) noexcept : data(other.data), chunkSize(other.chunkSize), pool(std::move(other.pool)) {}
        Chunk &operator=(const Chunk &other) {
            data = other.data;
            chunkSize = other.chunkSize;
            pool = other.pool;
            return *this;
        }
        ~Chunk() {
            if (pool) {
                pool->deallocate(data, sizeof(T) * chunkSize);
            }
        }
    };

private:
    static constexpr size_t PG_SIZE = 4096;
    static constexpr size_t DEFAULT_CHUNK_SIZE = PG_SIZE / sizeof(T);

public:
    ListDataHolder(std::shared_ptr<autil::mem_pool::Pool> pool, size_t chunkSize = DEFAULT_CHUNK_SIZE)
        : _lastChunk(nullptr), _lastChunkPos(chunkSize), _chunkSize(chunkSize), _pool(std::move(pool)) {}

    ~ListDataHolder() {
        _chunks.clear();
        _pool.reset();
    }

public:
    T *reserve(size_t count) {
        if (_lastChunk == nullptr || count + _lastChunkPos > _lastChunk->chunkSize) {
            _chunks.emplace_back(newChunk(count));
            _lastChunk = &_chunks.back();
            _lastChunkPos = 0;
        }
        T *addr = _lastChunk->data + _lastChunkPos;
        _lastChunkPos += count;
        return addr;
    }

    const T *append(const T *data, size_t count) {
        T *addr = reserve(count);
        std::copy(data, data + count, addr);
        return addr;
    }

    void merge(ListDataHolder<T> &other) {
        if (other._chunks.empty()) {
            return;
        }
        if (_chunks.empty()) {
            _chunks.swap(other._chunks);
        } else {
            _chunks.insert(_chunks.end(), other._chunks.begin(), other._chunks.end());
        }
        _lastChunk = &_chunks.back();
        _lastChunkPos = other._lastChunkPos;
    }

    uint32_t totalBytes() const {
        size_t chunkCount = _chunks.size();
        uint32_t bytes = 0;
        for (size_t i = 0; i < chunkCount; ++i) {
            bytes += _chunks[i].chunkSize;
        }
        return bytes;
    }

private:
    Chunk newChunk(uint32_t chunkSize) const {
        chunkSize = std::max(chunkSize, _chunkSize);
        Chunk chunk;
        chunk.data = (T *)_pool->allocate(bytesPerChunk(chunkSize));
        chunk.chunkSize = chunkSize;
        chunk.pool = _pool;
        return chunk;
    }

    size_t bytesPerChunk(uint32_t chunkSize) const { return chunkSize * sizeof(T); }

private:
    Chunk *_lastChunk = nullptr;
    uint32_t _lastChunkPos = 0;
    const uint32_t _chunkSize;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::vector<Chunk> _chunks;
};

} // namespace table
