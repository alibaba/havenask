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

#include "autil/mem_pool/Pool.h"

namespace matchdoc {

class VectorStorage {
public:
    struct Chunk {
        Chunk() = default;
        Chunk(Chunk &&other) = default;
        Chunk(void *data, const std::shared_ptr<autil::mem_pool::Pool> &pool) : data(data), pool(pool) {}
        void *data = nullptr;
        std::shared_ptr<autil::mem_pool::Pool> pool;
    };

public:
    VectorStorage(const std::shared_ptr<autil::mem_pool::Pool> &pool, const uint32_t docSize);
    ~VectorStorage();

private:
    VectorStorage(const VectorStorage &);
    VectorStorage &operator=(const VectorStorage &);

private:
    void initFromBuffer(char *base, uint32_t docCount);

public:
    autil::mem_pool::Pool *getPool() const { return _pool.get(); }
    char *get(uint32_t index) __attribute__((always_inline)) {
        return ((char *)_chunks[index >> CHUNKID].data) + _docSize * (index & CHUNKOFFSET);
    }
    void growToSize(uint32_t count);
    void append(VectorStorage &storage);
    void truncate(uint32_t size);
    uint32_t getDocSize() const { return _docSize; }
    uint32_t getSize() const { return _chunks.size() * CHUNKSIZE; }
    void reset();

    static inline size_t getAlignSize(size_t size) {
        uint32_t chunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE;
        return chunkCount * CHUNKSIZE;
    }

public:
    template <typename T>
    static std::unique_ptr<VectorStorage>
    fromBuffer(T *data, uint32_t count, const std::shared_ptr<autil::mem_pool::Pool> &pool) {
        return fromBuffer(reinterpret_cast<char *>(data), sizeof(T), count, pool);
    }

    static std::unique_ptr<VectorStorage>
    fromBuffer(char *data, uint32_t docSize, uint32_t count, const std::shared_ptr<autil::mem_pool::Pool> &pool);

private:
    size_t bytesPerChunk() const { return _docSize * CHUNKSIZE; }

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    const uint32_t _docSize;
    std::vector<Chunk> _chunks;

public:
    enum {
        CHUNKSIZE = 1024,
        CHUNKOFFSET = 1023,
        CHUNKID = 10
    };
};

} // namespace matchdoc
