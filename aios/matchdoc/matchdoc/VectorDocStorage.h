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
#ifndef ISEARCH_VECTORDOCSTORAGE_H
#define ISEARCH_VECTORDOCSTORAGE_H

#include "autil/mem_pool/Pool.h"

namespace matchdoc {

class VectorDocStorage
{
public:
    VectorDocStorage(autil::mem_pool::Pool *pool)
        : _pool(pool)
        , _docSize(0)
        , _mountSize(0)
    {
    }
    VectorDocStorage(autil::mem_pool::Pool *pool, void *base,
                     uint32_t docSize, uint32_t mountSize,
                     uint32_t capacity)
        : _pool(pool)
        , _docSize(docSize)
        , _mountSize(mountSize)
    {
        initFromMount(base, docSize, mountSize);
    }
    ~VectorDocStorage() {
    }
private:
    VectorDocStorage(const VectorDocStorage &);
    VectorDocStorage& operator=(const VectorDocStorage &);

private:
    void initFromMount(void *base, uint32_t docSize, uint32_t mountSize);

public:
    char *get(uint32_t index) __attribute__((always_inline)) {
        return ((char *)_chunks[index >> CHUNKID]) + _docSize * (index & CHUNKOFFSET);
    }
    void cowFromMount();
    void growToSize(uint32_t count);
    void addChunk(VectorDocStorage &storage);
    void truncate(uint32_t size);
    void incDocSize(uint32_t size) {
        _docSize += size;
    }
    uint32_t getDocSize() const {
        return _docSize;
    }
    uint32_t getSize() const {
        return _chunks.size() * CHUNKSIZE;
    }
    void reset() {
        _chunks.clear();
        _chunkPools.clear();
    }
    static inline size_t getAlignSize(size_t size) {
        uint32_t chunkCount = (size + CHUNKSIZE - 1) / CHUNKSIZE;
        return chunkCount * CHUNKSIZE;
    }
    void setPoolPtr(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr) {
        _poolPtr = poolPtr;
        if (poolPtr != nullptr) {
            _pool = poolPtr.get();
        }
    }
private:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_pool;
    std::vector<std::shared_ptr<autil::mem_pool::Pool>> _chunkPools;
    std::vector<void *> _chunks;
    uint32_t _docSize;
    uint32_t _mountSize;
    void *_mountedChunk = nullptr;
public:
    enum {
        CHUNKSIZE = 1024,
        CHUNKOFFSET = 1023,
        CHUNKID = 10
    };
};

}
#endif //ISEARCH_VECTORDOCSTORAGE_H
