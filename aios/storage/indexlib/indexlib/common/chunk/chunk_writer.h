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
#ifndef __INDEXLIB_CHUNK_WRITER_H
#define __INDEXLIB_CHUNK_WRITER_H

#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common/chunk/chunk_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/value_config.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class ChunkWriter
{
public:
    ChunkWriter(autil::mem_pool::PoolBase* pool, bool enableEncode)
        : mBuffer(NULL)
        , mCapacity(0)
        , mSize(0)
        , mPool(pool)
        , mEnableEncode(enableEncode)
    {
        // TODO: support encode
        assert(!mEnableEncode);
    }

    ~ChunkWriter()
    {
        if (mBuffer) {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mBuffer, mCapacity);
            mBuffer = NULL;
            mCapacity = 0;
            mSize = 0;
        }
    }

public:
    void Init(size_t initBufferSize)
    {
        mBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, char, initBufferSize);
        mCapacity = initBufferSize;
        mSize = 0;
    }

    void Flush(const file_system::FileWriterPtr& fileWriter, std::vector<uint32_t>& inChunkLocators)
    {
        inChunkLocators.clear();
        if (mOffsets.empty()) {
            return;
        }

        if (mEnableEncode) {
            FlushEncodeChunk(fileWriter, inChunkLocators);
        } else {
            FlushNonEncodeChunk(fileWriter, inChunkLocators);
        }
        mOffsets.clear();
        mSize = 0;
    }

    void InsertItem(const char* value, size_t length)
    {
        mOffsets.push_back(mSize);
        AppendData(value, length);
    }

    void AppendData(const char* value, size_t length)
    {
        size_t newLen = mSize + length;
        if (newLen > mCapacity) {
            Resize(newLen);
        }
        memcpy(mBuffer + mSize, value, length);
        mSize = newLen;
    }

    size_t ValueCount() const { return mOffsets.size(); }

    size_t TotalValueLength() const { return mSize; }

protected:
    void Resize(size_t totalSize)
    {
        assert(totalSize > mCapacity);
        char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, char, totalSize);
        memcpy(buffer, mBuffer, mSize);

        IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mBuffer, mCapacity);
        mBuffer = buffer;
        mCapacity = totalSize;
    }

    void FlushNonEncodeChunk(const file_system::FileWriterPtr& fileWriter, std::vector<uint32_t>& inChunkLocators)
    {
        ChunkMeta chunkMeta;
        chunkMeta.isEncoded = 0;
        chunkMeta.length = mSize;

        fileWriter->Write(&chunkMeta, sizeof(ChunkMeta)).GetOrThrow();
        fileWriter->Write(mBuffer, mSize).GetOrThrow();

        inChunkLocators.resize(mOffsets.size());
        for (size_t i = 0; i < mOffsets.size(); i++) {
            inChunkLocators[i] = mOffsets[i];
        }
    }

    void FlushEncodeChunk(const file_system::FileWriterPtr& fileWriter, std::vector<uint32_t>& inChunkLocators)
    {
        // TODO: support
        assert(false);
    }

protected:
    char* mBuffer;
    size_t mCapacity;
    size_t mSize;
    std::vector<uint32_t> mOffsets;
    autil::mem_pool::PoolBase* mPool;
    bool mEnableEncode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkWriter);
}} // namespace indexlib::common

#endif //__INDEXLIB_CHUNK_WRITER_H
