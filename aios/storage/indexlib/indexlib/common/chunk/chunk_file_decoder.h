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

#include "future_lite/CoroInterface.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib { namespace common {

class ChunkFileDecoder
{
public:
    ChunkFileDecoder();
    ~ChunkFileDecoder();

public:
    void Init(autil::mem_pool::Pool* sessionPool, file_system::FileReader* fileReader, uint32_t recordLen);

    void Seek(uint64_t chunkOffset, uint32_t inChunkOffset, file_system::ReadOption option);
    void Prefetch(uint64_t size, uint64_t offset, file_system::ReadOption option);
    FL_LAZY(file_system::FSResult<void>)
    PrefetchAsync(uint64_t size, uint64_t offset, file_system::ReadOption option) noexcept;

    // shuold not cross chunk
    void Read(autil::StringView& value, uint32_t len, file_system::ReadOption option);

    void ReadRecord(autil::StringView& value, file_system::ReadOption option);
    void ReadRecord(autil::StringView& value, uint64_t chunkOffset, uint32_t inChunkOffset,
                    file_system::ReadOption option);

    int64_t GetLoadChunkCount() const { return mLoadChunkCount; }

    int64_t GetReadSize() const { return mReadSize; }

    bool IsIntegratedFile() const { return mReaderBaseAddr != nullptr; }

    FL_LAZY(util::PooledUniquePtr<ChunkDecoder>) GetChunk(uint64_t& chunkOffset, file_system::ReadOption option)
    {
        util::PooledUniquePtr<ChunkDecoder> ret;
        if (mReaderBaseAddr) {
            auto chunk = common::ChunkDecoderCreator::Create(mReaderBaseAddr, chunkOffset, mRecordLen, mPool);
            ret.reset(chunk, mPool);
        } else {
            auto chunk = FL_COAWAIT common::ChunkDecoderCreator::CreateAsync(mFileReader, mOpenType, chunkOffset,
                                                                             mRecordLen, mPool, option);
            ret.reset(chunk, mPool);
        }
        chunkOffset += (sizeof(ChunkMeta) + ret->GetChunkLength());
        ++mLoadChunkCount;
        FL_CORETURN ret;
    }

private:
    template <typename DecoderType>
    void DoRead(autil::StringView& value, uint32_t len);

    template <typename DecoderType>
    void DoReadRecord(autil::StringView& value);

    template <typename DecoderType>
    void DoReadRecord(autil::StringView& value, uint32_t inChunkOffset);

    template <typename DecoderType>
    void DoSeekInChunk(uint32_t inChunkOffset);

    void ReleaseChunkDecoder();
    void LoadChunkDecoder(uint64_t chunkOffset, file_system::ReadOption option);
    void SwitchChunkDecoder(uint64_t chunkOffset, file_system::ReadOption option);

private:
    autil::mem_pool::Pool* mPool;
    const char* mReaderBaseAddr;
    file_system::FileReader* mFileReader;
    file_system::FSOpenType mOpenType;
    uint64_t mCurrentChunkOffset;
    uint32_t mRecordLen;
    ChunkDecoder* mChunkDecoder;
    int64_t mLoadChunkCount;
    int64_t mReadSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkFileDecoder);
}} // namespace indexlib::common
