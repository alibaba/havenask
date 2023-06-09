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
#include "indexlib/common/chunk/chunk_file_decoder.h"

#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/file_system/file/CompressFileReader.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, ChunkFileDecoder);

ChunkFileDecoder::ChunkFileDecoder()
    : mPool(NULL)
    , mReaderBaseAddr(NULL)
    , mOpenType(FSOT_UNKNOWN)
    , mCurrentChunkOffset(numeric_limits<uint64_t>::max())
    , mRecordLen(0)
    , mChunkDecoder(NULL)
    , mLoadChunkCount(0)
    , mReadSize(0)
{
}

ChunkFileDecoder::~ChunkFileDecoder() { ReleaseChunkDecoder(); }

void ChunkFileDecoder::Init(Pool* sessionPool, FileReader* fileReader, uint32_t recordLen)
{
    mPool = sessionPool;
    mFileReader = fileReader;
    mReaderBaseAddr = (const char*)fileReader->GetBaseAddress();
    mOpenType = fileReader->GetOpenType();
    mCurrentChunkOffset = numeric_limits<uint64_t>::max();
    mChunkDecoder = NULL;
    mRecordLen = recordLen;
}

void ChunkFileDecoder::Prefetch(uint64_t size, uint64_t offset, ReadOption option)
{
    if (mReaderBaseAddr) {
        return;
    }
    mFileReader->Prefetch(size, offset, option).GetOrThrow();
}

FL_LAZY(FSResult<void>) ChunkFileDecoder::PrefetchAsync(uint64_t size, uint64_t offset, ReadOption option) noexcept
{
    auto ec = (FL_COAWAIT mFileReader->PrefetchAsyncCoro(size, offset, option)).Code();
    if (unlikely(ec != FSEC_OK)) {
        FL_CORETURN ec;
    }
    FL_CORETURN FSEC_OK;
}

#define CALL_FUNCTION_MACRO(funcName, args...)                                                                         \
    switch (mChunkDecoder->GetType()) {                                                                                \
    case ct_plain_integrated:                                                                                          \
        funcName<IntegratedPlainChunkDecoder>(args);                                                                   \
        break;                                                                                                         \
    case ct_plain_slice:                                                                                               \
    case ct_encoded:                                                                                                   \
    default:                                                                                                           \
        assert(false);                                                                                                 \
    }

#define CALL_TEMPLATE_FUNCTION_MACRO(type, funcName, args...)                                                          \
    switch (mChunkDecoder->GetType()) {                                                                                \
    case ct_plain_integrated:                                                                                          \
        funcName<IntegratedPlainChunkDecoder, type>(args);                                                             \
        break;                                                                                                         \
    case ct_plain_slice:                                                                                               \
    case ct_encoded:                                                                                                   \
    default:                                                                                                           \
        assert(false);                                                                                                 \
    }

void ChunkFileDecoder::Seek(uint64_t chunkOffset, uint32_t inChunkOffset, file_system::ReadOption option)
{
    if (mCurrentChunkOffset != chunkOffset || !mChunkDecoder) {
        SwitchChunkDecoder(chunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoSeekInChunk, inChunkOffset);
}

void ChunkFileDecoder::ReadRecord(autil::StringView& value, ReadOption option)
{
    if (!mChunkDecoder) {
        LoadChunkDecoder(mCurrentChunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoReadRecord, value);
}

void ChunkFileDecoder::ReadRecord(autil::StringView& value, uint64_t chunkOffset, uint32_t inChunkOffset,
                                  ReadOption option)
{
    if (mCurrentChunkOffset != chunkOffset || !mChunkDecoder) {
        SwitchChunkDecoder(chunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoReadRecord, value, inChunkOffset);
}

void ChunkFileDecoder::Read(autil::StringView& value, uint32_t len, ReadOption option)
{
    if (!mChunkDecoder) {
        LoadChunkDecoder(mCurrentChunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoRead, value, len);
}

#undef CALL_FUNCTION_MACRO

template <typename DecoderType>
void ChunkFileDecoder::DoSeekInChunk(uint32_t inChunkOffset)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    chunkDecoder->Seek(inChunkOffset);
}

template <typename DecoderType>
void ChunkFileDecoder::DoRead(autil::StringView& value, uint32_t len)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    mReadSize += chunkDecoder->Read(value, len);
    if (chunkDecoder->IsEnd()) {
        mCurrentChunkOffset += mChunkDecoder->GetChunkLength() + sizeof(ChunkMeta);
        ReleaseChunkDecoder();
    }
}

template <typename DecoderType>
void ChunkFileDecoder::DoReadRecord(autil::StringView& value)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    mReadSize += chunkDecoder->ReadRecord(value);
    if (chunkDecoder->IsEnd()) {
        mCurrentChunkOffset += mChunkDecoder->GetChunkLength() + sizeof(ChunkMeta);
        ReleaseChunkDecoder();
    }
}

template <typename DecoderType>
void ChunkFileDecoder::DoReadRecord(autil::StringView& value, uint32_t inChunkOffset)
{
    DoSeekInChunk<DecoderType>(inChunkOffset);
    DoReadRecord<DecoderType>(value);
}

void ChunkFileDecoder::LoadChunkDecoder(uint64_t chunkOffset, ReadOption option)
{
    if (mReaderBaseAddr) {
        mChunkDecoder = common::ChunkDecoderCreator::Create(mReaderBaseAddr, chunkOffset, mRecordLen, mPool);
    } else {
        mChunkDecoder =
            common::ChunkDecoderCreator::Create(mFileReader, mOpenType, chunkOffset, mRecordLen, mPool, option);
    }
    ++mLoadChunkCount;
    mCurrentChunkOffset = chunkOffset;
}

void ChunkFileDecoder::SwitchChunkDecoder(uint64_t chunkOffset, ReadOption option)
{
    ReleaseChunkDecoder();
    LoadChunkDecoder(chunkOffset, option);
}
void ChunkFileDecoder::ReleaseChunkDecoder()
{
    if (mChunkDecoder) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mChunkDecoder);
        mChunkDecoder = NULL;
    }
}
}} // namespace indexlib::common
