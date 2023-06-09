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
#include "indexlib/index/kkv/suffix_key_dumper.h"

#include "indexlib/common/chunk/space_limit_chunk_strategy.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SuffixKeyDumper);

void SuffixKeyDumper::Init(const KKVIndexPreference& indexPreference, const DumpableSKeyNodeIteratorPtr& skeyNodeIter,
                           const DirectoryPtr& directory)
{
    mSkeyParam = indexPreference.GetSkeyParam();
    mSkeyNodeIter = skeyNodeIter;
    mChunkStrategy.reset(new common::SpaceLimitChunkStrategy(KKV_CHUNK_SIZE_THRESHOLD));
    mChunkWriter = CreateChunkWriter();
    mFileWriter = CreateFileWriter(directory);
    mPkeyOffsetDeque.reset(new PKeyOffsetDeque(PKeyOffsetDeque::allocator_type(mPool)));
}

common::ChunkWriterPtr SuffixKeyDumper::CreateChunkWriter()
{
    if (mSkeyParam.IsEncode()) {
        INDEXLIB_THROW(util::UnSupportedException, "Not support encoded chunk yet!");
    }

    common::ChunkWriterPtr chunkWriter(new common::ChunkWriter(mPool, mSkeyParam.IsEncode()));
    chunkWriter->Init(KKV_CHUNK_SIZE_THRESHOLD * 2);
    return chunkWriter;
}

FileWriterPtr SuffixKeyDumper::CreateFileWriter(const DirectoryPtr& directory)
{
    file_system::WriterOption option;
    if (mSkeyParam.EnableFileCompress()) {
        option.compressorName = mSkeyParam.GetFileCompressType();
        option.compressBufferSize = mSkeyParam.GetFileCompressBufferSize();
        option.compressorParams = mSkeyParam.GetFileCompressParameter();
    }
    return directory->CreateFileWriter(SUFFIX_KEY_FILE_NAME, option);
}

void SuffixKeyDumper::Flush()
{
    while (mSkeyNodeIter->IsValid()) {
        autil::StringView value;
        uint64_t pkey = mSkeyNodeIter->Get(value);

        if (pkey != mCurrentPKey) {
            mMaxSKeyCount = max(mMaxSKeyCount, mCurrentSKeyCount);
            mCurrentPKey = pkey;
            mCurrentSKeyCount = 0;
        }

        assert(mPkeyOffsetDeque);
        if (mPkeyOffsetDeque->empty() || mPkeyOffsetDeque->back().pkey != pkey ||
            mPkeyOffsetDeque->back().pkeyOffset.IsValidOffset()) {
            OnDiskPKeyOffset pkeyOffset;
            pkeyOffset.regionId = mSkeyNodeIter->GetRegionId();
            mPkeyOffsetDeque->push_back(InDequeItem(pkey, pkeyOffset, 1));
        } else {
            // skey of same pkey in same chunk
            mPkeyOffsetDeque->back().skeyCount++;
        }
        mChunkWriter->InsertItem(value.data(), value.size());
        if (mChunkStrategy->NeedFlush(mChunkWriter)) {
            size_t chunkOffset = GetDataLength(mFileWriter);
            mChunkWriter->Flush(mFileWriter, mInChunkLocators);
            UpdateFlushPkeyOffsetInfo(mPkeyOffsetDeque, chunkOffset, mInChunkLocators);
        }
        mSkeyNodeIter->MoveToNext();
        ++mSize;
        ++mCurrentSKeyCount;
    }
}

void SuffixKeyDumper::Close()
{
    assert(mChunkWriter);
    assert(mFileWriter);

    Flush();
    size_t chunkOffset = GetDataLength(mFileWriter);
    mChunkWriter->Flush(mFileWriter, mInChunkLocators);
    UpdateFlushPkeyOffsetInfo(mPkeyOffsetDeque, chunkOffset, mInChunkLocators);
    mMaxSKeyCount = max(mMaxSKeyCount, mCurrentSKeyCount);
    mFileWriter->Close().GetOrThrow();
}

void SuffixKeyDumper::UpdateFlushPkeyOffsetInfo(const PKeyOffsetDequePtr& pkeyOffsetDeque, size_t chunkOffset,
                                                const std::vector<uint32_t>& inChunkLocators)
{
    if (inChunkLocators.empty()) {
        return;
    }

    if (chunkOffset >= OnDiskPKeyOffset::MAX_VALID_CHUNK_OFFSET) {
        INDEXLIB_THROW(util::UnSupportedException, "kkv skey chunkOffset over %lu",
                       OnDiskPKeyOffset::MAX_VALID_CHUNK_OFFSET);
    }

    size_t cursor = 0;
    typename PKeyOffsetDeque::iterator iter = pkeyOffsetDeque->begin();

    // TODO: this can be optimized by directly accessing queue element
    for (; iter != pkeyOffsetDeque->end(); iter++) {
        if (iter->pkeyOffset.IsValidOffset()) {
            // skip be over-write offset
            continue;
        }

        uint32_t skeyCount = iter->skeyCount;
        OnDiskPKeyOffset& offset = iter->pkeyOffset;
        offset.chunkOffset = chunkOffset;
        assert(cursor < inChunkLocators.size());
        if (inChunkLocators[cursor] >= KKV_CHUNK_SIZE_THRESHOLD) {
            INDEXLIB_THROW(util::InconsistentStateException, "kkv skey in-chunkOffset over %lu",
                           KKV_CHUNK_SIZE_THRESHOLD);
        }

        offset.inChunkOffset = inChunkLocators[cursor];
        cursor += skeyCount;
    }
    assert(cursor == inChunkLocators.size());
}

size_t SuffixKeyDumper::GetDataLength(const file_system::FileWriterPtr& fileWriter)
{
    assert(fileWriter);
    return fileWriter->GetLogicLength();
}

void SuffixKeyDumper::ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter)
{
    assert(mFileWriter);
    file_system::CompressFileWriterPtr compressWriter =
        DYNAMIC_POINTER_CAST(file_system::CompressFileWriter, mFileWriter);
    file_system::FileWriterPtr writer = compressWriter ? compressWriter->GetDataFileWriter() : mFileWriter;
    assert(writer);
    file_system::BufferedFileWriterPtr bufferWriter = DYNAMIC_POINTER_CAST(file_system::BufferedFileWriter, writer);
    if (bufferWriter) {
        bufferWriter->ResetBufferParam(writerBufferSize, useAsyncWriter).GetOrThrow();
    }
}
}} // namespace indexlib::index
