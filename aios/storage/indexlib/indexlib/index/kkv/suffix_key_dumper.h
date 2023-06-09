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
#ifndef __INDEXLIB_SUFFIX_KEY_DUMPER_H
#define __INDEXLIB_SUFFIX_KEY_DUMPER_H

#include <memory>

#include "indexlib/common/chunk/chunk_strategy.h"
#include "indexlib/common/chunk/chunk_writer.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/index/kkv/dumpable_pkey_offset_iterator.h"
#include "indexlib/index/kkv/dumpable_skey_node_iterator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, PKeyOffsetIterator);

namespace indexlib { namespace index {

class SuffixKeyDumper
{
public:
    typedef DumpablePKeyOffsetIterator::InDequeItem InDequeItem;
    typedef DumpablePKeyOffsetIterator::PKeyOffsetDeque PKeyOffsetDeque;
    typedef DumpablePKeyOffsetIterator::PKeyOffsetDequePtr PKeyOffsetDequePtr;

public:
    SuffixKeyDumper(autil::mem_pool::PoolBase* pool)
        : mPool(pool)
        , mSize(0)
        , mCurrentPKey(0)
        , mCurrentSKeyCount(0)
        , mMaxSKeyCount(0)
    {
        assert(mPool);
    }

    ~SuffixKeyDumper() {}

public:
    void Init(const config::KKVIndexPreference& indexPreference, const DumpableSKeyNodeIteratorPtr& skeyNodeIter,
              const file_system::DirectoryPtr& directory);

    DumpablePKeyOffsetIteratorPtr CreatePkeyOffsetIterator()
    {
        return DumpablePKeyOffsetIteratorPtr(new DumpablePKeyOffsetIterator(mPkeyOffsetDeque));
    }

    void Flush();
    void Close();

    // for merge
    void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter);

    void Reserve(size_t totalFileLen)
    {
        assert(mFileWriter);
        mFileWriter->ReserveFile(totalFileLen).GetOrThrow();
    }

    size_t Size() const { return mSize; }

    template <typename SKeyType>
    static size_t GetTotalDumpSize(size_t totalSKeyCount, bool storeTs, bool storeExpireTime)
    {
        size_t skeySize = sizeof(NormalOnDiskSKeyNode<SKeyType>);
        if (storeTs) {
            skeySize += sizeof(uint32_t);
        }
        if (storeExpireTime) {
            skeySize += sizeof(uint32_t);
        }
        size_t estimateSkeySize = totalSKeyCount * skeySize;
        size_t estimateChunkCount = (estimateSkeySize + KKV_CHUNK_SIZE_THRESHOLD - 1) / KKV_CHUNK_SIZE_THRESHOLD;
        return estimateSkeySize + estimateChunkCount * sizeof(common::ChunkMeta);
    }

    uint32_t GetMaxSKeyCount() const { return mMaxSKeyCount; }

private:
    common::ChunkWriterPtr CreateChunkWriter();
    file_system::FileWriterPtr CreateFileWriter(const file_system::DirectoryPtr& directory);

    config::ValueConfigPtr MakeValueConfig()
    {
        assert(!mSkeyParam.IsEncode());
        // if skey support column storage, should generate valueConfig
        // encode must be column storage
        return config::ValueConfigPtr();
    }

public:
    static void UpdateFlushPkeyOffsetInfo(const PKeyOffsetDequePtr& pkeyOffsetDeque, size_t chunkOffset,
                                          const std::vector<uint32_t>& inChunkLocators);

    static size_t GetDataLength(const file_system::FileWriterPtr& fileWriter);

private:
    common::ChunkStrategyPtr mChunkStrategy;
    common::ChunkWriterPtr mChunkWriter;
    DumpableSKeyNodeIteratorPtr mSkeyNodeIter;
    PKeyOffsetDequePtr mPkeyOffsetDeque;
    file_system::FileWriterPtr mFileWriter;
    std::vector<uint32_t> mInChunkLocators;
    config::KKVIndexPreference::SuffixKeyParam mSkeyParam;
    autil::mem_pool::PoolBase* mPool;
    size_t mSize;
    uint64_t mCurrentPKey;
    uint32_t mCurrentSKeyCount;
    uint32_t mMaxSKeyCount;

private:
    friend class SuffixKeyDumperTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SuffixKeyDumper);
}} // namespace indexlib::index

#endif //__INDEXLIB_SUFFIX_KEY_DUMPER_H
