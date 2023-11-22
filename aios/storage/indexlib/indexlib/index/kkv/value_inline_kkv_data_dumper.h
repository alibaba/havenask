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

#include "indexlib/common/chunk/chunk_strategy.h"
#include "indexlib/common/chunk/chunk_writer.h"
#include "indexlib/common/chunk/space_limit_chunk_strategy.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/kkv/dumpable_pkey_offset_iterator.h"
#include "indexlib/index/kkv/kkv_data_dumper.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/suffix_key_dumper.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class ValueInlineKKVDataDumper final : public KKVDataDumper<SKeyType>
{
    using Base = KKVDataDumper<SKeyType>;
    using SKeyNode = typename SuffixKeyTraits<SKeyType, true>::SKeyNode;
    typedef DumpablePKeyOffsetIterator::InDequeItem InDequeItem;
    typedef DumpablePKeyOffsetIterator::PKeyOffsetDeque PKeyOffsetDeque;
    typedef DumpablePKeyOffsetIterator::PKeyOffsetDequePtr PKeyOffsetDequePtr;

public:
    ValueInlineKKVDataDumper(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                             bool storeTs, const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool)
        : Base(schema, kkvConfig, storeTs)
        , mMaxValueLen(0)
        , mPool(pool)
        , mSKeyCount(0)
        , mCurrentPKey(0)
        , mCurrentSKeyCount(0)
        , mMaxSKeyCount(0)
    {
        assert(pool);
        const auto& indexPreference = kkvConfig->GetIndexPreference();
        mParam = indexPreference.GetSkeyParam();
        mChunkStrategy.reset(new common::SpaceLimitChunkStrategy(KKV_CHUNK_SIZE_THRESHOLD));
        mChunkWriter = CreateChunkWriter();
        mFileWriter = CreateFileWriter(directory);
        mPKeyOffsetDeque.reset(new PKeyOffsetDeque(PKeyOffsetDeque::allocator_type(pool)));
    }
    ~ValueInlineKKVDataDumper() {}

public:
    DumpablePKeyOffsetIteratorPtr GetPKeyOffsetIterator() override final
    {
        return DumpablePKeyOffsetIteratorPtr(new DumpablePKeyOffsetIterator(mPKeyOffsetDeque));
    }

    void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter) override
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

    void Reserve(size_t totalSkeyCount, size_t totalValueLen) override
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "value inline dummper only used for merge, "
                                                "buffer file do not support reserve");
    }

    void Close() override
    {
        size_t chunkOffset = SuffixKeyDumper::GetDataLength(mFileWriter);
        mChunkWriter->Flush(mFileWriter, mInChunkLocators);
        SuffixKeyDumper::UpdateFlushPkeyOffsetInfo(mPKeyOffsetDeque, chunkOffset, mInChunkLocators);
        mMaxSKeyCount = std::max(mMaxSKeyCount, mCurrentSKeyCount);
        mFileWriter->Close().GetOrThrow();
    }

    size_t GetSKeyCount() const override { return mSKeyCount; }

    size_t GetMaxSKeyCount() const override { return mMaxSKeyCount; }

    size_t GetMaxValueLen() const override { return mMaxValueLen; }

    void Collect(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey, bool isDeletedSkey,
                 bool isLastNode, const autil::StringView& value, regionid_t regionId) override
    {
        // FIXME: dumplicate code
        assert(mChunkWriter);
        InsertSKeyNode(skey, ts, expireTime, isDeletedPkey, isDeletedSkey, isLastNode);
        if (!isDeletedPkey && !isDeletedSkey) {
            AppendValue(value, regionId);
        }
        if (pkey != mCurrentPKey) {
            mMaxSKeyCount = std::max(mMaxSKeyCount, mCurrentSKeyCount);
            mCurrentSKeyCount = 0;
            mCurrentPKey = pkey;
        }
        ++mCurrentSKeyCount;
        ++mSKeyCount;

        assert(mPKeyOffsetDeque);
        if (mPKeyOffsetDeque->empty() || mPKeyOffsetDeque->back().pkey != pkey ||
            mPKeyOffsetDeque->back().pkeyOffset.IsValidOffset()) {
            OnDiskPKeyOffset pkeyOffset;
            pkeyOffset.regionId = regionId;
            mPKeyOffsetDeque->push_back(InDequeItem(pkey, pkeyOffset, 1));
        } else {
            // skey of same pkey in same chunk
            mPKeyOffsetDeque->back().skeyCount++;
        }

        if (mChunkStrategy->NeedFlush(mChunkWriter)) {
            size_t chunkOffset = SuffixKeyDumper::GetDataLength(mFileWriter);
            mChunkWriter->Flush(mFileWriter, mInChunkLocators);
            SuffixKeyDumper::UpdateFlushPkeyOffsetInfo(mPKeyOffsetDeque, chunkOffset, mInChunkLocators);
        }
    }

private:
    void InsertSKeyNode(SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey, bool isDeletedSkey,
                        bool isLastNode)
    {
        SKeyNode node;

        if (isDeletedPkey) {
            node.SetPKeyDeleted(ts);
        } else if (isDeletedSkey) {
            node.SetSKeyDeleted(skey, ts);
        } else {
            node.SetSKeyNodeInfo(skey, ts);
        }
        node.SetLastNode(isLastNode);
        mChunkWriter->InsertItem((char*)&node, sizeof(node));

        if (Base::mStoreTs) {
            mChunkWriter->AppendData((char*)&ts, sizeof(ts));
        }
        if (Base::mKKVConfig->StoreExpireTime()) {
            mChunkWriter->AppendData((char*)&expireTime, sizeof(expireTime));
        }
    }

    void AppendValue(const autil::StringView& value, regionid_t regionId)
    {
        auto kkvConfig = static_cast<config::KKVIndexConfig*>(
            Base::mSchema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig().get());
        assert(kkvConfig);
        int32_t fixedValueLen = kkvConfig->GetValueConfig()->GetFixedLength();
        assert(fixedValueLen < 0 || (size_t)fixedValueLen == value.size());

        size_t headerSize = 0;
        if (fixedValueLen < 0) {
            char encodeLenBuf[4];
            size_t headerSize = common::VarNumAttributeFormatter::EncodeCount(value.size(), encodeLenBuf, 4);
            mChunkWriter->AppendData(encodeLenBuf, headerSize);
        }
        mChunkWriter->AppendData(value.data(), value.size());
        mMaxValueLen = std::max(mMaxValueLen, value.size() + headerSize);
    }

    common::ChunkWriterPtr CreateChunkWriter()
    {
        if (mParam.IsEncode()) {
            INDEXLIB_THROW(util::UnSupportedException, "Not support encoded chunk yet!");
        }

        assert(!mParam.IsEncode());
        // if skey support column storage, should generate valueConfig
        // encode must be column storage
        common::ChunkWriterPtr chunkWriter(new common::ChunkWriter(mPool, mParam.IsEncode()));
        chunkWriter->Init(KKV_CHUNK_SIZE_THRESHOLD * 2);
        return chunkWriter;
    }

    file_system::FileWriterPtr CreateFileWriter(const file_system::DirectoryPtr& directory)
    {
        file_system::WriterOption option;
        if (mParam.EnableFileCompress()) {
            option.compressorName = mParam.GetFileCompressType();
            option.compressBufferSize = mParam.GetFileCompressBufferSize();
            option.compressorParams = mParam.GetFileCompressParameter();
        }
        return directory->CreateFileWriter(SUFFIX_KEY_FILE_NAME, option);
    }

private:
    common::ChunkStrategyPtr mChunkStrategy;
    common::ChunkWriterPtr mChunkWriter;
    PKeyOffsetDequePtr mPKeyOffsetDeque;
    file_system::FileWriterPtr mFileWriter;
    std::vector<uint32_t> mInChunkLocators;
    config::KKVIndexPreference::ValueParam mParam;
    config::ValueConfigPtr mValueConfig;
    size_t mMaxValueLen;
    autil::mem_pool::PoolBase* mPool;
    size_t mSKeyCount;
    uint64_t mCurrentPKey;
    uint32_t mCurrentSKeyCount;
    uint32_t mMaxSKeyCount;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, ValueInlineKKVDataDumper);
}} // namespace indexlib::index
