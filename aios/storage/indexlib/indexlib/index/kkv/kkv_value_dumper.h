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

#include "autil/ConstString.h"
#include "autil/MultiValueFormatter.h"
#include "indexlib/common/chunk/chunk_writer.h"
#include "indexlib/common/chunk/space_limit_chunk_strategy.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/kkv/dumpable_skey_node_iterator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class ValueDumper
{
public:
    ValueDumper(autil::mem_pool::PoolBase* pool, bool storeTs)
        : mPool(pool)
        , mMaxValueLen(0)
        , mStoreTs(storeTs)
        , mStoreExpireTime(false)
    {
        assert(pool);
    }
    virtual ~ValueDumper() {}

public:
    virtual DumpableSKeyNodeIteratorPtr CreateSkeyNodeIterator() = 0;
    virtual void Init(const config::KKVIndexConfigPtr& kkvIndexConfig, const file_system::DirectoryPtr& directory) = 0;
    virtual void Insert(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey,
                        bool isDeletedSkey, bool isLastNode, const autil::StringView& value, regionid_t regionId,
                        int32_t fixedValueLen) = 0;
    virtual void UpdateFlushSkeyNodeInfo(size_t chunkOffset, const std::vector<uint32_t>& inChunkLocators) = 0;

public:
    static size_t GetTotalDumpSize(size_t totalValueLength)
    {
        size_t estimateChunkCount = (totalValueLength + KKV_CHUNK_SIZE_THRESHOLD - 1) / KKV_CHUNK_SIZE_THRESHOLD;
        size_t maxAlignPaddingSize = (1 << OnDiskValueChunkAlignBit) - 1;
        return totalValueLength + estimateChunkCount * (sizeof(common::ChunkMeta) + maxAlignPaddingSize);
    }

public:
    void Close()
    {
        assert(mChunkWriter);
        assert(mFileWriter);
        Flush();
        mFileWriter->Close().GetOrThrow();
    }

    // for merge
    void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter)
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

    void Reserve(size_t totalFileLen)
    {
        assert(mFileWriter);
        mFileWriter->ReserveFile(totalFileLen).GetOrThrow();
    }

    size_t GetMaxValueLen() const { return mMaxValueLen; }

protected:
    common::ChunkWriterPtr CreateChunkWriter()
    {
        if (mValueParam.IsEncode()) {
            INDEXLIB_THROW(util::UnSupportedException, "Not support encoded chunk yet!");
        }

        common::ChunkWriterPtr chunkWriter(new common::ChunkWriter(mPool, mValueParam.IsEncode()));
        chunkWriter->Init(KKV_CHUNK_SIZE_THRESHOLD * 2);
        return chunkWriter;
    }

    file_system::FileWriterPtr CreateFileWriter(const config::KKVIndexConfigPtr& kkvIndexConfig,
                                                const file_system::DirectoryPtr& directory)
    {
        file_system::WriterOption writerOption =
            kkvIndexConfig->GetUseSwapMmapFile() ? file_system::WriterOption::Buffer() : file_system::WriterOption();
        if (mValueParam.EnableFileCompress()) {
            writerOption.compressorName = mValueParam.GetFileCompressType();
            writerOption.compressBufferSize = mValueParam.GetFileCompressBufferSize();
            writerOption.compressorParams = mValueParam.GetFileCompressParameter();
        }
        return directory->CreateFileWriter(KKV_VALUE_FILE_NAME, writerOption);
    }

    size_t GetDataLength() const
    {
        assert(mFileWriter);
        return mFileWriter->GetLogicLength();
    }
    void AddPaddingForChunkOffsetAlign()
    {
        size_t fileLen = GetDataLength();
        size_t alignUnitSize = ((size_t)1 << OnDiskValueChunkAlignBit);
        size_t alignUnitCount = (fileLen + alignUnitSize - 1) >> OnDiskValueChunkAlignBit;
        size_t alignFileLen = alignUnitCount << OnDiskValueChunkAlignBit;

        if (alignFileLen > fileLen) {
            std::vector<char> paddingVec(alignFileLen - fileLen, 0);
            mFileWriter->Write(paddingVec.data(), paddingVec.size()).GetOrThrow();
        }
    }

    void Flush()
    {
        // make chunk offset align to 8 byte
        AddPaddingForChunkOffsetAlign();
        size_t chunkOffset = GetDataLength();
        mChunkWriter->Flush(mFileWriter, mInChunkLocators);
        UpdateFlushSkeyNodeInfo(chunkOffset, mInChunkLocators);
    }

protected:
    common::ChunkStrategyPtr mChunkStrategy;
    common::ChunkWriterPtr mChunkWriter;
    file_system::FileWriterPtr mFileWriter;
    std::vector<uint32_t> mInChunkLocators;
    config::KKVIndexPreference::ValueParam mValueParam;
    config::ValueConfigPtr mValueConfig;
    autil::mem_pool::PoolBase* mPool;
    size_t mMaxValueLen;
    bool mStoreTs;
    bool mStoreExpireTime;

private:
    friend class KKVValueDumperTest;
};

template <typename SKeyType, size_t SKeyAppendSize>
class KKVValueDumper : public ValueDumper<SKeyType>
{
private:
    using Base = ValueDumper<SKeyType>;
    using SKeyNodeIter = DumpableSKeyNodeIteratorTyped<SKeyType, SKeyAppendSize>;
    using SKeyNodeItem = typename SKeyNodeIter::SKeyNodeItem;
    using SKeyNodeDeque = typename SKeyNodeIter::SKeyNodeDeque;
    using SKeyNodeDequePtr = typename SKeyNodeIter::SKeyNodeDequePtr;

    using Base::mChunkStrategy;
    using Base::mChunkWriter;
    using Base::mFileWriter;
    using Base::mInChunkLocators;
    using Base::mMaxValueLen;
    using Base::mPool;
    using Base::mStoreExpireTime;
    using Base::mStoreTs;
    using Base::mValueConfig;
    using Base::mValueParam;

public:
    KKVValueDumper(autil::mem_pool::PoolBase* pool, bool storeTs) : Base(pool, storeTs) {}

public:
    void Init(const config::KKVIndexConfigPtr& kkvIndexConfig, const file_system::DirectoryPtr& directory) override
    {
        mStoreExpireTime = kkvIndexConfig->StoreExpireTime();
        const config::KKVIndexPreference& indexPreference = kkvIndexConfig->GetIndexPreference();
        mValueParam = indexPreference.GetValueParam();
        mValueConfig = kkvIndexConfig->GetValueConfig();
        assert(mValueConfig);
        mChunkStrategy.reset(new common::SpaceLimitChunkStrategy(KKV_CHUNK_SIZE_THRESHOLD));
        mChunkWriter = Base::CreateChunkWriter();
        mFileWriter = Base::CreateFileWriter(kkvIndexConfig, directory);
        mSkeyNodeDeque.reset(new SKeyNodeDeque(typename SKeyNodeDeque::allocator_type(mPool)));

        assert(SKeyAppendSize == (mStoreTs ? 4 : 0) + (mStoreExpireTime ? 4 : 0));
    }

    DumpableSKeyNodeIteratorPtr CreateSkeyNodeIterator() override
    {
        return DumpableSKeyNodeIteratorPtr(new DumpableSKeyNodeIteratorTyped<SKeyType, SKeyAppendSize>(mSkeyNodeDeque));
    }

    void Insert(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey, bool isDeletedSkey,
                bool isLastNode, const autil::StringView& value, regionid_t regionId, int32_t fixedValueLen) override
    {
        assert(mChunkWriter);
        NormalOnDiskSKeyNode<SKeyType> node;
        if (isDeletedPkey) {
            node.SetPKeyDeleted(ts);
        } else if (isDeletedSkey) {
            node.SetSKeyDeleted(skey, ts);
        } else {
            node.SetSKeyNodeInfo(skey, ts, OnDiskSKeyOffset::INVALID_NODE_OFFSET, 0);

            if (fixedValueLen > 0) {
                assert((size_t)fixedValueLen == value.size());
                mChunkWriter->InsertItem(value.data(), value.size());
                mMaxValueLen = std::max(mMaxValueLen, value.size());
            } else {
                char encodeLenBuf[4];
                size_t dataSize = autil::MultiValueFormatter::encodeCount(value.size(), encodeLenBuf, 4);
                mChunkWriter->InsertItem(encodeLenBuf, dataSize);
                mChunkWriter->AppendData(value.data(), value.size());
                mMaxValueLen = std::max(mMaxValueLen, dataSize + value.size());
            }
        }
        node.SetLastNode(isLastNode);
        assert(mSkeyNodeDeque);

        SKeyNodeItem nodeItem(pkey, regionId, node);
        char* appendBase = nodeItem.appendData;
        char* skeyAreaEnd = appendBase + SKeyAppendSize;
        if (mStoreTs) {
            if (appendBase + sizeof(ts) > skeyAreaEnd) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "skey append size [%lu] too small, can not store ts",
                                     SKeyAppendSize);
            }
            ::memcpy(appendBase, &ts, sizeof(ts));
            appendBase += sizeof(ts);
        }
        if (mStoreExpireTime) {
            if (appendBase + sizeof(expireTime) > skeyAreaEnd) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "skey append size [%lu] too small, can not store expireTime",
                                     SKeyAppendSize);
            }
            ::memcpy(appendBase, &expireTime, sizeof(expireTime));
            appendBase += sizeof(expireTime);
        }
        assert(appendBase - nodeItem.appendData == SKeyAppendSize);

        mSkeyNodeDeque->push_back(nodeItem);

        if (mChunkStrategy->NeedFlush(mChunkWriter)) {
            Base::Flush();
        }
    }

    void UpdateFlushSkeyNodeInfo(size_t chunkOffset, const std::vector<uint32_t>& inChunkLocators) override
    {
        if (inChunkLocators.empty()) {
            return;
        }

        if (chunkOffset >= OnDiskSKeyOffset::MAX_VALID_CHUNK_OFFSET) {
            INDEXLIB_THROW(util::UnSupportedException, "kkv value chunkOffset over %lu",
                           OnDiskSKeyOffset::MAX_VALID_CHUNK_OFFSET);
        }

        size_t cursor = 0;
        typename SKeyNodeDeque::iterator iter = mSkeyNodeDeque->begin();
        for (; iter != mSkeyNodeDeque->end(); iter++) {
            if (iter->node.IsValidNode()) {
                // skip delete pkey & delete skey & be over-write node
                continue;
            }

            OnDiskSKeyOffset& offset = iter->node.GetOffset();
            // chunkOffset = actual chunkOffset / 8;
            offset.chunkOffset = (chunkOffset >> OnDiskValueChunkAlignBit);
            assert(cursor < inChunkLocators.size());
            if (inChunkLocators[cursor] >= KKV_CHUNK_SIZE_THRESHOLD) {
                INDEXLIB_THROW(util::InconsistentStateException, "kkv value in chunkOffset over %lu",
                               KKV_CHUNK_SIZE_THRESHOLD);
            }
            offset.inChunkOffset = inChunkLocators[cursor++];
        }
        assert(cursor == inChunkLocators.size());
    }

private:
    SKeyNodeDequePtr mSkeyNodeDeque;

private:
    AUTIL_LOG_DECLARE();

private:
    friend class KKVValueDumperTest;
};

template <typename SKeyType, size_t SKeyAppendSize>
alog::Logger*
    KKVValueDumper<SKeyType, SKeyAppendSize>::_logger = alog::Logger::getLogger("indexlib.index.KKVValueDumper");

}} // namespace indexlib::index
