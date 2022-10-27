#ifndef __INDEXLIB_COMMON_OPERATION_QUEUE_OPERATION_META_H
#define __INDEXLIB_COMMON_OPERATION_QUEUE_OPERATION_META_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_block.h"

IE_NAMESPACE_BEGIN(partition);

class OperationMeta : public autil::legacy::Jsonizable
{
public:
    struct BlockMeta : public autil::legacy::Jsonizable
    {
    public:
        BlockMeta()
            : minTimestamp(INVALID_TIMESTAMP)
            , maxTimestamp(INVALID_TIMESTAMP)
            , serializeSize(0)
            , maxOperationSerializeSize(0)
            , dumpSize(0)
            , operationCount(0)
            , operationCompress(false)
        {}

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
        {
            json.Jsonize("min_operation_timestamp", minTimestamp, minTimestamp);
            json.Jsonize("max_operation_timestamp", maxTimestamp, maxTimestamp);
            json.Jsonize("block_serialize_size", serializeSize, serializeSize);
            json.Jsonize("max_operation_serialize_size", 
                         maxOperationSerializeSize, maxOperationSerializeSize);
            json.Jsonize("block_dump_size", dumpSize, dumpSize);
            json.Jsonize("operation_count", operationCount, operationCount);
            json.Jsonize("operation_compress", operationCompress, operationCompress);
        }

    public:
        int64_t minTimestamp;
        int64_t maxTimestamp;
        size_t serializeSize;
        size_t maxOperationSerializeSize;
        size_t dumpSize;
        uint32_t operationCount;
        bool operationCompress;
    };

    typedef std::vector<BlockMeta> BlockMetaVec;

public:
    OperationMeta()
        : mMaxOperationSerializeSize(0)
        , mMaxOperationSerializeSizeInCurBlock(0)
        , mOperationCount(0)
        , mNoLastBlockSerializeSize(0)
        , mTotalSerializeSize(0)
        , mTotalDumpSize(0)
    {}
    
    ~OperationMeta() {}
    
public:
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("total_serialize_size", mTotalSerializeSize, mTotalSerializeSize);
        json.Jsonize("total_dump_size", mTotalDumpSize, mTotalDumpSize);
        json.Jsonize("max_operation_serialize_size",
                     mMaxOperationSerializeSize, mMaxOperationSerializeSize);
        json.Jsonize("operation_count", mOperationCount, mOperationCount);
        json.Jsonize("operation_block_meta", mBlockMetaVec, mBlockMetaVec);
    }

    std::string ToString() const
    { return autil::legacy::ToJsonString(*this); }

    void InitFromString(const std::string& metaContent)
    { autil::legacy::FromJsonString(*this, metaContent);}
    
    void Update(size_t opSerializeSize)
    {
        mMaxOperationSerializeSizeInCurBlock = std::max(
                mMaxOperationSerializeSizeInCurBlock, opSerializeSize);
        mMaxOperationSerializeSize = std::max(
                mMaxOperationSerializeSize, opSerializeSize);
        ++mOperationCount;
        mTotalSerializeSize += opSerializeSize;
    }

    void EndOneBlock(const OperationBlockPtr& opBlock,
                     int64_t blockIdx)
    {
        if (opBlock->Size() == 0)
        {
            // empty block
            return;
        }

        if (blockIdx + 1 == (int64_t)mBlockMetaVec.size())
        {
            // already end current block
            return;
        }

        assert (blockIdx == (int64_t)mBlockMetaVec.size());
        assert(opBlock->GetMinTimestamp() <= opBlock->GetMaxTimestamp());

        BlockMeta blockMeta;
        blockMeta.minTimestamp = opBlock->GetMinTimestamp();
        blockMeta.maxTimestamp = opBlock->GetMaxTimestamp();
        blockMeta.operationCount = opBlock->Size();
        blockMeta.serializeSize = mTotalSerializeSize - mNoLastBlockSerializeSize;
        blockMeta.maxOperationSerializeSize = mMaxOperationSerializeSizeInCurBlock;
        blockMeta.dumpSize = blockMeta.serializeSize;
        blockMeta.operationCompress = false;

        mNoLastBlockSerializeSize = mTotalSerializeSize;
        mTotalDumpSize += blockMeta.dumpSize;
        mBlockMetaVec.push_back(blockMeta);
        mMaxOperationSerializeSizeInCurBlock = 0;
    }

    void EndOneCompressBlock(const OperationBlockPtr& opBlock,
                             int64_t blockIdx, size_t compressSize)
    {
        if (opBlock->Size() == 0)
        {
            // empty block
            return;
        }

        if (blockIdx + 1 == (int64_t)mBlockMetaVec.size())
        {
            // already end current block
            return;
        }

        assert (blockIdx == (int64_t)mBlockMetaVec.size());
        assert(opBlock->GetMinTimestamp() <= opBlock->GetMaxTimestamp());

        BlockMeta blockMeta;
        blockMeta.minTimestamp = opBlock->GetMinTimestamp();
        blockMeta.maxTimestamp = opBlock->GetMaxTimestamp();
        blockMeta.operationCount = opBlock->Size();
        blockMeta.serializeSize = mTotalSerializeSize - mNoLastBlockSerializeSize;
        blockMeta.maxOperationSerializeSize = mMaxOperationSerializeSizeInCurBlock;
        blockMeta.dumpSize = compressSize;
        blockMeta.operationCompress = true;

        mNoLastBlockSerializeSize = mTotalSerializeSize;
        mTotalDumpSize += blockMeta.dumpSize;
        mBlockMetaVec.push_back(blockMeta);
        mMaxOperationSerializeSizeInCurBlock = 0;
    }
    
    size_t GetLastBlockSerializeSize() const
    {
        return mTotalSerializeSize - mNoLastBlockSerializeSize;
    }

    size_t GetMaxOperationSerializeSize() const
    { return mMaxOperationSerializeSize; }

    virtual size_t GetTotalDumpSize() const
    { return mTotalDumpSize; }

    size_t GetTotalSerializeSize() const
    { return mTotalSerializeSize; }

    size_t GetOperationCount() const
    { return mOperationCount; }

    const OperationMeta::BlockMetaVec& GetBlockMetaVec() const
    { return mBlockMetaVec; }

private:
    size_t mMaxOperationSerializeSize;
    size_t mMaxOperationSerializeSizeInCurBlock;
    size_t mOperationCount;
    size_t mNoLastBlockSerializeSize;
    size_t mTotalSerializeSize;
    size_t mTotalDumpSize;
    BlockMetaVec mBlockMetaVec;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationMeta);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_COMMON_OPERATION_QUEUE_OPERATION_META_H
