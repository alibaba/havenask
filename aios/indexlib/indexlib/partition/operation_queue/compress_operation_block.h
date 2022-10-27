#ifndef __INDEXLIB_COMPRESS_OPERATION_BLOCK_H
#define __INDEXLIB_COMPRESS_OPERATION_BLOCK_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/partition/operation_queue/operation_block.h"

IE_NAMESPACE_BEGIN(partition);

class CompressOperationBlock : public OperationBlock
{
public:
    CompressOperationBlock();
    CompressOperationBlock(const CompressOperationBlock& other)
        : OperationBlock(other)
        , mOperationCount(other.mOperationCount)
        , mCompressDataBuffer(other.mCompressDataBuffer)
        , mCompressDataLen(other.mCompressDataLen)
    {}

    ~CompressOperationBlock();

public:
    void Init(size_t operationCount, 
              const char* compressBuffer, size_t compressSize,
              int64_t opMinTimeStamp, int64_t opMaxTimeStamp);

    OperationBlock* Clone() const override
    {
        assert(mPool);
        assert(mAllocator);
        return new CompressOperationBlock(*this);
    }

    size_t Size() const override { return mOperationCount; }
    void AddOperation(OperationBase *operation) override;

    OperationBlockPtr CreateOperationBlockForRead(
            const OperationFactory& mOpFactory) override;

    void Dump(const file_system::FileWriterPtr& fileWriter, 
              size_t maxOpSerializeSize, bool releaseAfterDump) override;

    size_t GetCompressSize() const
    { return mCompressDataLen; }

private:
    size_t mOperationCount;
    const char* mCompressDataBuffer;
    size_t mCompressDataLen;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressOperationBlock);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_COMPRESS_OPERATION_BLOCK_H
