#ifndef __INDEXLIB_OPERATION_BLOCK_H
#define __INDEXLIB_OPERATION_BLOCK_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/util/mmap_vector.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(util, MMapAllocator);

IE_NAMESPACE_BEGIN(partition);
class OperationBlock;
DEFINE_SHARED_PTR(OperationBlock);

class OperationBlock
{
public:
    typedef util::MMapVector<OperationBase*> OperationVec;
    typedef std::tr1::shared_ptr<OperationVec> OperationVecPtr;

    const static int64_t OPERATION_POOL_CHUNK_SIZE = 
        DEFAULT_CHUNK_SIZE * 1024 * 1024; //10M

public:
    OperationBlock(size_t maxBlockSize);

    OperationBlock(const OperationBlock& other);

    virtual ~OperationBlock()
    {
        Reset();
    }

public:
    virtual OperationBlock* Clone() const;

    virtual OperationBlockPtr CreateOperationBlockForRead(
            const OperationFactory& mOpFactory)
    {
        return OperationBlockPtr(new OperationBlock(*this));
    }

    virtual void AddOperation(OperationBase *operation);
        
    size_t GetTotalMemoryUse() const;

    virtual size_t Size() const;

    virtual void Dump(const file_system::FileWriterPtr& fileWriter, 
                      size_t maxOpSerializeSize,
                      bool releaseAfterDump);

    autil::mem_pool::Pool* GetPool() const
    { return mPool.get(); }

    int64_t GetMinTimestamp() const
    { return mMinTimestamp; }
    
    int64_t GetMaxTimestamp() const
    { return mMaxTimestamp; }

    const OperationVec& GetOperations() const;

    void Reset();

private:
    void UpdateTimestamp(int64_t ts);

protected:
    typedef std::tr1::shared_ptr<autil::mem_pool::ChunkAllocatorBase> AllocatorPtr;
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;

    OperationVecPtr mOperations;
    int64_t mMinTimestamp;
    int64_t mMaxTimestamp;
    PoolPtr mPool;
    util::MMapAllocatorPtr mAllocator;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<OperationBlockPtr> OperationBlockVec;


///////////////////////////////////////////////////////

class OperationBlockInfo
{
public:
    OperationBlockInfo()
        : mNoLastBlockOpCount(0)
        , mNoLastBlockMemUse(0)
    {}

    OperationBlockInfo(size_t noLastBlockOpCount,
                       size_t noLastBlockMemUse,
                       const OperationBlockPtr& curBlock)
        : mNoLastBlockOpCount(noLastBlockOpCount)
        , mNoLastBlockMemUse(noLastBlockMemUse)
        , mCurBlock(curBlock)
    {}

    size_t GetTotalMemoryUse() const
    { 
        assert(mCurBlock);
        return mNoLastBlockMemUse + mCurBlock->GetTotalMemoryUse();
    }

    size_t Size() const
    {
        assert(mCurBlock);
        return mNoLastBlockOpCount + mCurBlock->Size();
    }

    autil::mem_pool::Pool* GetPool() const
    {
        assert(mCurBlock);
        return mCurBlock->GetPool();
    }

    bool operator==(const OperationBlockInfo& other) const
    {
        return mNoLastBlockOpCount == other.mNoLastBlockOpCount
            && mNoLastBlockMemUse == other.mNoLastBlockMemUse
            && mCurBlock.get() == other.mCurBlock.get();
    }

public:
    size_t mNoLastBlockOpCount;
    size_t mNoLastBlockMemUse;
    OperationBlockPtr mCurBlock;
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_BLOCK_H
