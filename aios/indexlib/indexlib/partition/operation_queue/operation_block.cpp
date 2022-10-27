#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/partition/operation_queue/operation_dumper.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/util/mmap_allocator.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationBlock);

OperationBlock::OperationBlock(size_t maxBlockSize)
    : mMinTimestamp(INVALID_TIMESTAMP)
    , mMaxTimestamp(INVALID_TIMESTAMP)
{
    mAllocator.reset(new util::MMapAllocator);
    mPool.reset(new autil::mem_pool::Pool(
                    mAllocator.get(), OPERATION_POOL_CHUNK_SIZE));
    if (maxBlockSize > 0)
    {
        mOperations.reset(new OperationVec);
        mOperations->Init(mAllocator, maxBlockSize);
    }
}

OperationBlock::OperationBlock(const OperationBlock& other)
    : mOperations(other.mOperations)
    , mMinTimestamp(other.mMinTimestamp)
    , mMaxTimestamp(other.mMaxTimestamp)
    , mPool(other.mPool)
    , mAllocator(other.mAllocator)
{
}

void OperationBlock::Dump(const FileWriterPtr& fileWriter, 
                          size_t maxOpSerializeSize,
                          bool releaseAfterDump)
{
    assert(fileWriter);
    vector<char> buffer;
    buffer.resize(maxOpSerializeSize);
    for (size_t i = 0; i < Size(); ++i)
    {
        assert(mOperations);
        OperationBase* op = (*mOperations)[i];
        size_t dumpSize = OperationDumper::DumpSingleOperation(
                op, (char*)buffer.data(), buffer.size());
        fileWriter->Write(buffer.data(), dumpSize);
    }

    if (releaseAfterDump)
    {
        // release operation memory, avoid dump memory peak
        Reset();
    }
}

OperationBlock* OperationBlock::Clone() const
{
    assert(mPool);
    assert(mAllocator);
    return new OperationBlock(*this);
}

void OperationBlock::Reset()
{
    // OperationBlockForRead & buildOperationBlock shared same mOperations
    // should call ~OperationBase only once
    if (mOperations && mOperations.use_count() == 1)
    {
        for (size_t i = 0; i < mOperations->Size(); i++)
        {
            (*mOperations)[i]->~OperationBase();
        }
    }
    mOperations.reset();
    mPool.reset();
    mAllocator.reset();
}

void OperationBlock::AddOperation(OperationBase *operation)
{
    assert(operation);
    assert(mOperations);
    UpdateTimestamp(operation->GetTimestamp());
    mOperations->PushBack(operation);
}

size_t OperationBlock::GetTotalMemoryUse() const
{ 
    size_t memUse = 0;
    if (mOperations)
    {
        memUse += mOperations->GetUsedBytes(); 
    }
    if (mPool)
    {
        memUse += mPool->getUsedBytes();
    }
    return memUse;
}

size_t OperationBlock::Size() const
{
    return mOperations ? mOperations->Size() : 0;
}

const OperationBlock::OperationVec& OperationBlock::GetOperations() const
{
    if (mOperations)
    {
        return *mOperations;
    }

    static OperationVec emptyOpVec;
    return emptyOpVec;
}

void OperationBlock::UpdateTimestamp(int64_t ts)
{
    assert(mOperations);
    assert(mMinTimestamp <= mMaxTimestamp);
    if (unlikely(mOperations->Empty()))
    {
        mMaxTimestamp = ts;
        mMinTimestamp = ts;
        return;
    }

    if (ts > mMaxTimestamp)
    {
        mMaxTimestamp = ts;
        return;
    }

    if (ts < mMinTimestamp)
    {
        mMinTimestamp = ts;
    }
}

IE_NAMESPACE_END(partition);

