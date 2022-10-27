#include "indexlib/partition/operation_queue/file_operation_block.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"
#include "indexlib/file_system/file_reader.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, FileOperationBlock);

void FileOperationBlock::Init(const FileReaderPtr& fileReader, 
                              size_t beginOffset,const BlockMeta& blockMeta)
{
    mFileReader = fileReader;
    mReadOffset = beginOffset;
    if (mReadOffset + blockMeta.dumpSize > mFileReader->GetLength())
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "block meta[length:%lu] is not matched with operation log[%s]", 
                             blockMeta.dumpSize,fileReader->GetPath().c_str());
    }
    mBlockMeta = blockMeta;
    mMinTimestamp = mBlockMeta.minTimestamp;
    mMaxTimestamp = mBlockMeta.maxTimestamp;
}

OperationBlockPtr FileOperationBlock::CreateOperationBlockForRead(
        const OperationFactory& opFactory)
{
    OperationBlockPtr opBlock(new OperationBlock(mBlockMeta.operationCount));
    Pool* pool = opBlock->GetPool();

    const char *opBuffer = CreateOperationBuffer(pool);
    const char* baseAddr = opBuffer;

    for (size_t i = 0; i < mBlockMeta.operationCount; ++i)
    {
        size_t opSize = 0;
        OperationBase* operation = opFactory.DeserializeOperation(baseAddr, pool, opSize);
        opBlock->AddOperation(operation);
        baseAddr += opSize;
    }
    assert((size_t)(baseAddr - opBuffer) == mBlockMeta.serializeSize);
    return opBlock;
}

const char* FileOperationBlock::CreateOperationBuffer(Pool* pool)
{
    if (!mBlockMeta.operationCompress)
    {
        return (char*)(mFileReader->GetBaseAddress()) + mReadOffset;
    }

    SnappyCompressor compressor;
    compressor.SetBufferInLen(mBlockMeta.dumpSize);
    compressor.AddDataToBufferIn((char*)(mFileReader->GetBaseAddress()) + mReadOffset, 
                                 mBlockMeta.dumpSize);
    if (!compressor.Decompress())
    {
        INDEXLIB_FATAL_ERROR(Runtime, "fail to decompress dumped operation block");
    }
    size_t bufLen = compressor.GetBufferOutLen();
    if (bufLen != mBlockMeta.serializeSize)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "block decompress size[%lu] "
                             "is not equal to block serializeSize[%lu]",
                             bufLen, mBlockMeta.serializeSize);
    }
    return (char*)memcpy((pool->allocate(bufLen)), 
                  compressor.GetBufferOut(), bufLen);
}


IE_NAMESPACE_END(partition);

