#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/partition/operation_queue/operation_dumper.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CompressOperationWriter);

CompressOperationWriter::CompressOperationWriter()
    : mMaxOpBlockSerializeSize(1 * 1024 * 1024)
{}
    
CompressOperationWriter::CompressOperationWriter(size_t buildTotalMemUse)
    : mMaxOpBlockSerializeSize(1 * 1024 * 1024)
{
    mMaxOpBlockSerializeSize = GetCompressBufferSize(buildTotalMemUse);
    IE_LOG(INFO, "CompressOperationWriter reserve max operation block buffer size [%lu]",
           mMaxOpBlockSerializeSize);
}

CompressOperationWriter::~CompressOperationWriter() 
{
}

void CompressOperationWriter::CreateNewBlock(size_t maxBlockSize)
{
    FlushBuildingOperationBlock();
    OperationBlockPtr newBlock(new OperationBlock(maxBlockSize));
    mOpBlocks.push_back(newBlock);
    ResetBlockInfo(mOpBlocks);
}

CompressOperationBlockPtr CompressOperationWriter::CreateCompressOperationBlock()
{
    size_t blockSerializeSize = mOperationMeta.GetLastBlockSerializeSize();
    if (blockSerializeSize == 0)
    {
        return CompressOperationBlockPtr();
    }

    SnappyCompressor compressor;
    compressor.SetBufferInLen(blockSerializeSize);
    DynamicBuf& inBuffer = compressor.GetInBuffer();
    
    char* buffer = inBuffer.getBuffer();
    size_t bufLen = blockSerializeSize;
    size_t opCount = mBlockInfo.mCurBlock->Size();
    assert(opCount > 0);
    for (size_t i = 0; i < opCount; ++i)
    {
        size_t dumpSize = 
            OperationDumper::DumpSingleOperation(
                    mBlockInfo.mCurBlock->GetOperations()[i], buffer, bufLen);
        buffer += dumpSize;
        bufLen -= dumpSize;
    }
    if((size_t)(buffer - inBuffer.getBuffer()) != blockSerializeSize)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "fail to serialize operation block[size:%lu]", 
                             blockSerializeSize);
    }

    inBuffer.movePtr(blockSerializeSize);
    if (!compressor.Compress()) 
    {
        INDEXLIB_FATAL_ERROR(Runtime, "fail to compress operation block");
    }
    
    const char* outBuffer = compressor.GetBufferOut();
    size_t compressedSize = compressor.GetBufferOutLen();
    CompressOperationBlockPtr opBlock(new CompressOperationBlock);
    opBlock->Init(opCount, outBuffer, compressedSize, 
                  mBlockInfo.mCurBlock->GetMinTimestamp(), 
                  mBlockInfo.mCurBlock->GetMaxTimestamp());
    return opBlock;
}

void CompressOperationWriter::FlushBuildingOperationBlock()
{
    CompressOperationBlockPtr opBlock = CreateCompressOperationBlock();
    if (opBlock)
    {
        // replace curblock to compressblock
        mOpBlocks.pop_back();
        mOpBlocks.push_back(opBlock);
        mOperationMeta.EndOneCompressBlock(opBlock, (int64_t)mOpBlocks.size() - 1, 
                opBlock->GetCompressSize());
        UpdateBuildResourceMetrics();
    }
}

bool CompressOperationWriter::NeedCreateNewBlock() const
{
    return mOperationMeta.GetLastBlockSerializeSize() >= mMaxOpBlockSerializeSize
        || mBlockInfo.mCurBlock->Size() >= mMaxBlockSize;
}

size_t CompressOperationWriter::GetCompressBufferSize(size_t totalMemSize)
{
    size_t reserveSize = totalMemSize / 64;
    size_t maxBufSize = 64 * 1024 * 1024;
    size_t compBufSize = 1 * 1024 * 1024;
    while (compBufSize < reserveSize && compBufSize < maxBufSize)
    {
        compBufSize *= 2;
    }
    return compBufSize;
}

IE_NAMESPACE_END(partition);

