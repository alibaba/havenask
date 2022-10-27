#include "indexlib/partition/operation_queue/normal_segment_operation_iterator.h"
#include "indexlib/partition/operation_queue/file_operation_block.h"
#include "indexlib/file_system/directory.h"

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, NormalSegmentOperationIterator);

void NormalSegmentOperationIterator::Init(const DirectoryPtr& operationDirectory)
{
    if (mOpMeta.GetOperationCount() == 0
        || mBeginPos > mOpMeta.GetOperationCount() - 1)
    {
        IE_LOG(INFO, "beginPos [%lu] is not valid, operation count is [%lu]",
               mBeginPos, mOpMeta.GetOperationCount());
        mLastCursor.pos = (int32_t)mOpMeta.GetOperationCount() - 1;
        return;
    }

    mFileReader = operationDirectory->CreateFileReader(
        OPERATION_DATA_FILE_NAME, FSOT_MMAP);

    assert(mFileReader);
    OperationBlockVec opBlockVec;
    const OperationMeta::BlockMetaVec& blockMeta = mOpMeta.GetBlockMetaVec();
    size_t beginOffset = 0;
    for (size_t i = 0; i < blockMeta.size(); ++i)
    {
        FileOperationBlockPtr opBlock(new FileOperationBlock());
        opBlock->Init(mFileReader, beginOffset, blockMeta[i]);
        beginOffset += blockMeta[i].dumpSize;
        opBlockVec.push_back(opBlock);
    }
    InMemSegmentOperationIterator::Init(opBlockVec);
}

IE_NAMESPACE_END(partition);

