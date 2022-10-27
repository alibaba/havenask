#include "indexlib/partition/operation_queue/operation_dumper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationDumper);


void OperationDumper::Init(const OperationBlockVec& opBlocks, bool releaseBlockAfterDump)
{
    mOpBlockVec.assign(opBlocks.begin(), opBlocks.end());
    // last building operation block may be empty, and has no corresponding block meta 
    if (mOpBlockVec.back()->Size() == 0)
    {
        mOpBlockVec.pop_back();
    }

    if (mOpBlockVec.size() != mOperationMeta.GetBlockMetaVec().size())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, 
                             "operation block size [%lu] not equal to block meta vector[%lu]",
                             mOpBlockVec.size(), mOperationMeta.GetBlockMetaVec().size());
    }
    CheckOperationBlocks();
    mReleaseBlockAfterDump = releaseBlockAfterDump;
}

void OperationDumper::Dump(const DirectoryPtr& directory,
                           autil::mem_pool::PoolBase* dumpPool)
{
    IE_LOG(INFO, "Begin dump operation to directory: %s", directory->GetPath().c_str());
    // dump data file
    FileWriterPtr dataFileWriter =
        directory->CreateFileWriter(OPERATION_DATA_FILE_NAME);
    dataFileWriter->ReserveFileNode(mOperationMeta.GetTotalDumpSize());

    const OperationMeta::BlockMetaVec& blockMetas = 
        mOperationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < mOpBlockVec.size(); i++)
    {
        mOpBlockVec[i]->Dump(dataFileWriter,
                             blockMetas[i].maxOperationSerializeSize,
                             mReleaseBlockAfterDump);
    }

    size_t actualDumpSize = dataFileWriter->GetLength();
    if (actualDumpSize != mOperationMeta.GetTotalDumpSize())
    {
        INDEXLIB_FATAL_ERROR(
            InconsistentState,
            "actual dump size[%lu] is not equal with estimated dump size[%lu]",
            actualDumpSize, mOperationMeta.GetTotalDumpSize());
    }
    dataFileWriter->Close();
    // dump meta file
    directory->Store(OPERATION_META_FILE_NAME, mOperationMeta.ToString(), true);
    IE_LOG(INFO, "Dump operation data length is %lu bytes, total operation count : %lu.",
           actualDumpSize, mOperationMeta.GetOperationCount());
    IE_LOG(INFO, "End dump operation to directory: %s", directory->GetPath().c_str());
}

void OperationDumper::CheckOperationBlocks() const
{
    const OperationMeta::BlockMetaVec& blockMetaVec = mOperationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < blockMetaVec.size(); i++)
    {
        if (blockMetaVec[i].operationCompress != blockMetaVec[0].operationCompress)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "operation blocks are not the same compress type!");
        }
    }
}

IE_NAMESPACE_END(partition);

