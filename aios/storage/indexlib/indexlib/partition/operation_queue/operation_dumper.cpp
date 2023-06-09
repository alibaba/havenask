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
#include "indexlib/partition/operation_queue/operation_dumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"

using namespace std;

using namespace indexlib::file_system;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationDumper);

void OperationDumper::Init(const OperationBlockVec& opBlocks, bool releaseBlockAfterDump, bool dumpToDisk)
{
    mOpBlockVec.assign(opBlocks.begin(), opBlocks.end());
    // last building operation block may be empty, and has no corresponding block meta
    if (mOpBlockVec.back()->Size() == 0) {
        mOpBlockVec.pop_back();
    }

    if (mOpBlockVec.size() != mOperationMeta.GetBlockMetaVec().size()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "operation block size [%lu] not equal to block meta vector[%lu]",
                             mOpBlockVec.size(), mOperationMeta.GetBlockMetaVec().size());
    }
    CheckOperationBlocks();
    mReleaseBlockAfterDump = releaseBlockAfterDump;
    mDumpToDisk = dumpToDisk;
}

void OperationDumper::Dump(const DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool)
{
    IE_LOG(INFO, "Begin dump operation to directory: %s", directory->DebugString().c_str());
    // dump data file
    FileWriterPtr dataFileWriter = directory->CreateFileWriter(
        OPERATION_DATA_FILE_NAME, mDumpToDisk ? file_system::WriterOption::BufferAtomicDump() : WriterOption());
    dataFileWriter->ReserveFile(mOperationMeta.GetTotalDumpSize()).GetOrThrow();

    const OperationMeta::BlockMetaVec& blockMetas = mOperationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < mOpBlockVec.size(); i++) {
        mOpBlockVec[i]->Dump(dataFileWriter, blockMetas[i].maxOperationSerializeSize, mReleaseBlockAfterDump);
    }

    size_t actualDumpSize = dataFileWriter->GetLength();
    if (actualDumpSize != mOperationMeta.GetTotalDumpSize()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "actual dump size[%lu] is not equal with estimated dump size[%lu]",
                             actualDumpSize, mOperationMeta.GetTotalDumpSize());
    }
    dataFileWriter->Close().GetOrThrow();
    // dump meta file
    directory->Store(OPERATION_META_FILE_NAME, mOperationMeta.ToString(), WriterOption::AtomicDump());
    IE_LOG(INFO, "Dump operation data length is %lu bytes, total operation count : %lu.", actualDumpSize,
           mOperationMeta.GetOperationCount());
    IE_LOG(INFO, "End dump operation to directory: %s", directory->DebugString().c_str());
}

void OperationDumper::CheckOperationBlocks() const
{
    const OperationMeta::BlockMetaVec& blockMetaVec = mOperationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < blockMetaVec.size(); i++) {
        if (blockMetaVec[i].operationCompress != blockMetaVec[0].operationCompress) {
            INDEXLIB_FATAL_ERROR(Runtime, "operation blocks are not the same compress type!");
        }
    }
}
}} // namespace indexlib::partition
