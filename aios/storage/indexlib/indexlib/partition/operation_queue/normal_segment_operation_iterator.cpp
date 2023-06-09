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
#include "indexlib/partition/operation_queue/normal_segment_operation_iterator.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/partition/operation_queue/file_operation_block.h"

using namespace std;

using namespace indexlib::file_system;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, NormalSegmentOperationIterator);

void NormalSegmentOperationIterator::Init(const DirectoryPtr& operationDirectory)
{
    if (mOpMeta.GetOperationCount() == 0 || mBeginPos > mOpMeta.GetOperationCount() - 1) {
        IE_LOG(INFO, "beginPos [%lu] is not valid, operation count is [%lu]", mBeginPos, mOpMeta.GetOperationCount());
        mLastCursor.pos = (int32_t)mOpMeta.GetOperationCount() - 1;
        return;
    }

    mFileReader = operationDirectory->CreateFileReader(OPERATION_DATA_FILE_NAME, FSOT_MMAP);

    assert(mFileReader);
    OperationBlockVec opBlockVec;
    const OperationMeta::BlockMetaVec& blockMeta = mOpMeta.GetBlockMetaVec();
    size_t beginOffset = 0;
    for (size_t i = 0; i < blockMeta.size(); ++i) {
        FileOperationBlockPtr opBlock(new FileOperationBlock());
        opBlock->Init(mFileReader, beginOffset, blockMeta[i]);
        beginOffset += blockMeta[i].dumpSize;
        opBlockVec.push_back(opBlock);
    }
    InMemSegmentOperationIterator::Init(opBlockVec);
}
}} // namespace indexlib::partition
