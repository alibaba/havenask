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
#include "indexlib/partition/operation_queue/in_mem_segment_operation_iterator.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemSegmentOperationIterator);

void InMemSegmentOperationIterator::Init(const OperationBlockVec& opBlocks)
{
    mOpBlocks = opBlocks;

    if (mOpMeta.GetOperationCount() == 0 || mBeginPos > mOpMeta.GetOperationCount() - 1) {
        IE_LOG(INFO, "beginPos [%lu] is not valid, operation count is [%lu]", mBeginPos, mOpMeta.GetOperationCount());
        mLastCursor.pos = (int32_t)mOpMeta.GetOperationCount() - 1;
        return;
    }

    // seek to operation (to be read) by mBeginPos
    int32_t blockIdx = 0;
    size_t count = 0;
    while (count < mBeginPos && blockIdx < (int32_t)mOpBlocks.size()) {
        size_t blockOpCount = mOpBlocks[blockIdx]->Size();
        if (mBeginPos - count < blockOpCount) {
            break;
        }
        count += blockOpCount;
        ++blockIdx;
    }
    assert(count < mOpMeta.GetOperationCount());
    mOpBlockIdx = blockIdx;
    mInBlockOffset = mBeginPos - count;
    mLastCursor.pos = mBeginPos - 1;
    mReservedOperation = NULL;
    // seek next valid operation by timestamp
    SeekNextValidOpBlock();
    SeekNextValidOperation();
}

OperationBase* InMemSegmentOperationIterator::Next()
{
    assert(HasNext());
    SwitchToReadBlock(mOpBlockIdx);
    mReservedOperation = mCurBlockForRead->GetOperations()[mInBlockOffset];
    ToNextReadPosition();
    SeekNextValidOperation();
    OperationBase* op = mReservedOperation;
    mReservedOperation = NULL;
    return op;
}
}} // namespace indexlib::partition
