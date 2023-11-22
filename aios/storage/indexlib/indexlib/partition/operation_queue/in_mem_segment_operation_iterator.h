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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/partition/operation_queue/segment_operation_iterator.h"

namespace indexlib { namespace partition {

class InMemSegmentOperationIterator : public SegmentOperationIterator
{
public:
    InMemSegmentOperationIterator(const config::IndexPartitionSchemaPtr& schema, const OperationMeta& operationMeta,
                                  segmentid_t segmentId, size_t offset, int64_t timestamp)
        : SegmentOperationIterator(schema, operationMeta, segmentId, offset, timestamp)
        , mOpBlockIdx(-1)
        , mInBlockOffset(-1)
        , mCurReadBlockIdx(-1)
        , mReservedOperation(NULL)
    {
    }

    ~InMemSegmentOperationIterator()
    {
        for (size_t i = 0; i < mReservedOpVec.size(); ++i) {
            mReservedOpVec[i]->~OperationBase();
        }
    }

public:
    void Init(const OperationBlockVec& opBlocks);
    OperationBase* Next() override;

private:
    void ToNextReadPosition();
    void SeekNextValidOpBlock();
    void SeekNextValidOperation();
    void SwitchToReadBlock(int32_t blockIdx);

private:
    static const size_t RESET_POOL_THRESHOLD = 10 * 1024 * 1024; // 10 Mbytes

protected:
    OperationBlockVec mOpBlocks;
    int32_t mOpBlockIdx;    // point to operation block to be read by next
    int32_t mInBlockOffset; // point to in block position to be read by next
    OperationBlockPtr mCurBlockForRead;
    int32_t mCurReadBlockIdx;

    // operation is created by Pool in OpBlock.
    // Next() may trigger switch operation block.
    // In this case, clone the operation to ensure life cycle
    OperationBase* mReservedOperation;
    std::vector<OperationBase*> mReservedOpVec;
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSegmentOperationIterator);

///////////////////////////////////////////////////////////////
inline void InMemSegmentOperationIterator::ToNextReadPosition()
{
    assert(mOpBlockIdx < (int32_t)mOpBlocks.size());

    ++mLastCursor.pos;
    ++mInBlockOffset;
    if (mInBlockOffset == (int32_t)mOpBlocks[mOpBlockIdx]->Size()) {
        ++mOpBlockIdx;
        mInBlockOffset = 0;
        SeekNextValidOpBlock();
    }
}

inline void InMemSegmentOperationIterator::SeekNextValidOpBlock()
{
    while (mOpBlockIdx < (int32_t)mOpBlocks.size() && mOpBlocks[mOpBlockIdx]->GetMaxTimestamp() < mTimestamp) {
        if (mOpBlockIdx == mOpBlocks.size() - 1) {
            // last block
            mLastCursor.pos = int32_t(mOpMeta.GetOperationCount()) - 1;
            break;
        }
        mLastCursor.pos += mOpBlocks[mOpBlockIdx]->Size() - mInBlockOffset;
        ++mOpBlockIdx;
        mInBlockOffset = 0;
    }
}

inline void InMemSegmentOperationIterator::SwitchToReadBlock(int32_t blockIdx)
{
    if (blockIdx == mCurReadBlockIdx) {
        return;
    }
    if (mReservedOperation) {
        if (mPool.getUsedBytes() >= RESET_POOL_THRESHOLD) {
            for (size_t i = 0; i < mReservedOpVec.size(); ++i) {
                mReservedOpVec[i]->~OperationBase();
            }
            mReservedOpVec.clear();
            mPool.reset();
        }
        mReservedOperation = mReservedOperation->Clone(&mPool);
        mReservedOpVec.push_back(mReservedOperation);
    }
    mCurBlockForRead = mOpBlocks[blockIdx]->CreateOperationBlockForRead(mOpFactory);
    mCurReadBlockIdx = blockIdx;
}

inline void InMemSegmentOperationIterator::SeekNextValidOperation()
{
    while (HasNext()) {
        SwitchToReadBlock(mOpBlockIdx);
        OperationBase* operation = mCurBlockForRead->GetOperations()[mInBlockOffset];
        if (operation->GetTimestamp() >= mTimestamp) {
            break;
        }
        ToNextReadPosition();
    }
}
}} // namespace indexlib::partition
