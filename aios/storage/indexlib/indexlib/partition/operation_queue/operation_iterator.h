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
#include "indexlib/partition/operation_queue/operation_cursor.h"
#include "indexlib/partition/operation_queue/segment_operation_iterator.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace partition {

class OperationIterator
{
public:
    typedef std::vector<index_base::SegmentData> SegmentDataVector;
    typedef std::vector<OperationMeta> OperationMetaVector;

public:
    OperationIterator(const index_base::PartitionDataPtr& partitionData, const config::IndexPartitionSchemaPtr& schema);

    ~OperationIterator() {}

public:
    // if operation.ts < timestamp, option will be skipped
    void Init(int64_t timestamp, const OperationCursor& cursor);
    bool HasNext();
    OperationBase* Next();
    OperationCursor GetLastCursor() const { return mLastOpCursor; }
    segmentid_t GetCurrentSegment() const;

    static size_t GetMaxUnObsoleteSegmentOperationSize(const index_base::PartitionDataPtr& partitionData,
                                                       int64_t reclaimTimestamp);

    static size_t GetTotalBuildingOperationCntFromCursor(const index_base::PartitionDataPtr& partitionData,
                                                         const OperationCursor& cursor);

private:
    bool LocateStartLoadPosition(const OperationCursor& skipCursor, int& segIdx, int& offset);
    bool LocateStartLoadPositionInNormalSegment(const OperationCursor& skipCursor, int& segIdx, int& offset);
    bool LocateStartLoadPositionInDumpingSegment(const OperationCursor& skipCursor, int& segIdx, int& offset);
    bool LocateStartLoadPositionInBuildingSegment(const OperationCursor& skipCursor, int& segIdx, int& offset);

    void InitBuiltSegments(const index_base::PartitionDataPtr& partitionData, const OperationCursor& skipCursor);

    void InitInMemSegments(const index_base::PartitionDataPtr& partitionData, const OperationCursor& skipCursor);

    SegmentOperationIteratorPtr LoadSegIterator(int segmentIdx, int offset);

    void InitSegmentIterator(const OperationCursor& cursor);

    void UpdateLastCursor(const SegmentOperationIteratorPtr& iter);

    static void ExtractUnObsolteRtSegmentDatas(const index_base::PartitionDataPtr& partitionData, int64_t timestamp,
                                               SegmentDataVector& segDatas);

    static void LoadOperationMeta(const SegmentDataVector& segDataVec, OperationMetaVector& segMetaVec);

    static void AppendInMemOperationMeta(const index_base::PartitionDataPtr& partitionData,
                                         OperationMetaVector& segMetaVec);

private:
    SegmentOperationIteratorPtr mCurrentSegIter;
    int mCurrentSegIdx;
    SegmentDataVector mSegDataVec;
    OperationMetaVector mSegMetaVec;
    std::vector<index_base::InMemorySegmentPtr> mDumpingSegments;
    index_base::InMemorySegmentPtr mBuildingSegment;
    OperationCursor mLastOpCursor;
    int64_t mTimestamp;

    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationIterator);

inline bool OperationIterator::HasNext()
{
    if (likely(mCurrentSegIter && mCurrentSegIter->HasNext())) {
        return true;
    }

    // !mCurrentSegIter || !mCurrentSegIter->HasNext()
    UpdateLastCursor(mCurrentSegIter);
    mCurrentSegIter = LoadSegIterator(mCurrentSegIdx + 1, 0);
    while (!mCurrentSegIter && mCurrentSegIdx < (int)(mSegDataVec.size() + mDumpingSegments.size())) {
        mCurrentSegIter = LoadSegIterator(mCurrentSegIdx + 1, 0);
    }
    return mCurrentSegIter != NULL;
}

inline OperationBase* OperationIterator::Next()
{
    assert(mCurrentSegIter);
    assert(mCurrentSegIter->HasNext());

    OperationBase* op = mCurrentSegIter->Next();
    mLastOpCursor = mCurrentSegIter->GetLastCursor();
    return op;
}

inline void OperationIterator::UpdateLastCursor(const SegmentOperationIteratorPtr& iter)
{
    if (likely(iter != NULL)) {
        OperationCursor cursor = iter->GetLastCursor();
        if (likely(cursor.pos >= 0)) // when not read any operation, cursor.pos = -1
        {
            mLastOpCursor = cursor;
        }
    }
}
}} // namespace indexlib::partition
