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
#include "indexlib/partition/operation_queue/optimized_reopen_redo_strategy.h"

#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using indexlib::index_base::PartitionDataPtr;
using indexlib::index_base::Version;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OptimizedReopenRedoStrategy);

OptimizedReopenRedoStrategy::OptimizedReopenRedoStrategy() {}

OptimizedReopenRedoStrategy::~OptimizedReopenRedoStrategy() {}

void OptimizedReopenRedoStrategy::Init(const index_base::PartitionDataPtr& newPartitionData, Version& oldVersion,
                                       const config::IndexPartitionSchemaPtr& schema)
{
    Version newVersion = newPartitionData->GetOnDiskVersion();
    Version diffVersion = newVersion - oldVersion;
    IE_LOG(INFO, "new version [%s]", ToJsonString(newVersion).c_str());
    IE_LOG(INFO, "old version [%s]", ToJsonString(oldVersion).c_str());
    IE_LOG(INFO, "diff version [%s]", ToJsonString(diffVersion).c_str());
    map<segmentid_t, pair<docid_t, docid_t>> docRange;
    map<segmentid_t, pair<docid_t, docid_t>> subDocRange;
    CaculatorDocRange(newPartitionData, docRange);
    if (newPartitionData->GetSubPartitionData() != nullptr) {
        auto deleteIter = newVersion.CreateIterator();
        auto updateIter = diffVersion.CreateIterator();
        CaculatorDocRange(newPartitionData->GetSubPartitionData(), subDocRange);
        FillDocRange(deleteIter, subDocRange, mSubDeleteDocRange);
        FillDocRange(updateIter, subDocRange, mSubUpdateDocRange);
    }

    auto deleteIterMain = newVersion.CreateIterator();
    auto updateIterMain = diffVersion.CreateIterator();
    FillDocRange(deleteIterMain, docRange, mDeleteDocRange);
    FillDocRange(updateIterMain, docRange, mUpdateDocRange);
}

void OptimizedReopenRedoStrategy::CaculatorDocRange(const index_base::PartitionDataPtr& partitionData,
                                                    map<segmentid_t, pair<docid_t, docid_t>>& docRange)
{
    index_base::SegmentIteratorPtr iter = partitionData->CreateSegmentIterator()->CreateIterator(index_base::SIT_BUILT);
    while (iter->IsValid()) {
        segmentid_t segmentId = iter->GetSegmentId();
        if (!index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentId)) {
            uint64_t docCount = iter->GetSegmentData().GetSegmentInfo()->docCount;
            exdocid_t baseDocId = iter->GetBaseDocId();
            docRange.insert(make_pair(segmentId, make_pair(baseDocId, baseDocId + docCount)));
        }
        iter->MoveToNext();
    }
}

void OptimizedReopenRedoStrategy::FillDocRange(Version::Iterator& iter,
                                               const map<segmentid_t, pair<docid_t, docid_t>>& globalDocRange,
                                               vector<pair<docid_t, docid_t>>& docRange)
{
    while (iter.HasNext()) {
        segmentid_t segId = iter.Next();
        auto it = globalDocRange.find(segId);
        if (it != globalDocRange.end()) {
            docRange.push_back(it->second);
        }
    }
}

bool OptimizedReopenRedoStrategy::NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                                           OperationRedoHint& redoHint)
{
    auto docOpType = operation->GetDocOperateType();
    redoHint.SetHintType(OperationRedoHint::REDO_DOC_RANGE);
    if (docOpType == UPDATE_FIELD) {
        redoHint.SetCachedDocRanges(mUpdateDocRange, mSubUpdateDocRange);
    } else if (docOpType == DELETE_DOC || docOpType == DELETE_SUB_DOC) {
        redoHint.SetCachedDocRanges(mDeleteDocRange, mSubDeleteDocRange);
    } else {
        IE_LOG(ERROR, "unknow op type [%d]", docOpType);
        assert(false);
        return false;
    }
    return true;
}
}} // namespace indexlib::partition
