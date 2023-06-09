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
#include "indexlib/partition/operation_queue/dump_operation_redo_strategy.h"

#include "indexlib/partition/operation_queue/update_field_operation.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpOperationRedoStrategy);

DumpOperationRedoStrategy::DumpOperationRedoStrategy(segmentid_t dumppedSegmentId) : mDumppedSegmentId(dumppedSegmentId)
{
}

bool DumpOperationRedoStrategy::NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                                         OperationRedoHint& redoHint)
{
    redoHint.Reset();
    auto opSegId = operation->GetSegmentId();
    auto docOpType = operation->GetDocOperateType();
    if (docOpType == UPDATE_FIELD && opSegId == mDumppedSegmentId) {
        // BUG_37663484
        // redoHint.SetSegmentId(opSegId);
        redoHint.AddCachedSegment(opSegId);
        redoHint.SetHintType(OperationRedoHint::REDO_CACHE_SEGMENT);
        return true;
    }
    return false;
}
}} // namespace indexlib::partition
