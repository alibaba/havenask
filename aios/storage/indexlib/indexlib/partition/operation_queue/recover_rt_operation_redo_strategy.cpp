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
#include "indexlib/partition/operation_queue/recover_rt_operation_redo_strategy.h"

#include "indexlib/index_base/segment/realtime_segment_directory.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RecoverRtOperationRedoStrategy);

RecoverRtOperationRedoStrategy::RecoverRtOperationRedoStrategy(const index_base::Version& realtimeVersion)
    : mRealtimeVersion(realtimeVersion)
    , mOptimizeRedo(true)
{
    string envParam = autil::EnvUtil::getEnv("disable_recover_rt_op_redo_optimize");
    if (envParam == "true") {
        mOptimizeRedo = false;
    } else {
        mOptimizeRedo = true;
    }
}

RecoverRtOperationRedoStrategy::~RecoverRtOperationRedoStrategy() {}

bool RecoverRtOperationRedoStrategy::NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                                              OperationRedoHint& redoHint)
{
    redoHint.Reset();
    if (mOptimizeRedo) {
        auto opSegId = operation->GetSegmentId();
        if (opSegId == INVALID_SEGMENTID || opSegId >= operationSegment) {
            return false;
        }
        if (!mRealtimeVersion.HasSegment(opSegId)) {
            return false;
        }
        redoHint.SetHintType(OperationRedoHint::REDO_CACHE_SEGMENT);
        redoHint.AddCachedSegment(opSegId);
        return true;
    }
    bool hasCachedSegment = false;
    for (size_t i = 0; i < mRealtimeVersion.GetSegmentCount(); i++) {
        if (mRealtimeVersion[i] >= operationSegment) {
            break;
        }
        hasCachedSegment = true;
        redoHint.AddCachedSegment(mRealtimeVersion[i]);
    }
    if (!hasCachedSegment) {
        return false;
    }
    redoHint.SetHintType(OperationRedoHint::REDO_CACHE_SEGMENT);
    return true;
}
}} // namespace indexlib::partition
