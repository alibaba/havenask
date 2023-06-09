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
#ifndef __INDEXLIB_RECOVER_RT_OPERATION_REDO_STRATEGY_H
#define __INDEXLIB_RECOVER_RT_OPERATION_REDO_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"

namespace indexlib { namespace partition {

class RecoverRtOperationRedoStrategy : public OperationRedoStrategy
{
public:
    RecoverRtOperationRedoStrategy(const index_base::Version& realtimeVersion);
    ~RecoverRtOperationRedoStrategy();

public:
    bool NeedRedo(segmentid_t operationSegment, OperationBase* operation, OperationRedoHint& redoHint);
    const std::set<segmentid_t>& GetSkipDeleteSegments() const { return mSkipDeleteSegments; }

private:
    std::set<segmentid_t> mSkipDeleteSegments;
    index_base::Version mRealtimeVersion;
    bool mOptimizeRedo;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RecoverRtOperationRedoStrategy);
}} // namespace indexlib::partition

#endif //__INDEXLIB_RECOVER_RT_OPERATION_REDO_STRATEGY_H
