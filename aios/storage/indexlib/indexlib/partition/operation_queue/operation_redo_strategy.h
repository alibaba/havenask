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
#ifndef __INDEXLIB_OPERATION_REDO_STRATEGY_H
#define __INDEXLIB_OPERATION_REDO_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib { namespace partition {
struct OperationRedoCounter {
    OperationRedoCounter()
        : updateOpCount(0)
        , deleteOpCount(0)
        , otherOpCount(0)
        , skipRedoUpdateOpCount(0)
        , skipRedoDeleteOpCount(0)
        , hintOpCount(0)
    {
    }
    ~OperationRedoCounter() {}

    int64_t updateOpCount;
    int64_t deleteOpCount;
    int64_t otherOpCount;
    int64_t skipRedoUpdateOpCount;
    int64_t skipRedoDeleteOpCount;
    int64_t hintOpCount;
};

class OperationRedoStrategy
{
public:
    OperationRedoStrategy() {}
    virtual ~OperationRedoStrategy() {}

public:
    virtual bool NeedRedo(segmentid_t operationSegment, OperationBase* operation, OperationRedoHint& redoHint) = 0;
    const OperationRedoCounter& GetCounter() const { return mRedoCounter; }
    virtual const std::set<segmentid_t>& GetSkipDeleteSegments() const = 0;

protected:
    OperationRedoCounter mRedoCounter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationRedoStrategy);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATION_REDO_STRATEGY_H
