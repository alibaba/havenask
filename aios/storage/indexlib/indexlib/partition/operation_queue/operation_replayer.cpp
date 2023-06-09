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
#include "indexlib/partition/operation_queue/operation_replayer.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"
#include "indexlib/util/FutureExecutor.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationReplayer);

OperationReplayer::OperationReplayer(const index_base::PartitionDataPtr& partitionData,
                                     const config::IndexPartitionSchemaPtr& schema,
                                     const util::SimpleMemoryQuotaControllerPtr& memController, bool ignoreRedoFail)
    : mPartitionData(partitionData)
    , mSchema(schema)
    , mMemController(memController)
    , mRedoCount(0)
    , mIgnoreRedoFail(ignoreRedoFail)
{
}

bool OperationReplayer::RedoOneOperation(const PartitionModifierPtr& modifier, OperationBase* operation,
                                         const OperationIterator& iter, const OperationRedoHint& redoHint,
                                         future_lite::Executor* executor)
{
    const util::BuildResourceMetricsPtr& buildResMetrics = modifier->GetBuildResourceMetrics();
    bool processResult = operation->Process(modifier, redoHint, executor);
    if (!mIgnoreRedoFail && !processResult) {
        IE_LOG(WARN, "redo operation fail");
        return false;
    }
    ++mRedoCount;
    int64_t currentMemUse = buildResMetrics->GetValue(BMT_CURRENT_MEMORY_USE);
    int64_t currentUsedQuota = mMemController->GetUsedQuota();
    int64_t diff = 0;
    if (currentMemUse > currentUsedQuota) {
        diff = currentMemUse - currentUsedQuota;
        mMemController->Allocate(diff);
    }
    if (unlikely(mMemController->GetFreeQuota() <= 0)) {
        IE_LOG(WARN, "redo operation fail, lack of memory, allocate[%ld], free quota [%ld]", diff,
               mMemController->GetFreeQuota());
        mCursor = iter.GetLastCursor();
        return false;
    }
    return true;
}

bool OperationReplayer::RedoOperations(const PartitionModifierPtr& modifier, const Version& onDiskVersion,
                                       const OperationRedoStrategyPtr& redoStrategy)
{
    std::shared_ptr<future_lite::Executor> redoExecutor;
    if (modifier->GetPrimaryKeyIndexReader()->GetBuildExecutor()) {
        redoExecutor.reset(util::FutureExecutor::CreateExecutor(1, 32), [](auto* p) {
            if (p) {
                util::FutureExecutor::DestroyExecutor(p);
            }
        });
    }

    IE_LOG(INFO, "redo operations begin");
    OnlineJoinPolicy joinPolicy(onDiskVersion, mSchema->GetTableType(), mPartitionData->GetSrcSignature());

    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();

    OperationIterator iter(mPartitionData, mSchema);
    iter.Init(reclaimTimestamp, mCursor);

    size_t skipRedoCount = 0;
    ;
    OperationRedoHint redoHint;
    while (iter.HasNext()) {
        OperationBase* operation = iter.Next();
        redoHint.Reset();
        if (redoStrategy && !redoStrategy->NeedRedo(iter.GetCurrentSegment(), operation, redoHint)) {
            skipRedoCount++;
            continue;
        }
        if (!RedoOneOperation(modifier, operation, iter, redoHint, redoExecutor.get())) {
            return false;
        }
    }
    mCursor = iter.GetLastCursor();

    IE_LOG(INFO, "redo operations end, total skip redo operation count: %lu", skipRedoCount);
    if (redoStrategy) {
        const OperationRedoCounter& redoCounter = redoStrategy->GetCounter();
        IE_LOG(INFO, "redo counter [%ld][%ld][%ld][%ld][%ld][%ld]", redoCounter.updateOpCount,
               redoCounter.deleteOpCount, redoCounter.otherOpCount, redoCounter.skipRedoUpdateOpCount,
               redoCounter.skipRedoDeleteOpCount, redoCounter.hintOpCount);
    } else {
        IE_LOG(WARN, "redoStrategy is null.");
    }
    IE_LOG(INFO, "redo operations end, total redo operation count: %lu", mRedoCount);
    return true;
}
}} // namespace indexlib::partition
