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
#include "indexlib/partition/open_executor/redo_and_lock_executor.h"

#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RedoAndLockExecutor);
typedef util::SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
DEFINE_SHARED_PTR(SimpleMemoryQuotaController);

bool RedoAndLockExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeRedoTime(resource.mOnlinePartMetrics.GetRedoLatencyMetric().get());
    IE_LOG(INFO, "Begin redo and lock execute for version [%d]", resource.mIncVersion.GetVersionId());
    util::BlockMemoryQuotaControllerPtr blockMemController(
        new util::BlockMemoryQuotaController(resource.mPartitionMemController, "redo_and_lock_executor"));

    PartitionModifierPtr modifier = OpenExecutorUtil::CreateInplaceModifier(resource.mOptions, resource.mSchema,
                                                                            resource.mBranchPartitionDataHolder.Get());

    OptimizedReopenRedoStrategyPtr redoStrategy(new OptimizedReopenRedoStrategy);
    redoStrategy->Init(resource.mBranchPartitionDataHolder.Get(), resource.mLoadedIncVersion, resource.mSchema);
    IE_LOG(INFO, "Begin redo operations for version [%d]", resource.mIncVersion.GetVersionId());
    OperationCursor lastCursor;
    size_t redoLoop = 0;
    while (!ReachRedoTarget(redoLoop)) {
        index_base::PartitionDataPtr clonedData(resource.mPartitionDataHolder.Get()->Clone());
        util::SimpleMemoryQuotaControllerPtr memController(new util::SimpleMemoryQuotaController(blockMemController));
        OperationReplayer opReplayer(clonedData, resource.mSchema, memController);
        redoLoop++;
        if (!DoOperations(modifier, resource.mIncVersion, redoStrategy, opReplayer, lastCursor)) {
            IE_LOG(WARN, "redo Failed on loop [%lu]", redoLoop);
            return false;
        }
    }
    mLock->lock();
    util::SimpleMemoryQuotaControllerPtr memController(new util::SimpleMemoryQuotaController(blockMemController));
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetLockedRedoLatencyMetric().get());
    OperationReplayer opReplayer(resource.mPartitionDataHolder.Get(), resource.mSchema, memController);
    IE_LOG(INFO, "lock data lock for redo final operations for version [%d]", resource.mIncVersion.GetVersionId());
    mHasLocked = true;
    bool ret = DoOperations(modifier, resource.mIncVersion, redoStrategy, opReplayer, lastCursor);
    IE_LOG(INFO, "End redo final operations for version [%d]", resource.mIncVersion.GetVersionId());
    IE_LOG(INFO, "End redo and lock execute for version [%d]", resource.mIncVersion.GetVersionId());
    return ret;
}

bool RedoAndLockExecutor::DoOperations(const PartitionModifierPtr& modifier, const index_base::Version& incVersion,
                                       OptimizedReopenRedoStrategyPtr& redoStrategy, OperationReplayer& opReplayer,
                                       OperationCursor& cursor)
{
    opReplayer.Seek(cursor);
    if (!opReplayer.RedoOperations(modifier, incVersion, redoStrategy)) {
        return false;
    }
    cursor = opReplayer.GetCursor();
    return true;
}

bool RedoAndLockExecutor::ReachRedoTarget(size_t currentRedoLoop)
{
    if (currentRedoLoop >= mMaxRedoTime) {
        return true;
    }
    return false;
}

void RedoAndLockExecutor::Drop(ExecutorResource& resource)
{
    if (mHasLocked) {
        mLock->unlock();
    }
}
}} // namespace indexlib::partition
