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
#ifndef __INDEXLIB_REDO_AND_LOCK_EXECUTOR_H
#define __INDEXLIB_REDO_AND_LOCK_EXECUTOR_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/operation_queue/optimized_reopen_redo_strategy.h"

namespace indexlib { namespace util {
template <typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
DEFINE_SHARED_PTR(SimpleMemoryQuotaController);
}} // namespace indexlib::util

namespace indexlib { namespace partition {

class RedoAndLockExecutor : public OpenExecutor
{
public:
    RedoAndLockExecutor(autil::ThreadMutex* lock, int64_t maxRedoTime) : mLock(lock), mHasLocked(false)
    {
        mMaxRedoTime = maxRedoTime <= 0 ? DEFAULT_LOOP_TIME : maxRedoTime;
    }
    ~RedoAndLockExecutor() {}

public:
    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    bool ReachRedoTarget(size_t currentRedoLoop);
    bool DoOperations(const PartitionModifierPtr& modifier, const index_base::Version& incVersion,
                      OptimizedReopenRedoStrategyPtr& redoStrategy, OperationReplayer& opReplayer,
                      OperationCursor& cursor);

private:
    static constexpr size_t DEFAULT_LOOP_TIME = 4;
    autil::ThreadMutex* mLock;
    bool mHasLocked;
    int64_t mMaxRedoTime;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RedoAndLockExecutor);
}} // namespace indexlib::partition

#endif //__INDEXLIB_REDO_AND_LOCK_EXECUTOR_H
