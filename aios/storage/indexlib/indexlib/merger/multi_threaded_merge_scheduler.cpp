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
#include "indexlib/merger/multi_threaded_merge_scheduler.h"

#include <algorithm>

#include "indexlib/merger/merge_file_system.h"
#include "indexlib/util/resource_control_thread_pool.h"
using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MultiThreadedMergeScheduler);

//// MultiThreadedMergeScheduler
MultiThreadedMergeScheduler::MultiThreadedMergeScheduler(int64_t maxMemUse, uint32_t threadNum)
    : mMaxMemUse(maxMemUse)
    , mThreadNum(threadNum)
{
}

MultiThreadedMergeScheduler::~MultiThreadedMergeScheduler() {}

void MultiThreadedMergeScheduler::Run(const vector<ResourceControlWorkItemPtr>& workItems,
                                      const MergeFileSystemPtr& mergeFileSystem)
{
    IE_LOG(INFO, "instance start run [%lu] merge work items", workItems.size());
    uint32_t workItemCount = workItems.size();
    if (workItemCount == 0) {
        IE_LOG(INFO, "instance has no work items to merge");
        return;
    }
    vector<int64_t> requiredMemVec(workItemCount, 0);
    for (size_t i = 0; i < workItemCount; i++) {
        requiredMemVec[i] = workItems[i]->GetRequiredResource();
    }
    vector<int64_t>::iterator it = std::max_element(requiredMemVec.begin(), requiredMemVec.end());
    int64_t maxRequiredMemUse = *it;
    // make sure all work item can success
    if (maxRequiredMemUse > mMaxMemUse) {
        IE_LOG(WARN, "maxMemUse[%ld] is too small, adjust to maxRequiredMemUse[%ld]", mMaxMemUse, maxRequiredMemUse);
        mMaxMemUse = maxRequiredMemUse;
    }

    auto onThreadNormalExitFunc = [mergeFileSystem]() {
        IE_LOG(INFO, "merge thread exit");
        mergeFileSystem->Commit();
    };

    ResourceControlThreadPool threadPool(mThreadNum, mMaxMemUse);
    threadPool.Init(workItems, onThreadNormalExitFunc);
    threadPool.Run("indexMerge");
}
}} // namespace indexlib::merger
