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
#ifndef __INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H
#define __INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_scheduler.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

namespace indexlib { namespace merger {

class MultiThreadedMergeScheduler : public MergeScheduler
{
public:
    MultiThreadedMergeScheduler(int64_t maxMemUse, uint32_t threadNum);
    ~MultiThreadedMergeScheduler();

public:
    void Run(const std::vector<util::ResourceControlWorkItemPtr>& workItems,
             const merger::MergeFileSystemPtr& mergeFileSystem) override;
    size_t GetThreadNum() const { return mThreadNum; }

private:
    int64_t mMaxMemUse;
    uint32_t mThreadNum;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiThreadedMergeScheduler);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H
