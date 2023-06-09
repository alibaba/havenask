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
#ifndef __INDEXLIB_MERGE_SCHEDULER_H
#define __INDEXLIB_MERGE_SCHEDULER_H

#include <memory>

#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/resource_control_work_item.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

namespace indexlib { namespace merger {

class MergeScheduler
{
public:
    MergeScheduler() {}
    virtual ~MergeScheduler() {}

public:
    virtual void Run(const std::vector<util::ResourceControlWorkItemPtr>& workItems,
                     const merger::MergeFileSystemPtr& mergeFileSystem) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeScheduler);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_SCHEDULER_H
