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
#ifndef __INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H
#define __INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H

#include <functional>
#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/resource_control_work_item.h"

namespace indexlib { namespace util {

class ResourceControlThreadPool
{
public:
    ResourceControlThreadPool(uint32_t threadNum, int64_t resourceLimit);
    ~ResourceControlThreadPool();

public:
    void Init(const std::vector<ResourceControlWorkItemPtr>& workItems,
              const std::function<void()>& onThreadNormalExit = nullptr);
    void Run(const std::string& name);
    size_t GetThreadNum() const { return mThreadNum; }

private:
    void ConsumWorkItem(const ResourceControlWorkItemPtr& workItem);
    bool PopWorkItem(ResourceControlWorkItemPtr& workItem);
    void WorkLoop();
    void CleanWorkItems();

private:
    uint32_t mThreadNum;
    int64_t mLeftResource;
    std::vector<ResourceControlWorkItemPtr> mWorkItems;
    std::function<void()> mOnThreadNormalExit;
    mutable autil::ThreadMutex mMutex;
    util::ExceptionBase mException;
    volatile bool mHasException;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ResourceControlThreadPool);
}} // namespace indexlib::util

#endif //__INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H
