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
#include "indexlib/util/resource_control_thread_pool.h"

#include <unistd.h>

#include "autil/Thread.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
IE_LOG_SETUP(util, ResourceControlThreadPool);

ResourceControlThreadPool::ResourceControlThreadPool(uint32_t threadNum, int64_t resourceLimit)
    : mThreadNum(threadNum)
    , mLeftResource(resourceLimit)
    , mHasException(false)
{
}

ResourceControlThreadPool::~ResourceControlThreadPool() {}

void ResourceControlThreadPool::Init(const std::vector<ResourceControlWorkItemPtr>& workItems,
                                     const std::function<void()>& onThreadNormalExit)
{
    mOnThreadNormalExit = onThreadNormalExit;
    mWorkItems.clear();
    for (size_t i = 0; i < workItems.size(); i++) {
        if (!workItems[i]) {
            IE_LOG(ERROR, "workItem empty, dropped");
            continue;
        }

        if (mLeftResource < workItems[i]->GetRequiredResource()) {
            INDEXLIB_FATAL_ERROR(OutOfRange, "max resource [%ld], require [%ld]", mLeftResource,
                                 workItems[i]->GetRequiredResource());
        }
        mWorkItems.push_back(workItems[i]);
    }
}

void ResourceControlThreadPool::Run(const std::string& name)
{
    vector<ThreadPtr> threads(mThreadNum);
    for (uint32_t i = 0; i < mThreadNum; i++) {
        ThreadPtr tp = autil::Thread::createThread(std::bind(&ResourceControlThreadPool::WorkLoop, this), name);
        if (!tp) {
            INDEXLIB_FATAL_ERROR(InitializeFailed, "create thread failed");
        }
        threads[i] = tp;
    }

    for (uint32_t i = 0; i < mThreadNum; i++) {
        threads[i]->join();
    }

    CleanWorkItems();

    if (mHasException) {
        throw mException;
    }
}

void ResourceControlThreadPool::CleanWorkItems()
{
    for (size_t i = 0; i < mWorkItems.size(); i++) {
        mWorkItems[i]->Destroy();
    }
    mWorkItems.clear();
}

void ResourceControlThreadPool::WorkLoop()
{
    while (!mHasException) {
        ResourceControlWorkItemPtr workItem;
        try {
            if (!PopWorkItem(workItem)) {
                if (mOnThreadNormalExit) {
                    mOnThreadNormalExit();
                }
                break;
            }
            ConsumWorkItem(workItem);
            if (workItem) {
                workItem->Destroy();
            }
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR, "thread exception:[%s].", e.what());
            if (workItem) {
                try {
                    workItem->Destroy();
                } catch (...) {
                    IE_LOG(ERROR, "work item destroy exception ignore");
                }
            }
            {
                ScopedLock lock(mMutex);
                mException = e;
                mHasException = true;
            }
        } catch (...) {
            IE_LOG(ERROR, "thread unknown exception");
            if (workItem) {
                try {
                    workItem->Destroy();
                } catch (...) {
                    IE_LOG(ERROR, "work item destroy exception ignore");
                }
            }
            throw;
        }
    }
}

void ResourceControlThreadPool::ConsumWorkItem(const ResourceControlWorkItemPtr& workItem)
{
    if (workItem) {
        workItem->Process();
        {
            ScopedLock lock(mMutex);
            mLeftResource += workItem->GetRequiredResource();
        }
    } else {
        // do nothing wait resource enough
        usleep(1000); // 1ms
    }
}

bool ResourceControlThreadPool::PopWorkItem(ResourceControlWorkItemPtr& workItem)
{
    ScopedLock lock(mMutex);
    if (mWorkItems.empty()) {
        return false;
    }
    for (vector<ResourceControlWorkItemPtr>::iterator it = mWorkItems.begin(); it != mWorkItems.end();) {
        int64_t resourceUse = (*it)->GetRequiredResource();
        if (mLeftResource >= resourceUse) {
            workItem = *it;
            mLeftResource -= resourceUse;
            it = mWorkItems.erase(it);
            break;
        } else {
            it++;
        }
    }
    return true;
}
}} // namespace indexlib::util
