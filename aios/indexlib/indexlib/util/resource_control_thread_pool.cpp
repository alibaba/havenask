#include <autil/Thread.h>
#include "indexlib/util/resource_control_thread_pool.h"
#include <unistd.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ResourceControlThreadPool);


ResourceControlThreadPool::ResourceControlThreadPool(
        uint32_t threadNum, int64_t resourceLimit) 
    : mThreadNum(threadNum)
    , mLeftResource(resourceLimit)
    , mHasException(false)
{
}

ResourceControlThreadPool::~ResourceControlThreadPool() 
{
}

void ResourceControlThreadPool::Init(
    const std::vector<ResourceControlWorkItemPtr>& workItems,
    const std::function<void()>& onThreadNormalExit)
{
    mOnThreadNormalExit = onThreadNormalExit;
    mWorkItems.clear();
    for (size_t i = 0; i < workItems.size(); i++)
    {
        if (!workItems[i])
        {
            IE_LOG(ERROR, "workItem empty, dropped");
            continue;
        }

        if (mLeftResource < workItems[i]->GetRequiredResource())
        {
            INDEXLIB_FATAL_ERROR(OutOfRange, "max resource [%ld], require [%ld]",
                    mLeftResource, workItems[i]->GetRequiredResource());
        }
        mWorkItems.push_back(workItems[i]);
    }
}

void ResourceControlThreadPool::Run(const std::string& name)
{
    vector<ThreadPtr> threads(mThreadNum);
    for (uint32_t i = 0; i < mThreadNum; i++)
    {
        ThreadPtr tp = Thread::createThread(
                std::tr1::bind(&ResourceControlThreadPool::WorkLoop, this), name);
        threads[i] = tp;
    }

    for (uint32_t i = 0; i < mThreadNum; i++)
    {
        threads[i]->join();
    }

    CleanWorkItems();
    
    if (mHasException)
    {
        throw mException;
    }
}

void ResourceControlThreadPool::CleanWorkItems()
{
    for (size_t i = 0; i < mWorkItems.size(); i++)
    {
        mWorkItems[i]->Destroy();
    }
    mWorkItems.clear();
}

void ResourceControlThreadPool::WorkLoop()
{
    while (!mHasException)
    {
        ResourceControlWorkItemPtr workItem;
        try
        {
            if (!PopWorkItem(workItem))
            {
                if (mOnThreadNormalExit)
                {
                    mOnThreadNormalExit();
                }
                break;
            }
            ConsumWorkItem(workItem);
            if (workItem)
            {
                workItem->Destroy();
            }
        }
        catch(const misc::ExceptionBase& e)
        {
            IE_LOG(ERROR, "thread exception:[%s].", e.what());
            if (workItem)
            {
                try
                {
                    workItem->Destroy();
                }
                catch(...)
                {
                    IE_LOG(ERROR, "work item destroy exception ignore");
                }
            }
            {
                ScopedLock lock(mMutex);
                mException = e;
                mHasException = true;
            }
        }
        catch(...)
        {
            IE_LOG(ERROR, "thread unknown exception");
            if (workItem)
            {
                try
                {
                    workItem->Destroy();
                }
                catch(...)
                {
                    IE_LOG(ERROR, "work item destroy exception ignore");
                }
            }
            throw;
        }
    }
}

void ResourceControlThreadPool::ConsumWorkItem(
        const ResourceControlWorkItemPtr& workItem)
{
    if (workItem)
    {
        workItem->Process();
        {
            ScopedLock lock(mMutex);
            mLeftResource += workItem->GetRequiredResource();
        }
    }
    else
    {
        //do nothing wait resource enough
        usleep(1000); // 1ms
    }
}

bool ResourceControlThreadPool::PopWorkItem(ResourceControlWorkItemPtr& workItem)
{
    ScopedLock lock(mMutex);
    if (mWorkItems.empty())
    {
        return false;
    }
    for (vector<ResourceControlWorkItemPtr>::iterator it = mWorkItems.begin();
         it != mWorkItems.end();)
    {
        int64_t resourceUse = (*it)->GetRequiredResource();
        if (mLeftResource >= resourceUse)
        {
            workItem = *it;
            mLeftResource -= resourceUse;
            it = mWorkItems.erase(it);
            break;
        }
        else 
        {
            it++;
        }
    }
    return true;
}

IE_NAMESPACE_END(util);

