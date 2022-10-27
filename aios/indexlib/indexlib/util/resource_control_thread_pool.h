#ifndef __INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H
#define __INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <functional>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/resource_control_work_item.h"

IE_NAMESPACE_BEGIN(util);

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
    misc::ExceptionBase mException;
    volatile bool mHasException;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ResourceControlThreadPool);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_RESOURCE_CONTROL_THREAD_POOL_H
