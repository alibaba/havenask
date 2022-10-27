#ifndef __INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H
#define __INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_scheduler.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

IE_NAMESPACE_BEGIN(merger);

class MultiThreadedMergeScheduler : public MergeScheduler
{
public:
    MultiThreadedMergeScheduler(int64_t maxMemUse, uint32_t threadNum);
    ~MultiThreadedMergeScheduler();

public:
    void Run(const std::vector<util::ResourceControlWorkItemPtr> &workItems,
             const merger::MergeFileSystemPtr& mergeFileSystem) override;
    size_t GetThreadNum() const { return mThreadNum; }
private:
    int64_t mMaxMemUse;
    uint32_t mThreadNum;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiThreadedMergeScheduler);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MULTI_THREADED_MERGE_SCHEDULER_H
