#ifndef __INDEXLIB_MERGE_SCHEDULER_H
#define __INDEXLIB_MERGE_SCHEDULER_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/resource_control_work_item.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

IE_NAMESPACE_BEGIN(merger);

class MergeScheduler
{
public:
    MergeScheduler() {}
    virtual ~MergeScheduler() {}

public:
    virtual void Run(const std::vector<util::ResourceControlWorkItemPtr> &workItems,
                     const merger::MergeFileSystemPtr& mergeFileSystem) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeScheduler);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_SCHEDULER_H
