#ifndef __INDEXLIB_MERGE_TASK_DISPATCHER_H
#define __INDEXLIB_MERGE_TASK_DISPATCHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/task_execute_meta.h"

IE_NAMESPACE_BEGIN(table);

class MergeTaskDispatcher
{
public:
    MergeTaskDispatcher();
    ~MergeTaskDispatcher();
public:
    virtual std::vector<TaskExecuteMetas> DispatchMergeTasks(
        const std::vector<MergeTaskDescriptions>& taskDescriptions,
        uint32_t instanceCount);

private:
    class WorkLoadCounter
    {
    public:
        WorkLoadCounter(TaskExecuteMetas& _taskMetas, uint64_t _workLoad)
            : taskMetas(_taskMetas)
            , workLoad(_workLoad)
        {}
        ~WorkLoadCounter() {}
    public:
        TaskExecuteMetas& taskMetas;
        uint64_t workLoad;
    };
    typedef std::shared_ptr<WorkLoadCounter> WorkLoadCounterPtr;
    class WorkLoadComp
    {
    public:
        bool operator() (const WorkLoadCounterPtr& lhs,
                         const WorkLoadCounterPtr& rhs)
        {
            if (lhs->workLoad == rhs->workLoad)
            {
                return lhs->taskMetas.size() > rhs->taskMetas.size();
            }
            return lhs->workLoad > rhs->workLoad;
        }
    };
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskDispatcher);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_MERGE_TASK_DISPATCHER_H
