#ifndef ISEARCH_BS_MERGEINSTANCEWORKITEM_H
#define ISEARCH_BS_MERGEINSTANCEWORKITEM_H

#include <autil/WorkItem.h>
#include "build_service/task_base/MergeTask.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service {
namespace local_job {

class MergeInstanceWorkItem : public autil::WorkItem {
public:
    MergeInstanceWorkItem(volatile bool *failflag,
                          task_base::TaskBase::Mode mode,
                          uint16_t instanceId,
                          const std::string &jobParams)
        : _failflag(failflag)
        , _mode(mode)
        , _instanceId(instanceId)
        , _jobParams(jobParams)
    {
    }
    ~MergeInstanceWorkItem() {
    }

private:
    MergeInstanceWorkItem(const MergeInstanceWorkItem &);
    MergeInstanceWorkItem& operator=(const MergeInstanceWorkItem &);

public:
    /* override */ void process() {
        if (!_mergeTask.run(_jobParams, _instanceId, _mode, true)) {
            std::string errorMsg = "run [" + task_base::TaskBase::getModeStr(_mode)
                                   + "] instance[" + autil::StringUtil::toString(_instanceId) + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            *_failflag = true;
        }
    }

    /* override */ void destroy() {
        delete this;
    }
    /* override */ void drop() {
        destroy();
    }
private:
    volatile bool *_failflag;
    const task_base::TaskBase::Mode _mode;
    const uint16_t _instanceId;
    const std::string _jobParams;
    task_base::MergeTask _mergeTask;
private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(local_job, MergeInstanceWorkItem);
    
}
}

#endif //ISEARCH_BS_MERGEINSTANCEWORKITEM_H
