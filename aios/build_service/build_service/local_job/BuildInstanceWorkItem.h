#ifndef ISEARCH_BS_BUILDINSTANCEWORKITEM_H
#define ISEARCH_BS_BUILDINSTANCEWORKITEM_H

#include <autil/WorkItem.h>
#include <autil/StringUtil.h>
#include "build_service/task_base/BuildTask.h"
#include "build_service/local_job/LocalBrokerFactory.h"

namespace build_service {
namespace local_job {

class BuildInstanceWorkItem : public autil::WorkItem {
public:
    BuildInstanceWorkItem(volatile bool *failflag,
                          task_base::TaskBase::Mode mode,
                          uint16_t instanceId,
                          const std::string &jobParams,
                          ReduceDocumentQueue* queue)
        : _failflag(failflag)
        , _mode(mode)
        , _instanceId(instanceId)
        , _jobParams(jobParams)
        , _localBrokerFactory(queue)
    {
    }
    ~BuildInstanceWorkItem() {
    }

private:
    BuildInstanceWorkItem(const BuildInstanceWorkItem &);
    BuildInstanceWorkItem& operator=(const BuildInstanceWorkItem &);

public:
    /* override */ void process() {
        if (!_buildTask.run(_jobParams, &_localBrokerFactory, _instanceId, _mode)) {
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
    LocalBrokerFactory _localBrokerFactory;
    task_base::BuildTask _buildTask;

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(local_job, BuildInstanceWorkItem);
    
}
}

#endif //ISEARCH_BS_BUILDINSTANCEWORKITEM_H
