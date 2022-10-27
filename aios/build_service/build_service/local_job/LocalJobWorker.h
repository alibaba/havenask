#ifndef ISEARCH_BS_LOCALJOBWORKER_H
#define ISEARCH_BS_LOCALJOBWORKER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/TaskBase.h"

namespace build_service {

namespace local_job {

class LocalBrokerFactory;

class LocalJobWorker
{
public:
    LocalJobWorker();
    ~LocalJobWorker();
private:
    LocalJobWorker(const LocalJobWorker &);
    LocalJobWorker& operator=(const LocalJobWorker &);
public:
    bool run(const std::string &step, uint16_t mapCount,
             uint16_t reduceCount, const std::string &jobParams);
private:
    bool buildJob(uint16_t mapCount, uint16_t reduceCount, const std::string &jobParams);
    bool mergeJob(uint16_t mapCount, uint16_t reduceCount, const std::string &jobParams);
    bool endMergeJob(uint16_t mapCount, uint16_t reduceCount, const std::string &jobParams);
    bool parallelRunMergeTask(size_t instanceCount,
                              const std::string &jobParams,
                              task_base::TaskBase::Mode mode);
    bool downloadConfig(const std::string &jobParams);
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_LOCALJOBWORKER_H
