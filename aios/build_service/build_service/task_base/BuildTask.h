#ifndef ISEARCH_BS_BUILDTASKBASE_H
#define ISEARCH_BS_BUILDTASKBASE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/task_base/TaskBase.h"

namespace build_service {
namespace task_base {

class BuildTask : public TaskBase
{
public:
    BuildTask();
    virtual ~BuildTask();
private:
    BuildTask(const BuildTask &);
    BuildTask& operator=(const BuildTask &);
public:
    bool run(const std::string &jobParam,
             workflow::BrokerFactory *brokerFactory,
             int32_t instanceId, Mode mode);
    static bool buildRawFileIndex(
            const config::ResourceReaderPtr &resourceReader,
            const KeyValueMap &kvMap,
            const proto::PartitionId &partitionId);
private:
    bool startBuildFlow(const proto::PartitionId &partitionId,
                        workflow::BrokerFactory *brokerFactory,
                        Mode mode);
    static bool atomicCopy(const std::string &src, const std::string &file);
    static workflow::BuildFlow::BuildFlowMode getBuildFlowMode(
            bool needPartition, Mode mode);
private:
    virtual workflow::BuildFlow *createBuildFlow() const;
    bool resetResourceReader(const proto::PartitionId &partitionId);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildTask);

}
}

#endif //ISEARCH_BS_BUILDTASKBASE_H
