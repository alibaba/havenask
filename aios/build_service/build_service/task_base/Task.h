#ifndef ISEARCH_BS_TASK_H
#define ISEARCH_BS_TASK_H

#include <indexlib/util/counter/counter_map.h>
#include <indexlib/misc/metric_provider.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/io/InputCreator.h"
#include "build_service/io/OutputCreator.h"

namespace build_service {
namespace task_base {

using InputCreatorMap = std::unordered_map<std::string, io::InputCreatorPtr>;
using OutputCreatorMap = std::unordered_map<std::string, io::OutputCreatorPtr>;

class Task : public proto::ErrorCollector
{
public:
    Task();
    virtual ~Task();
    
private:
    Task(const Task &);
    Task& operator=(const Task &);
    
public:
    struct InstanceInfo {
        InstanceInfo()
            : partitionCount(0)
            , partitionId(0)
            , instanceCount(0)
            , instanceId(0)
        {}
        uint32_t partitionCount;
        uint32_t partitionId;
        uint32_t instanceCount;
        uint32_t instanceId;
    };

    struct BuildId {
        std::string appName;
        std::string dataTable;
        generationid_t generationId;
    };
    
    struct TaskInitParam {
        TaskInitParam()
            : resourceReader(NULL)
            , metricProvider(NULL)
        {}
        InstanceInfo  instanceInfo;
        BuildId buildId;
        proto::PartitionId pid;
        std::string clusterName;
        config::ResourceReader *resourceReader;
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider;
        InputCreatorMap inputCreators;
        OutputCreatorMap outputCreators;
    };

public:
    virtual bool init(TaskInitParam& initParam) = 0;
    virtual bool handleTarget(const config::TaskTarget& target) = 0;
    virtual bool isDone(const config::TaskTarget& target) = 0;
    virtual IE_NAMESPACE(util)::CounterMapPtr getCounterMap() = 0;
    virtual std::string getTaskStatus() = 0;

    virtual bool hasFatalError() = 0;
    virtual void handleFatalError() {
        handleFatalError("fatal error exit");
    }
protected:
    void handleFatalError(const std::string& fatalErrorMsg);
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Task);
////////////////////////////////////////////////
}
}

#endif //ISEARCH_BS_TASK_H
