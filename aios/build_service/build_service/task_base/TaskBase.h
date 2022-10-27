#ifndef ISEARCH_BS_TASKBASE_H
#define ISEARCH_BS_TASKBASE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/config/ResourceReader.h"
#include <indexlib/misc/metric_provider.h>
#include <indexlib/config/index_partition_options.h>
#include <kmonitor_adapter/MonitorFactory.h>
#include <kmonitor/client/MetricsReporter.h>

namespace build_service {
namespace task_base {

class TaskBase
{
public:
    enum Mode {
        BUILD_MAP,
        BUILD_REDUCE,
        MERGE_MAP,
        MERGE_REDUCE,
        END_MERGE_MAP,
        END_MERGE_REDUCE
    };
public:
    TaskBase();
    TaskBase(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    virtual ~TaskBase();
private:
    TaskBase(const TaskBase &);
    TaskBase& operator=(const TaskBase &);
public:
    virtual bool init(const std::string &jobParam);
public:
    static std::string getModeStr(Mode mode);
protected:
    proto::PartitionId createPartitionId(uint32_t instanceId, Mode mode, 
            const std::string &role);
    bool startMonitor(const proto::Range &range, Mode mode);

    virtual bool getIndexPartitionOptions(
            IE_NAMESPACE(config)::IndexPartitionOptions &options);

    bool getIncBuildParallelNum(uint32_t& parallelNum) const;
    bool getWorkerPathVersion(int32_t& workerPathVersion) const;
    bool getBuildStep(proto::BuildStep &buildStep) const;

private:
    bool initKeyValueMap(const std::string &jobParam,
                         KeyValueMap &kvMap);
    proto::Range createRange(uint32_t instanceId, Mode mode);
    bool addDataDescription(const std::string &dataTable, KeyValueMap &kvMap);
    bool initDocReclaimConfig(IE_NAMESPACE(config)::IndexPartitionOptions &options,
                              const std::string& mergeConfigName, 
                              const std::string& clusterName);    

protected:
    KeyValueMap _kvMap;
    JobConfig _jobConfig;
    std::string _dataTable;
    config::ResourceReaderPtr _resourceReader;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;
    kmonitor::MetricsReporterPtr _reporter;
    kmonitor_adapter::Monitor* _monitor;
    IE_NAMESPACE(config)::IndexPartitionOptions _mergeIndexPartitionOptions;
private:
    friend class MergeTaskTest;
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_TASKBASE_H
