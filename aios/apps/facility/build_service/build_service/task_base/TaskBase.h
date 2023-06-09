/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_BS_TASKBASE_H
#define ISEARCH_BS_TASKBASE_H

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor_adapter/MonitorFactory.h"

namespace build_service { namespace task_base {

class TaskBase
{
public:
    enum Mode { BUILD_MAP, BUILD_REDUCE, MERGE_MAP, MERGE_REDUCE, END_MERGE_MAP, END_MERGE_REDUCE };

public:
    TaskBase(const std::string& epochId = "");
    TaskBase(indexlib::util::MetricProviderPtr metricProvider, const std::string& epochId = "");
    virtual ~TaskBase();

private:
    TaskBase(const TaskBase&);
    TaskBase& operator=(const TaskBase&);

public:
    virtual bool init(const std::string& jobParam);
    // also use for localJobMode
    virtual void cleanUselessResource() {}

public:
    static std::string getModeStr(Mode mode);

protected:
    proto::PartitionId createPartitionId(uint32_t instanceId, Mode mode, const std::string& role);
    bool startMonitor(const proto::Range& range, Mode mode);

    virtual bool getIndexPartitionOptions(indexlib::config::IndexPartitionOptions& options);

    bool getIncBuildParallelNum(uint32_t& parallelNum) const;
    bool getWorkerPathVersion(int32_t& workerPathVersion) const;
    bool getBuildStep(proto::BuildStep& buildStep) const;

private:
    bool initKeyValueMap(const std::string& jobParam, KeyValueMap& kvMap);
    proto::Range createRange(uint32_t instanceId, Mode mode);
    bool addDataDescription(const std::string& dataTable, KeyValueMap& kvMap);
    bool initDocReclaimConfig(indexlib::config::IndexPartitionOptions& options, const std::string& mergeConfigName,
                              const std::string& clusterName);

protected:
    KeyValueMap _kvMap;
    JobConfig _jobConfig;
    std::string _dataTable;
    config::ResourceReaderPtr _resourceReader;
    indexlib::util::MetricProviderPtr _metricProvider;
    kmonitor::MetricsReporterPtr _reporter;
    kmonitor_adapter::Monitor* _monitor;
    indexlib::config::IndexPartitionOptions _mergeIndexPartitionOptions;
    std::string _epochId;

private:
    friend class MergeTaskTest;
    BS_LOG_DECLARE();
};

}} // namespace build_service::task_base

#endif // ISEARCH_BS_TASKBASE_H
