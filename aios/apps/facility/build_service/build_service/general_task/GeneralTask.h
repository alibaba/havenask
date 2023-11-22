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
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/LoopThread.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "future_lite/Executor.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/CustomIndexTaskClassInfo.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/LocalExecuteEngine.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace build_service::task_base {

class GeneralTask : public Task
{
public:
    GeneralTask() = default;
    ~GeneralTask();

public:
    static const std::string TASK_NAME;
    static const std::string CHECKPOINT_NAME;
    static const std::string DEFAULT_GLOBAL_BLOCK_CACHE_PARAM_TEMPLATE;

public:
    class GeneralTaskMetrics : public indexlibv2::framework::IMetrics
    {
    public:
        GeneralTaskMetrics(const std::shared_ptr<kmonitor::MetricsTags>& metricsTags,
                           const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
        ~GeneralTaskMetrics();

    public:
        void ReportMetrics() override;
        void RegisterMetrics() override;
        void UpdateOperationCount(int64_t count) { _operationCount = count; }

    private:
        std::shared_ptr<kmonitor::MetricsTags> _metricsTags;
        std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
        std::shared_ptr<indexlib::util::Metric> _reportMetric;
        volatile int64_t _operationCount;
    };

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }
    static std::vector<std::shared_ptr<indexlibv2::framework::IndexTaskResource>>
    prepareExtendResource(const std::shared_ptr<config::ResourceReader>& resourceReader,
                          const std::string& clusterName);

public:
    size_t TEST_getPendingAndRunningOpSize() const;

private:
    struct OperationDetail {
        indexlibv2::framework::IndexOperationDescription desc;
        std::vector<std::string> dependOpExecuteEpochIds;
        proto::OperationResult result;
        bool isRemoved = false;
    };
    using OpDetailMap = std::map<indexlibv2::framework::IndexOperationId, std::shared_ptr<OperationDetail>>;
    using OpPair = std::pair<indexlibv2::framework::IndexOperationId, std::shared_ptr<OperationDetail>>;

private:
    static void unsafeUpdateOpDetail(const std::vector<std::shared_ptr<OperationDetail>>& toRuns,
                                     const std::vector<std::shared_ptr<OperationDetail>>& toRemoves,
                                     OpDetailMap* opDetails);

    static std::pair</*toRun=*/std::vector<std::shared_ptr<OperationDetail>>,
                     /*toRemove=*/std::vector<std::shared_ptr<OperationDetail>>>
    generateTodoOps(std::vector<std::shared_ptr<OperationDetail>> newDetails,
                    std::vector<std::shared_ptr<OperationDetail>> oldDetails);

    void initExecuteEpochId();
    void initGeneralTaskMetrics();
    bool prepare(const std::string taskName, const std::string taskType, const config::TaskTarget& target);
    bool initIndexTaskContextCreator(const std::string taskName, const std::string taskType,
                                     const config::TaskTarget& target);
    bool initEngine();
    std::vector<std::shared_ptr<OperationDetail>> getOperationDetails(const proto::OperationTarget& opTarget) const;
    void updateCurrent();
    void workLoop();
    bool startWorkLoop();
    bool hasRunningOps() const;
    std::unique_ptr<indexlibv2::framework::IIndexOperationCreator>
    getCustomOperationCreator(const indexlibv2::config::CustomIndexTaskClassInfo& customClassInfo,
                              const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);
    std::unique_ptr<indexlibv2::framework::IndexTaskContext> createIndexTaskContext(const OperationDetail& detail);
    // virtual for mock
    virtual std::unique_ptr<indexlibv2::framework::IndexTaskContext> createIndexTaskContext();
    bool GetIndexPath(const config::TaskTarget& target, std::string& indexPath);
    indexlib::Status addExtendResource(const indexlibv2::framework::IndexTaskContext* context) const;

private:
    Task::TaskInitParam _initParam;
    std::string _executeEpochId;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    std::shared_ptr<indexlibv2::config::TabletOptions> _tabletOptions;
    std::shared_ptr<indexlibv2::framework::MetricsManager> _metricsManager;
    std::unique_ptr<indexlibv2::framework::ITabletFactory> _tabletFactory;
    std::unique_ptr<indexlibv2::framework::IndexTaskContextCreator> _contextCreator;
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<indexlibv2::framework::LocalExecuteEngine> _engine;
    std::shared_ptr<GeneralTaskMetrics> _generalTaskMetrics;
    std::atomic<int64_t> _availableMemory = 0;

    autil::LoopThreadPtr _workLoopThread;

    mutable std::mutex _opDetailsMutex;
    OpDetailMap _opDetails;
    uint32_t _threadNum = 2;
    std::mutex _currentMutex;
    proto::OperationCurrent _current;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::task_base
;
