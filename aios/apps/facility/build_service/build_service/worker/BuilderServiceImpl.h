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
#ifndef ISEARCH_BS_BUILDERSERVICEIMPL_H
#define ISEARCH_BS_BUILDERSERVICEIMPL_H

#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/FlowFactory.h"
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

namespace build_service { namespace worker {

class ServiceWorker;

class BuilderServiceImpl : public WorkerStateHandler
{
private:
    enum CommandType { CT_START_BUILD, CT_STOP_BUILD, CT_NONE };

public:
    BuilderServiceImpl(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                       const proto::LongAddress& address, const std::string& appZkRoot = "",
                       const std::string& adminServiceName = "", const std::string& epochId = "");
    ~BuilderServiceImpl();

private:
    BuilderServiceImpl(const BuilderServiceImpl&);
    BuilderServiceImpl& operator=(const BuilderServiceImpl&);

public:
    bool init() override;
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override;
    bool needRestartProcess(const std::string& state) override;
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override;

private:
    CommandType getCommandType(const proto::BuilderTarget& target, const proto::BuilderCurrent& current) const;
    bool prepareIntervalTask(const config::ResourceReaderPtr& resourceReader, workflow::BuildFlowMode mode,
                             workflow::BuildFlow* buildFlow, const proto::PartitionId& pid);
    void fillProgressStatus(workflow::BuildFlow* buildFlow, proto::BuilderCurrent& current);
    bool getBuildVersionTimestamp(const proto::BuilderTarget& target, int64_t& versionStopTimestamp);

private:
    // command
    bool startBuild(const proto::BuilderTarget& target);
    void stopBuild(const proto::BuilderTarget& target);
    bool checkWorkerPathVersion(const proto::BuilderTarget& target);
    bool checkUpdateConfig(const proto::BuilderTarget& target);
    bool parseTargetDescription(const proto::BuilderTarget& target, KeyValueMap& kvMap);
    bool parseTargetDescriptionCompatibleWithBs_1_8(const proto::BuilderTarget& target, KeyValueMap& kvMap);
    bool parseReservedVersions(const proto::BuilderTarget& target, KeyValueMap& kvMap);

private:
    virtual void startBuildFlow(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                                KeyValueMap& kvMap);
    virtual workflow::FlowFactory* createFlowFactory(const config::ResourceReaderPtr& resourceReader,
                                                     const proto::PartitionId& pid, KeyValueMap& kvMap);
    virtual common::ResourceKeeperMap createResources(const config::ResourceReaderPtr& resourceReader,
                                                      const proto::PartitionId& pid, KeyValueMap& kvMap);
    virtual workflow::BuildFlow* createBuildFlow();

private:
    std::string getAmonitorNodePath() const;

private:
    static const int64_t UPDATE_LOCATOR_INTERVAL = 2 * 1000 * 1000; // 2s
private:
    proto::BuilderCurrent _current;
    workflow::BuildFlow* _buildFlow;
    workflow::FlowFactory* _brokerFactory;
    config::CounterConfigPtr _counterConfig;
    common::CounterSynchronizerPtr _counterSynchronizer;
    indexlib::util::TaskSchedulerPtr _taskScheduler;
    int32_t _updateLocatorTaskId;
    int64_t _startTimestamp;
    int64_t _baseDocCount;
    volatile int32_t _workerPathVersion;
    indexlib::util::AccumulativeCounterPtr _totalDocCountCounter;
    mutable autil::RecursiveThreadMutex _lock; // attention lock code should use less time
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderServiceImpl);

}} // namespace build_service::worker

#endif // ISEARCH_BS_BUILDERSERVICEIMPL_H
