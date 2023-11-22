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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "build_service/common/CpuRatioSampler.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/ProcessorWorkflowErrorSampler.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace build_service { namespace worker {

class ProcessorServiceImpl : public WorkerStateHandler
{
private:
    enum CommandType { CT_START, CT_UPDATE_CONFIG, CT_STOP, CT_SUSPEND, CT_NONE };

public:
    ProcessorServiceImpl(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                         const proto::LongAddress& address, const std::string& appZkRoot,
                         const std::string& adminServiceName);
    virtual ~ProcessorServiceImpl();

private:
    ProcessorServiceImpl(const ProcessorServiceImpl&);
    ProcessorServiceImpl& operator=(const ProcessorServiceImpl&);

public:
    bool init() override;
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override;
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override;
    void doGetResourceKeeperMap(common::ResourceKeeperMap& usingKeepers) override;

private:
    CommandType getCommandType(const proto::ProcessorTarget& target, const proto::ProcessorCurrent& current) const;

private:
    bool startProcessor(const proto::ProcessorTarget& target);
    bool updateConfig(const std::string& configPath);
    void stopProcessor(const proto::ProcessorTarget& target);
    void suspendProcessor(const proto::ProcessorTarget& target);
    void fillSuspendStatus(workflow::BuildFlow* buildFlow, proto::ProcessorCurrent& current);
    void fillProgressStatus(workflow::BuildFlow* buildFlow, proto::ProcessorCurrent& current);
    void fillCpuRatio(proto::ProcessorCurrent& current);
    void fillWorkflowError(workflow::BuildFlow* buildFlow, proto::ProcessorCurrent& current);
    bool fillInput(const std::string dataDescriptionStr, const proto::PartitionId& pid, KeyValueMap& kvMap);
    // virtual for test
    virtual void updateResourceKeeperMap(bool& needRestart);
    bool parseTargetDesc(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                         const proto::ProcessorTarget& target, proto::PartitionId& newPid, KeyValueMap& output);

    bool getTargetClusterNames(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                               std::vector<std::string>& clusterNames);

    bool checkSingleUpdatableClusterName(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                                         const std::string& clusterName);

    bool getTargetClustersForDefaultProcessorGroup(const config::ResourceReaderPtr& resourceReader,
                                                   const proto::PartitionId& pid,
                                                   std::vector<std::string>& clusterNames);
    bool getDataDescription(const std::string& dataDescriptionStr, proto::DataDescription& description);
    bool getStopTimestamp(const proto::ProcessorTarget& target, const proto::DataDescription& dataDescription,
                          int64_t& stopTimestamp);

private:
    // virtual for test
    virtual bool startProcessFlow(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                                  KeyValueMap& kvMap);
    // virtual for test
    virtual workflow::BuildFlow* createBuildFlow(const config::ResourceReaderPtr& resourceReader);
    // virtual for test
    virtual bool loadGenerationMeta(const config::ResourceReaderPtr resourceReader, const std::string& indexRoot);
    bool restartProcessor(const proto::ProcessorTarget& target);

    std::string getWorkerConfigPath(const proto::ProcessorTarget& target, const proto::PartitionId& pid);
    common::ResourceKeeperMap createResources(const config::ResourceReaderPtr& resourceReader,
                                              const proto::PartitionId& pid, KeyValueMap& kvMap);
    bool startSamplersForAdaptiveScaling(const config::ProcessorAdaptiveScalingConfig& adaptiveScalingConfig);

private:
    proto::ProcessorCurrent _current;
    workflow::BuildFlow* _buildFlow;
    workflow::FlowFactory* _brokerFactory;
    int64_t _startTimestamp;
    int64_t _baseDocCount;
    indexlib::util::AccumulativeCounterPtr _consumedDocCountCounter;
    mutable autil::RecursiveThreadMutex _lock; // attention lock code should use less time
    bool _isTablet = false;
    std::shared_ptr<common::CpuRatioSampler> _cpuRatioSampler;
    std::shared_ptr<workflow::ProcessorWorkflowErrorSampler> _workflowErrorSampler;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorServiceImpl);

}} // namespace build_service::worker
