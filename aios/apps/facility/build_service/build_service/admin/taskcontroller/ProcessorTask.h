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

#include <map>
#include <memory>
#include <optional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/FatalErrorDiscoverer.h"
#include "build_service/admin/ProcessorCheckpointAccessor.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/ProcessorAdaptiveScaling.h"
#include "build_service/admin/taskcontroller/ProcessorInput.h"
#include "build_service/admin/taskcontroller/ProcessorParamParser.h"
#include "build_service/admin/taskcontroller/ProcessorTargetInfos.h"
#include "build_service/admin/taskcontroller/ProcessorWriterVersion.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorRuleConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SlowNodeDetectConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {
class ProcessorNodesUpdater;

BS_TYPEDEF_PTR(ProcessorNodesUpdater);

class ProcessorTask : public AdminTaskBase
{
public:
    ProcessorTask(const proto::BuildId& buildId, proto::BuildStep step, const TaskResourceManagerPtr& resMgr);
    ~ProcessorTask();

private:
    ProcessorTask& operator=(const ProcessorTask&);

public:
    enum ConfigUpdateType { CONFIG_ILLEGAL, CONFIG_CHANGE_GROUP, NORMAL_UPDATE_CONFIG };

    bool init(const KeyValueMap& kvMap);
    bool init(const ProcessorInput& input, const common::ProcessorOutput& output,
              const std::vector<std::string>& clusterNames, const ProcessorConfigInfo& configInfo, bool isTablet);
    bool isBatchMode() { return _batchMode; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void notifyStopped() override;
    bool run(proto::WorkerNodes& workerNodes) override;
    bool run(proto::ProcessorNodes& processorNodes);
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    void startFromLocator(const common::Locator& locator);
    bool switchBuild();
    void clearFullWorkerZkNode(const std::string& generationDir) const;
    bool updateConfig() override;
    bool isTablet() const;
    void clearCounterAndIncZkNode(const std::string& generationDir);

public:
    common::Locator getLocator() const;
    ProcessorInput getInput()
    {
        // todo direct return input
        ProcessorInput input = _input;
        input.src = -1;
        input.offset = -1;
        return input;
    }
    common::ProcessorOutput getOutput() { return _output; }
    const std::vector<std::string>& getClusterNames() const { return _clusterNames; }
    ProcessorConfigInfo getConfigInfo() const
    {
        ProcessorConfigInfo info;
        info.configPath = _configPath;
        info.stage = _stage;
        info.preStage = _preStage;
        info.isLastStage = _isLastStage;
        info.configName = _configName;
        return info;
    }
    // const std::string& getUsingConfig() const { return _configPath; }
    bool finish(const KeyValueMap& kvMap) override;
    void setNeedToRun() { _alreadyRun = false; }
    void setBeeperTags(const beeper::EventTagsPtr beeperTags) override;

public:
    bool suspendTask(bool forceSuspend, const std::string& suspendReason);
    std::string getSuspendReason() const { return _suspendReason; }
    bool suspendTask(bool forceSuspend) override;
    void stop(int64_t stopTimestamp);
    int64_t getStopTimestamp() { return _switchTimestamp; }
    int32_t getProcessorTaskId() const { return _id; }
    std::string getTaskIdentifier();
    bool getSchemaId(const std::string& clusterName, int64_t& schemaId) const;
    void fillProcessorInfo(proto::ProcessorInfo* processorInfo, const proto::WorkerNodes& nodes,
                           const CounterCollector::RoleCounterMap& roleCounterMap) const;
    proto::BuildStep getBuildStep() const { return _step; }
    bool checkAllClusterConfigEqual()
    {
        std::string targetConfig;
        return checkAllClusterConfigEqual(targetConfig);
    }

    void doSupplementLableInfo(KeyValueMap& info) const override;
    std::string getTaskPhaseIdentifier() const override;

private:
    bool registCheckpointInfo();
    bool checkAllClusterConfigEqual(std::string& targetConfig);
    bool checkUpdateConfigLegal(const std::string& configPath);
    void waitSuspended(proto::ProcessorNodes& nodes);
    void getStartLocator(common::Locator& locator);
    void getStartLocator(std::string& clusterName, int64_t schemaId, const std::string& topicId,
                         common::Locator& locator);
    bool loadFromConfig(const config::ResourceReaderPtr& resourceReader) override;
    bool fillSchemaIdInfo(const config::ResourceReaderPtr& resourceReader);
    void startWorkers(proto::ProcessorNodes& nodes);
    void switchDataSource();
    bool isInLastDataSource() const;
    proto::ProcessorTarget generateTargetStatus() const;
    virtual int64_t getCurrentTime() const;
    uint32_t getParallelNum(const config::ProcessorRuleConfig& processorRuleConfig) const;
    uint32_t getPartitionCount(const config::ProcessorRuleConfig& processorRuleConfig) const;

    ProcessorNodesUpdaterPtr createProcessorNodesUpdater();

    std::string calculateRoleClusterName();
    void updateCheckpoint();
    void ReportCheckpointFreshness(const proto::ProcessorNodes& processorNodes);
    void registBrokerTopics();
    void deregistBrokerTopics();
    void createProcessorTaskId();
    bool doInit(const ProcessorInput& input, const common::ProcessorOutput& output,
                const std::vector<std::string>& clusterNames, const ProcessorConfigInfo& configInfo, bool changeTaskId);
    bool getBrokerTopicId(const std::string& clusterName, std::string& topicId);
    ProcessorCheckpointAccessorPtr getProcessorCheckpointAccessor();
    bool prepareStartLocator(common::Locator& locator);
    void prepareBrokerTopicForCluster(const std::string& clusterName, const proto::BuildStep& step,
                                      const config::ResourceReaderPtr& configReader,
                                      TaskResourceManagerPtr& resourceManager);
    bool prepareSlowNodeDetector(const std::string& clusterName) override;
    bool rewriteDetectSlowNodeByLocatorConfig(config::SlowNodeDetectConfig& slowNodeDetectConfig);
    bool executeWriterTodo(const proto::ProcessorNodes& processorNodes);
    // virtual for test
    virtual bool updateSwiftWriterVersion(const proto::ProcessorNodes& processorNodes, uint32_t majorVersion,
                                          const std::vector<std::pair<size_t, uint32_t>>& updatedMinorVersions) const;
    virtual common::SwiftAdminFacadePtr getSwiftAdminFacade(const std::string& clusterName) const;
    void syncWriterVersions();
    bool getSyncedWriterVersionsIfEmpty();
    void AdaptiveScaling();

private:
    ProcessorInput _input;
    common::ProcessorOutput _output;
    uint32_t _partitionCount;
    uint32_t _parallelNum;
    config::ProcessorRuleConfig _processorRuleConfig;
    ProcessorTargetInfos _lastTargetInfos;
    ProcessorWriterVersion _writerVersion;

    proto::BuildStep _step;
    int64_t _switchTimestamp;
    bool _useCustomizeProcessorCount;
    int32_t _id;
    ProcessorNodesUpdaterPtr _nodesUpdater;
    FatalErrorDiscovererPtr _fatalErrorDiscoverer;
    std::vector<std::string> _clusterNames;
    std::map<std::string, int64_t> _clusterToSchemaId;
    std::map<std::string, std::string> _clusterToTopicId;
    bool _fullBuildProcessLastSwiftSrc;
    bool _hasIncDatasource;
    bool _alreadyRun;
    std::string _suspendReason;
    volatile int64_t _beeperReportTs;
    volatile int64_t _reportCheckpointTs;
    bool _batchMode;
    std::string _preStage;
    std::string _stage;
    bool _isLastStage;
    std::string _configName;
    bool _isTablet;
    bool _safeWrite;
    bool _needUpdateTopicVersionControl;

    ProcessorCheckpointAccessorPtr _processorCheckpointAccessor;
    std::map<std::string, common::ResourceKeeperGuardPtr> _brokerTopicKeepers;

    uint32_t _runningPartitionCount;
    uint32_t _runningParallelNum;

    std::optional<WriterUpdateInfo> _toUpdateWriterInfo;
    std::unique_ptr<ProcessorAdaptiveScaling> _adaptiveScaler;
    bool _adaptiveScalingFixedMax;

public:
    static const int64_t DEFAULT_SWITCH_TIMESTAMP;
    static const int64_t DEFAULT_SUSPEND_TIMESTAMP;
    static const int64_t DEFAULT_SWIFT_SRC_FULL_BUILD_TIMEOUT;
    static const int64_t READER_SYNC_TIME_INTERVAL;
    static const std::string PROCESSOR_WRITER_VERSION;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTask);

}} // namespace build_service::admin
