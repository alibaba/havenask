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

#include <algorithm>
#include <limits>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/DefaultSlowNodeDetectStrategy.h"
#include "build_service/admin/FatalErrorDiscoverer.h"
#include "build_service/admin/NewSlowNodeDetectStrategy.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {

class SingleBuilderTask : public AdminTaskBase
{
public:
    SingleBuilderTask(const proto::BuildId& buildId, const std::string& cluserName, const std::string& taskId,
                      const TaskResourceManagerPtr& resMgr);

    ~SingleBuilderTask();

public:
    bool init(proto::BuildStep buildStep);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool run(proto::WorkerNodes& workerNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override;
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    bool updateConfig() override;

public:
    void fillClusterInfo(proto::SingleClusterInfo* clusterInfo, const proto::BuilderNodes& nodes,
                         const CounterCollector::RoleCounterMap& roleCounterMap) const;
    proto::BuildStep getStep() const;
    void clearFullWorkerZkNode(const std::string& generationDir) const;
    bool isBatchMode() const { return _batchMode; }
    void notifyStopped() override;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    std::string getTaskPhaseIdentifier() const override;
    std::string getClusterName() const;
    proto::BuildStep getBuildStep() const;
    bool getBuilderCheckpoint(proto::BuilderCheckpoint& checkpoint) const;
    int32_t getWorkerPathVersion() const
    {
        // _workerProtoclVersion = 1 is buggy, which does not support inc parallel build
        if (_workerProtocolVersion == 1 || _disableIncParallelInstanceDir) {
            return -1;
        }
        return _workerPathVersion;
    }
    int32_t getWorkerProtocolVersion() const { return _workerProtocolVersion; }
    std::string getOngoingOpIds() const { return _opIds; }
    int64_t getSchemaId() const { return _schemaVersion; }
    uint32_t getBuildParallelNum() const { return _buildParallelNum; }
    int32_t getBatchMask() const { return _batchMask; }

private:
    /**
     * return true when builder stopped at specified timestamp
     */
    std::string getTopicName() const;
    void stopBuild(int64_t finishTimeInSeconds, uint64_t readAfterFinishTsInSeconds);
    bool run(int32_t workerProtocolVersion, proto::BuilderNodes& activeNodes);
    bool updateConfig(const std::string& configPath);
    bool loadFromConfig(const config::ResourceReaderPtr& resourceReader) override;
    bool determineBuilderCountPerPartition(size_t currentBuilderCount, int32_t currentWorkerVersion,
                                           int32_t& builderCountPerPartition);
    void updateWorkerPathVersion();
    bool startFromCheckpoint(proto::BuildStep buildStep, int64_t schemaId);
    int32_t generateInitialWorkerPathVersion() const;
    int32_t getNextWorkerPathVersion(int32_t lastWorkerPathVersion) const
    {
        return lastWorkerPathVersion == std::numeric_limits<int32_t>::max() ? 1 : (lastWorkerPathVersion + 1);
    }
    void generateAndSetTarget(proto::BuilderNodes& nodes) const;
    proto::BuilderTarget generateTargetStatus() const;
    void endBuild();
    bool prepareBuilderDataDescription(const KeyValueMap& kvMap, proto::DataDescription& ds);
    std::string getTopicClusterName(const KeyValueMap& kvMap, const proto::DataDescription& ds);

    bool needBuildFromDataSource(const proto::DataDescription& ds) const;
    void registBrokerTopic();
    void deregistBrokerTopic();

    bool getStopTimestamp(const KeyValueMap& kvMap, const proto::DataDescription& ds, int64_t& stopTs);

    void updateMinLocatorAndConsumeRate(int64_t minLocatorTs);
    void doSupplementLableInfoForLocatorInfo(KeyValueMap& info) const;
    bool getTopicId(std::string& topicId);
    bool initOpIds(uint32_t maxOpId);
    bool canStopBuild(int64_t processorCheckpoint, int64_t stopBuildRequiredLatencyInSecond,
                      int64_t stopBuildTimeoutInSecond);
    bool doWaitSuspended(proto::BuilderNodes& builderNodes);
    bool prepareSlowNodeDetector(const std::string& clusterName) override
    {
        return doPrepareSlowNodeDetector<proto::BuilderNodes>(clusterName);
    }

private:
    proto::BuildStep _buildStep;
    std::string _taskId;
    std::string _clusterName;
    std::string _topicClusterName;
    uint32_t _partitionCount;
    uint32_t _buildParallelNum;
    int64_t _startTimestamp;
    int64_t _stopTimestamp;
    int64_t _versionTimestamp;
    int32_t _workerProtocolVersion;
    int32_t _workerPathVersion;
    int64_t _schemaVersion;
    bool _disableIncParallelInstanceDir;
    bool _incBuildParallelConfigUpdated;
    int64_t _processorCheckpoint;
    int64_t _processorDataSourceIdx;
    uint64_t _builderStartTimestamp;
    uint64_t _builderStopTimestamp;
    int64_t _minLocatorTimestamp;
    int64_t _lastMinLocatorTsValue;
    int64_t _lastMinLocatorSystemTs;
    double _consumeRate;
    proto::DataDescription _builderDataDesc;
    bool _batchMode;
    std::string _opIds;

    common::IndexCheckpointAccessorPtr _indexCkpAccessor;
    common::BuilderCheckpointAccessorPtr _builderCkpAccessor;

    FatalErrorDiscovererPtr _fatalErrorDiscoverer;
    // this is used for data align in batch, not same as batchMode
    int32_t _batchMask;
    bool _useRandomInitialWorkerPathVersion;
    bool _versionTimestampAlignSrc;
    common::ResourceKeeperGuardPtr _brokerTopicKeeper;

private:
    static const int64_t DEFAULT_STOP_TIMESTAMP;
    static const int64_t DEFAULT_VERSION_TIMESTAMP;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleBuilderTask);

}} // namespace build_service::admin
