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
#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/admin/AgentMetricReporter.h"
#include "build_service/admin/AgentRoleInfo.h"
#include "build_service/admin/AgentRolePlanMaker.h"
#include "build_service/admin/AppPlanMaker.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/FatalErrorMetricReporter.h"
#include "build_service/admin/GenerationMetricsReporter.h"
#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/admin/ProhibitedIpCollector.h"
#include "build_service/admin/ScheduleMetricReporter.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/admin/TaskStatusMetricReporter.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/catalog/CatalogPartitionIdentifier.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/SharedPtrGuard.h"
#include "hippo/DriverCommon.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"
#include "kmonitor_adapter/Monitor.h"
#include "master_framework/ScheduleUnit.h"
#include "worker_framework/ZkState.h"

namespace build_service::common {
class RpcChannelManager;
}

namespace build_service { namespace admin {
class CatalogVersionPublisher;

class GenerationKeeper : public proto::ErrorCollector
{
public:
    struct Param {
        const proto::BuildId& buildId;
        const std::string& zkRoot;
        const std::string& adminServiceName;
        const std::string& amonitorPort;
        cm_basic::ZkWrapper* zkWrapper = nullptr;
        const ProhibitedIpCollectorPtr& prohibitIpCollector;
        kmonitor_adapter::Monitor* monitor = nullptr;
        std::shared_ptr<common::RpcChannelManager> catalogRpcChannelManager;
        std::shared_ptr<CatalogPartitionIdentifier> catalogPartitionIdentifier;
        std::vector<hippo::PackageInfo> specifiedWorkerPkgList;
    };
    typedef worker_framework::ZkState ZkState;
    struct GenerationRolePlan {
        AppPlanPtr appPlan;
        AgentRoleInfoMapPtr agentRolePlan;
        std::string configPath;
        bool allowAssignToGlobalAgents = false;
        std::string targetGlobalAgentGroupName;
    };
    typedef std::shared_ptr<GenerationRolePlan> GenerationRolePlanPtr;

    struct WorkerRoleInfo {
        std::string identifier;
        std::string roleType;
        int64_t heartbeatTime;
        int64_t workerStartTime;
        bool isFinish;
        std::string agentRoleName;
        std::string agentIdentifier;
        std::string taskIdentifier;
        std::string workerStatus;
        int64_t cpuAmount;
        int64_t memAmount;
    };
    typedef std::map<std::string, WorkerRoleInfo> WorkerRoleInfoMap;
    typedef std::shared_ptr<WorkerRoleInfoMap> WorkerRoleInfoMapPtr;

    using AgentRoleTargetMap = GenerationTaskBase::AgentRoleTargetMap;

public:
    GenerationKeeper(const Param& param);
    ~GenerationKeeper();

private:
    GenerationKeeper(const GenerationKeeper&);
    GenerationKeeper& operator=(const GenerationKeeper&);

public:
    void transferErrorInfos(std::vector<proto::ErrorInfo>& errorInfos)
    {
        fillErrorInfos(errorInfos);
        clearErrorInfos();
    }
    bool recover();
    bool recoverStoppedKeeper();

public:
    // public operator for tools
    void startBuild(const proto::StartBuildRequest* request, proto::StartBuildResponse* response);
    void startTask(const proto::StartTaskRequest* request, proto::InformResponse* response);
    void stopTask(const proto::StopTaskRequest* request, proto::InformResponse* response);
    void callGraph(const proto::CallGraphRequest* request, proto::InformResponse* response);
    void printGraph(const proto::PrintGraphRequest* request, proto::InformResponse* response);

    void fillTaskInfo(int64_t taskId, proto::TaskInfo* taskInfo, bool& isExist);
    size_t getRunningTaskNum() const;
    bool stopBuild();
    bool suspendBuild(const std::string& flowId, std::string& errorMsg);
    bool resumeBuild(const std::string& flowId, std::string& errorMsg);
    bool getBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                         const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges, std::string* resultStr,
                         std::string& errorMsg);
    bool bulkload(const std::string& clusterName, const std::string& bulkloadId,
                  const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                  const std::string& options, const std::string& action, std::string* errorMsg);
    bool rollBack(const std::string& clusterName, versionid_t versionId, int64_t startTimestamp, std::string& errorMsg);
    bool rollBackCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                            const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges, std::string& errorMsg);
    bool markCheckpoint(const std::string& clusterName, versionid_t versionId, bool reserveFlag, std::string& errorMsg);
    bool createSavepoint(const std::string& clusterName, checkpointid_t checkpointId, const std::string& comment,
                         std::string& savepointStr, std::string& errorMsg);
    bool removeSavepoint(const std::string& clusterName, checkpointid_t checkpointId, std::string& errorMsg);
    bool commitVersion(const std::string& clusterName, const proto::Range& range, const std::string& versionMeta,
                       bool instantPublish, std::string& errorMsg);
    bool getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                              std::string& result, std::string& errorMsg);
    bool listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
                        std::string* result) const;
    bool getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId, std::string* result) const;
    bool getReservedVersions(const std::string& clusterName, const proto::Range& range,
                             std::set<indexlibv2::framework::VersionCoord>* reservedVersions) const;
    bool updateConfig(const std::string& configPath);
    const std::string getConfigPath() const;
    const std::string getFatalErrorString() const;
    std::shared_ptr<AgentRoleTargetMap> getAgentRoleTargetMap() const;
    bool createVersion(const std::string& clusterName, const std::string& mergeConfigName);
    bool switchBuild();
    void fillGenerationInfo(proto::GenerationInfo* generationInfo, bool fillTaskHistory, bool fillCounterInfo = true,
                            bool fillFlowInfo = false, bool fillEffectiveIndexInfo = false) const;

    void fillWorkerRoleInfo(WorkerRoleInfoMap& roleInfoMap, bool onlyActiveRole);
    void fillWorkerNodeStatus(const std::set<std::string>& roleNames, proto::WorkerNodeStatusResponse* response);

    void stepBuild(proto::InformResponse* response);

    void migrateTargetRoles(const std::set<std::string>& roleNames,
                            std::map<std::string, std::string>* successRoleInfos);

    bool getTotalRemainIndexSize(int64_t& totalSize);

public:
    typedef GenerationMetricsReporter::MetricsRecord GenerationMetrics;
    typedef std::shared_ptr<GenerationMetrics> GenerationMetricsPtr;
    typedef std::shared_ptr<RoleSlotInfos> RoleSlotInfosPtr;

public:
    // for service keeper
    virtual GenerationRolePlanPtr generateRolePlan(AgentSimpleMasterScheduler* scheduler);

    void fillSlowWorkerSlots(RoleSlotInfos& roleSlots);
    void updateSlotInfos(const RoleSlotInfosPtr& slotInfos) { _roleSlotInfoGuard.set(slotInfos); }
    void syncCounters()
    {
        if (_jobCounterCollector) {
            _jobCounterCollector->syncCounters();
        }
    }
    bool validateConfig(const std::string& configPath, std::string& errorMsg, bool firstTime)
    {
        return _generationTask->validateConfig(configPath, errorMsg, firstTime);
    }

    GenerationMetricsPtr getGenerationMetrics() const
    {
        GenerationMetricsPtr generationMetrics;
        _metricsGuard.get(generationMetrics);
        return generationMetrics;
    }

    bool isStarting() const;
    bool isStopping() const;
    bool getBuildStep(proto::BuildStep& buildStep) const;
    GenerationTaskBase::GenerationStep getGenerationStep() const;

    bool isStopped() const;
    int64_t getStopTimeStamp() const;

    const proto::BuildId& getBuildId() const;
    bool isIndexDeleted() const;
    bool deleteTmpBuilderIndex() const { return _generationTask->deleteTmpBuilderIndex(); }
    void deleteIndex() const { _generationTask->deleteIndex(); }
    void clearThreads();
    bool clearCounters();

    const GenerationMetricsReporterPtr& getMetricsReporter() const { return _metricsReporter; }
    GenerationMetricsReporterPtr stealMetricsReporter()
    {
        GenerationMetricsReporterPtr ret = _metricsReporter;
        _metricsReporter.reset();
        return ret;
    }
    void resetMetricsReporter(const GenerationMetricsReporterPtr& reporter) { _metricsReporter = reporter; }
    void reportMetrics();
    // following interfaces will trigger _decisionMutex
    bool cleanVersions(const std::string& clusterName, versionid_t versionId);
    int64_t getLastRefreshTime() const;
    void updateLastRefreshTime();
    const std::string& getIndexRoot() const;
    std::vector<std::string> getClusterNames() const;
    GenerationTaskBase::TaskType getGenerationTaskType() const
    {
        return _generationTask ? _generationTask->getType() : GenerationTaskBase::TT_UNKNOWN;
    }

    std::string getGenerationTaskTypeString() const;

    const std::string& getGenerationDir() const { return _generationDir; }

    void fillGenerationCheckpointInfo(std::map<std::string, std::vector<std::pair<std::string, int64_t>>>& infoMap);

    void fillGenerationTaskNodeStatus(std::vector<TaskStatusMetricReporter::TaskLogItem>& taskInfo);

    void fillGenerationGraphData(std::string& flowGraph, std::map<std::string, std::string>& flowSubGraphMap,
                                 std::map<std::string, std::map<std::string, std::string>>& taskDetailInfoMap);
    bool cleanGenerationDir() const;

protected:
    // only for mock test
    virtual bool needAutoStop();
    bool needSyncCounter() const;
    virtual GenerationTaskBase* createGenerationTask(const proto::BuildId& buildId, bool jobMode,
                                                     const std::string& buildMode);
    void workThread();

private:
    static bool getImportedVersionFromRequest(const proto::StartBuildRequest* request,
                                              GenerationTaskBase::ImportedVersionIdMap* importedVersionIdMap);
    bool importBuild(const std::string& configPath, const std::string& dataDescriptionKvs,
                     const proto::BuildId& buildId,
                     const GenerationTaskBase::ImportedVersionIdMap& importedVersionIdMap,
                     proto::StartBuildResponse* response);

    void updateActiveNodeStatistics(const RoleSlotInfosPtr& assignRoleSlots, RoleSlotInfosPtr& slowRoleSlots,
                                    GenerationMetricsPtr& generationMetrics);

    bool restart();
    template <class ResponseType>
    void initFromStatus(ResponseType* response);
    bool recoverGenerationTask(const std::string& status);
    virtual GenerationTaskBase* innerRecoverGenerationTask(const std::string& status);
    void setAppPlanMaker(AppPlanMaker* maker);
    void fillGenerationSlotInfo(const RoleSlotInfosPtr& roleSlots) const;
    void setAgentRoleTargetMap(const std::shared_ptr<AgentRoleTargetMap>& targetMap);

private:
    // status operation
    bool writeStatus(const std::string& status);
    bool readStatus(std::string& status);
    bool readStopFile(std::string& status);

private:
    void releaseInnerResource();
    bool moveGenerationStatus(const std::string& src, const std::string& dst);
    AppPlanMaker* createAppPlanMaker(const std::string& configPath, const std::string& heartbeatType);
    bool prepareCounterCollector(const config::CounterConfig& counter);

private:
    void fillGenerationResource(proto::GenerationInfo* generationInfo) const;
    template <typename RoleInfo>
    void fillRoleResources(RoleInfo* roleInfo) const;
    void rewriteRoleArguments(const std::string& roleName, RolePlan& rolePlan);

private:
    virtual const std::string& createTaskSnapshot();
    bool commitTaskStatus(const std::string& oldStatus);
    void getStatus(GenerationTaskBase::GenerationStep& generationStep, proto::BuildStep& buildStep);

    void syncProgress(GenerationTaskBase::GenerationStep lastGenerationStep, proto::BuildStep lastBuildStep);

    bool initGraphParameter(const proto::CallGraphRequest* request, KeyValueMap& kvMap) const;
    void resetAgentRolePlanMaker();
    void syncConfigPathForAgentPlan(const GenerationRolePlanPtr& rolePlan);
    CounterCollector* createCounterCollector(config::CounterConfig counterConfig) const;

private:
    // for test
    GenerationTaskBase* getGenerationTask() const { return _generationTask; }

public:
    static const uint32_t GENERATION_STATUS_SIZE_THRESHOLD;

protected:
    proto::BuildId _buildId;
    GenerationTaskBase* _generationTask;
    std::string _latestSnapshot;
    std::string _zkRoot;
    std::string _adminServiceName;
    std::string _amonitorPort;
    std::string _generationDir;
    cm_basic::ZkWrapper* _zkWrapper;
    ZkState _zkStatus;

    volatile bool _isStopped;
    volatile bool _needSyncStatus;
    mutable autil::RecursiveThreadMutex _decisionMutex;
    mutable autil::RecursiveThreadMutex _generationMutex;
    autil::LoopThreadPtr _loopThreadPtr;
    WorkerTable* _workerTable;
    CounterCollectorPtr _jobCounterCollector;
    GenerationMetricsReporterPtr _metricsReporter;
    ScheduleMetricReporterPtr _scheduleMetricReporter;
    CheckpointMetricReporterPtr _checkpointMetricReporter;
    TaskStatusMetricReporterPtr _taskStatusMetricReporter;
    FatalErrorMetricReporterPtr _fatalErrorMetricReporter;
    SlowNodeMetricReporterPtr _slowNodeMetricReporter;
    ProhibitedIpCollectorPtr _prohibitIpCollector;
    util::SharedPtrGuard<RoleSlotInfos> _roleSlotInfoGuard;
    util::SharedPtrGuard<RoleSlotInfos> _slowRoleSlotInfoGuard;
    util::SharedPtrGuard<AppPlanMaker> _appPlanMakerGuard;
    util::SharedPtrGuard<AgentRolePlanMaker> _agentRolePlanMakerGuard;
    util::SharedPtrGuard<GenerationMetrics> _metricsGuard;

    int64_t _startingBuildTimestamp;
    int64_t _stoppingBuildTimestamp;
    volatile int64_t _refreshTimestamp;
    bool _configPathChanged;
    AgentMetricReporter _reporter;
    std::unique_ptr<CatalogVersionPublisher> _catalogVersionPublisher;
    std::shared_ptr<common::RpcChannelManager> _catalogRpcChannelManager;
    std::shared_ptr<CatalogPartitionIdentifier> _catalogPartitionIdentifier;
    std::vector<hippo::PackageInfo> _specifiedWorkerPkgList;

private:
    struct RoleReleaseInfo {
        std::vector<std::string> releasedIps;
        bool reachReleaseLimit;
        RoleReleaseInfo() : reachReleaseLimit(false) {}
    };
    typedef std::map<std::string, RoleReleaseInfo> RoleReleaseInfoMap;
    RoleReleaseInfoMap _roleReleaseInfoMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationKeeper);

}} // namespace build_service::admin
