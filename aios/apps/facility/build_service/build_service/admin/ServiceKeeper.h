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
#ifndef ISEARCH_BS_SERVICEKEEPER_H
#define ISEARCH_BS_SERVICEKEEPER_H

#include "autil/LoopThread.h"
#include "autil/OutputOrderedThreadPool.h"
#include "build_service/admin/AdminHealthChecker.h"
#include "build_service/admin/AdminMetricsReporter.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/admin/GenerationRecoverWorkItem.h"
#include "build_service/admin/GlobalAgentMaintainer.h"
#include "build_service/admin/IndexInfoCollector.h"
#include "build_service/admin/ProhibitedIpsTable.h"
#include "build_service/admin/ServiceInfoFilter.h"
#include "build_service/admin/ServiceKeeperObsoleteDataCleaner.h"
#include "build_service/admin/TCMallocMemController.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/util/Log.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "master_framework/ScheduleUnit.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "worker_framework/ZkState.h"

namespace cm_basic {
class ZkWrapper;
};

namespace build_service { namespace admin {

class ServiceKeeper
{
public:
    typedef MASTER_FRAMEWORK_NS(simple_master)::SimpleMasterScheduler SimpleMasterScheduler;
    typedef worker_framework::LeaderChecker LeaderChecker;
    typedef worker_framework::ZkState ZkState;
    typedef std::map<proto::BuildId, GenerationKeeperPtr> GenerationKeepers;

public:
    ServiceKeeper();
    virtual ~ServiceKeeper();

private:
    ServiceKeeper(const ServiceKeeper&);
    ServiceKeeper& operator=(const ServiceKeeper&);

public:
    // client code must guarantee *start* first
    bool start(const std::string& zkRoot, const std::string& adminServiceName, cm_basic::ZkWrapper* zkWrapper,
               SimpleMasterScheduler* scheduler, uint16_t amonitorPort, uint32_t prohibitTime = DEFAULT_PROHIBIT_TIME,
               uint32_t prohibitSize = DEFAULT_PROHIBIT_NUM, kmonitor_adapter::Monitor* monitor = NULL);

    virtual void stop();

public:
    void startBuild(const proto::StartBuildRequest* request, proto::StartBuildResponse* response);
    void startTask(const proto::StartTaskRequest* request, proto::InformResponse* response);
    void stopTask(const proto::StopTaskRequest* request, proto::InformResponse* response);
    void suspendBuild(const proto::SuspendBuildRequest* request, proto::InformResponse* response);
    void resumeBuild(const proto::ResumeBuildRequest* request, proto::InformResponse* response);
    void rollBack(const proto::RollBackRequest* request, proto::InformResponse* response);
    void rollBackCheckpoint(const proto::RollBackCheckpointRequest* request, proto::InformResponse* response);
    void createSnapshot(const proto::CreateSnapshotRequest* request, proto::InformResponse* response);
    void removeSnapshot(const proto::RemoveSnapshotRequest* request, proto::InformResponse* response);
    void createSavepoint(const proto::CreateSavepointRequest* request, proto::InformResponse* response);
    void removeSavepoint(const proto::RemoveSavepointRequest* request, proto::InformResponse* response);
    void listCheckpoint(const proto::ListCheckpointRequest* request, proto::InformResponse* response);
    void commitVersion(const proto::CommitVersionRequest* request, proto::InformResponse* response);
    void getCommittedVersions(const proto::GetCommittedVersionsRequest* request, proto::InformResponse* response);
    void getCheckpoint(const proto::GetCheckpointRequest* request, proto::InformResponse* response);
    void stopBuild(const proto::StopBuildRequest* request, proto::InformResponse* response);
    void updateConfig(const proto::UpdateConfigRequest* request, proto::InformResponse* response);
    void stepBuild(const proto::StepBuildRequest* request, proto::InformResponse* response);
    void createVersion(const proto::CreateVersionRequest* request, proto::InformResponse* response);
    void switchBuild(const proto::SwitchBuildRequest* request, proto::InformResponse* response);
    void fillServiceInfo(const proto::ServiceInfoRequest* request, proto::ServiceInfoResponse* response) const;

    void fillTaskInfo(const proto::TaskInfoRequest* request, proto::TaskInfoResponse* response);
    void fillWorkerRoleInfo(const proto::WorkerRoleInfoRequest* request, proto::WorkerRoleInfoResponse* response);
    void fillWorkerNodeStatus(const proto::WorkerNodeStatusRequest* request, proto::WorkerNodeStatusResponse* response);

    void cleanVersions(const proto::CleanVersionsRequest* request, proto::InformResponse* response);

    void callGraph(const proto::CallGraphRequest* request, proto::InformResponse* response);
    void printGraph(const proto::PrintGraphRequest* request, proto::InformResponse* response);
    void updateGsTemplate(const proto::UpdateGsTemplateRequest* request, proto::InformResponse* response);
    void updateGlobalAgentConfig(const proto::UpdateGlobalAgentConfigRequest* request, proto::InformResponse* response);
    void startGeneralTask(const proto::StartGeneralTaskRequest* request, proto::InformResponse* response);
    void migrateTargetRoles(const proto::MigrateRoleRequest* request, proto::InformResponse* response);
    void getGeneralInfo(const proto::InformRequest* request, proto::InformResponse* response);
    void getBulkloadInfo(const proto::BulkloadInfoRequest* request, proto::InformResponse* response);

public:
    const GenerationKeeperPtr& getGeneration(const proto::BuildId& buildId, bool includeHistory, bool& isRecoverFailed,
                                             bool fuzzy = false);
    void removeStoppedGeneration(const proto::BuildId& buildId);
    const std::string& getZkRoot() const { return _zkRoot; }
    void getStoppedGenerations(BizGenerationsMap& generations) const;
    int64_t getLastRefreshTime() const;
    int64_t getLastScheduleTimestamp() const;
    static std::string buildIdToJobId(const proto::BuildId& buildId);
    void setHttpAddress(const std::string& addrStr) { _httpAddress = addrStr; }
    void setHippoZkRoot(const std::string& hippoRoot) { _hippoZkRoot = hippoRoot; }

protected:
    virtual GenerationKeeperPtr doCreateGenerationKeeper(const GenerationKeeper::Param& param);

private:
    static std::vector<hippo::PackageInfo> prepareSpecifiedWorkerPkgList();
    // virtual for test
    void syncAdminInfo();
    bool collectRecoverBuildIds(std::vector<proto::BuildId>& buildIds);
    virtual bool recover(const std::vector<proto::BuildId>& buildIds);
    void waitRecoverFinished(const std::vector<proto::BuildId>& buildIds,
                             const autil::OutputOrderedThreadPoolPtr& recoverThreadPool);
    void keepServiceLoop();
    void releaseSlowWorkerSlots(const RoleSlotInfos& slowWokerSlots);
    void releaseFailedSlots();
    bool getActiveGenerationsToStop(const proto::BuildId& buildId, std::vector<GenerationKeeperPtr>& keepers) const;
    void setGeneration(const proto::BuildId& buildId, const GenerationKeeperPtr& keeper);
    const GenerationKeeperPtr& findGeneration(const GenerationKeepers& keepers, const proto::BuildId& buildId,
                                              bool fuzzy) const;
    bool isIndexDeleted(const proto::BuildId& buildId) const;
    virtual AppPlan generateAppPlan(const GenerationKeepers& generationKeepers,
                                    const std::vector<std::string>& prohibitedIps);

    void resetMetricsRecord();
    void recoverLoop();
    GenerationRecoverWorkItem* createGenerationRecoverWorkItem(const proto::BuildId& buildId);
    bool isBuildIdMatchRequest(const proto::ServiceInfoRequest* request, const proto::BuildId& buildId) const;
    bool isBuildIdMatched(const proto::BuildId& requestBuildId, const proto::BuildId& candidateBuildId) const;
    bool needFillCounterInfo(const proto::ServiceInfoRequest* request) const;
    size_t getThreadNumFromEnv() const;
    size_t getRecoverSleepSecondFromEnv() const;
    bool needRecover();
    int32_t getSyncCounterInterval() const;
    virtual GenerationKeeperPtr createGenerationKeeper(const proto::BuildId& buildId); // virtual for test
    virtual bool doInit();

    void syncScheduleTimestamp();
    template <typename RequestType>
    proto::BuildId getBuildId(const RequestType* request) const;
    bool isMatchedBuildId(const std::string& jobId, const proto::BuildId& buildId) const;
    std::vector<GenerationKeeperPtr> getAttachedGenerations(const proto::BuildId& buildId) const;

    bool updateGlobalAgentConfig(const std::string& configStr);

    /* { global_id, group_id } -> agentNodeCount */
    bool updateGlobalAgentNodeCount(const std::shared_ptr<GlobalAgentMaintainer>& targetAgentMaintainer,
                                    const std::map<std::pair<std::string, std::string>, uint32_t>& agentGroupCountMap);

    static bool
    generateNewGlobalAgentConfig(const std::shared_ptr<GlobalAgentMaintainer>& agentMaintainer,
                                 const std::map<std::pair<std::string, std::string>, uint32_t>& agentGroupCountMap,
                                 std::string& newConfigStr);

    void setGlobalAgentMaintainer(const std::shared_ptr<GlobalAgentMaintainer>& agentMaintainer);
    std::shared_ptr<GlobalAgentMaintainer> getGlobalAgentMaintainer() const;
    std::string getGenerationListInfoStr() const;
    bool getGenerationDetailInfoStr(const proto::BuildId& buildid, const std::string& paramStr,
                                    std::string& detailInfoStr) const;

    std::vector<GenerationKeeperPtr> getBuildIdMatchedGenerationKeepers(const proto::BuildId& buildid,
                                                                        bool onlyActive) const;
    void fillWorkerRoleInfoMap(
        const std::vector<GenerationKeeperPtr>& keepers, bool onlyActiveRole,
        std::map<proto::BuildId, GenerationKeeper::WorkerRoleInfoMapPtr>& multiGenerationRoleMap) const;

    void fillTaskStatusInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const;
    void fillSummaryInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const;
    void fillTaskFlowGraphInfo(const GenerationKeeperPtr& keeper, bool onlyBriefInfo,
                               autil::legacy::json::JsonMap& jsonMap) const;
    void fillCheckpointInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const;
    void fillWorkerRoleInfo(const GenerationKeeper::WorkerRoleInfoMapPtr& roleInfoMap,
                            autil::legacy::json::JsonMap& jsonMap) const;

    std::string getBuildStepString(const GenerationKeeperPtr& keeper) const;
    std::string getGenerationStepString(const GenerationKeeperPtr& keeper) const;

private:
    struct ServiceKeeperMetric {
        kmonitor_adapter::MetricPtr krpcRequestLatency;
        kmonitor_adapter::MetricPtr krpcRequestQps;
        kmonitor_adapter::MetricPtr kstartBuildLatency;
        kmonitor_adapter::MetricPtr kstartBuildQps;
        kmonitor_adapter::MetricPtr kstartTaskLatency;
        kmonitor_adapter::MetricPtr kstartTaskQps;

        kmonitor_adapter::MetricPtr kgsLatency;
        kmonitor_adapter::MetricPtr kgsQps;
        kmonitor_adapter::MetricPtr kstopBuildQps;
        kmonitor_adapter::MetricPtr kstopTaskQps;

    public:
        ServiceKeeperMetric();
        void init(kmonitor_adapter::Monitor* monitor);
    };

protected:
    static constexpr uint32_t DEFAULT_PROHIBIT_TIME = 2 * 60 * 60;
    static constexpr uint32_t DEFAULT_PROHIBIT_NUM = 5;
    static constexpr size_t DEFAULT_RECOVER_SLEEP_SEC = 60;
    static constexpr size_t DEFAULT_THREAD_NUM = 1;
    static constexpr size_t DEFAULT_SYNC_SCHEDULE_TIMESTAMP_SEC = 10;

    std::string _zkRoot;
    std::string _adminServiceName;
    cm_basic::ZkWrapper* _zkWrapper;
    ZkState* _zkStatus;
    SimpleMasterScheduler* _scheduler;
    ServiceKeeperObsoleteDataCleaner* _cleaner;
    uint16_t _amonitorPort;

    autil::LoopThreadPtr _loopThread;
    autil::LoopThreadPtr _counterSyncThread;
    autil::ThreadPtr _recoverThread;
    mutable autil::ThreadMutex _mapLock;

    GenerationKeepers _activeGenerationKeepers AUTIL_GUARDED_BY(_mapLock);
    GenerationKeepers _allGenerationKeepers AUTIL_GUARDED_BY(_mapLock);
    std::set<proto::BuildId> _recoverFailedGenerations;

    kmonitor_adapter::Monitor* _monitor;
    AdminMetricsReporterPtr _metricsReporter;
    AdminMetricsReporter::GlobalMetricsRecord _globalMetricRecord;
    ProhibitedIpsTablePtr _prohibitedIpsTable;

    mutable autil::ThreadMutex _startBuildLock;
    std::set<proto::BuildId> _startingGenerations;
    std::vector<hippo::PackageInfo> _specifiedWorkerPkgList;

    ServiceKeeperMetric _keeperMetric;
    AdminHealthCheckerPtr _healthChecker;
    TCMallocMemControllerPtr _memController;
    IndexInfoCollectorPtr _indexInfoCollector;
    ServiceInfoFilter _serviceInfoFilter;
    volatile int64_t _refreshTimestamp;
    volatile int64_t _scheduleTimestamp;
    volatile int64_t _lastSyncScheduleTs;

    util::SharedPtrGuard<GlobalAgentMaintainer> _globalAgentMaintainerGuard;
    autil::ThreadMutex _updateGlobalAgentConfigLock;
    std::string _httpAddress;
    std::string _hippoZkRoot;
    std::string _kmonServiceName;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ServiceKeeper);
///////////////////////////////////////////////////////////////
template <typename RequestType>
proto::BuildId ServiceKeeper::getBuildId(const RequestType* request) const
{
    if (request->has_buildid()) {
        return request->buildid();
    }
    proto::BuildId emptyBuildId;
    GenerationKeepers keepers;
    {
        autil::ScopedLock lock(_mapLock);
        keepers = _allGenerationKeepers;
    }
    if (request->has_jobid()) {
        std::string jobId = request->jobid();
        for (GenerationKeepers::const_iterator it = keepers.begin(); it != keepers.end(); ++it) {
            if (isMatchedBuildId(jobId, it->first)) {
                return it->first;
            }
        }
    }
    return emptyBuildId;
}

}} // namespace build_service::admin

#endif // ISEARCH_BS_SERVICEKEEPER_H
