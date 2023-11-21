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
#include <set>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/BrokerDecisionMaker.h"
#include "swift/admin/ErrorHandler.h"
#include "swift/admin/TopicInStatusManager.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/WorkerManager.h"
#include "swift/admin/WorkerTable.h"
#include "swift/common/Common.h"
#include "swift/config/ConfigVersionManager.h"
#include "swift/heartbeat/HeartbeatMonitor.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "worker_framework/LeaderChecker.h"

namespace swift {
namespace admin {
class ModuleManager;
} // namespace admin

namespace config {
class AdminConfig;
} // namespace config
namespace monitor {
class AdminMetricsReporter;
} // namespace monitor
namespace protocol {
class BrokerStatusRequest;
class BrokerStatusResponse;
class HeartbeatInfo;
} // namespace protocol
namespace auth {
class RequestAuthenticator;

typedef std::shared_ptr<RequestAuthenticator> RequestAuthenticatorPtr;
} // namespace auth
} // namespace swift

namespace swift {
namespace admin {

class SysController {
public:
    typedef worker_framework::LeaderChecker LeaderChecker;

public:
    SysController(config::AdminConfig *config, monitor::AdminMetricsReporter *reporter);
    virtual ~SysController();

private:
    SysController(const SysController &);
    SysController &operator=(const SysController &);

public:
    bool init();
    bool start(cm_basic::ZkWrapper *zkWrapper, LeaderChecker *leaderChecker);
    bool turnToLeader(const std::string &address, const std::string &httpAddress);
    void turnToFollower();
    void stop();
    bool isStopped() const;

public: // arpc service methods
    void createTopic(const protocol::TopicCreationRequest *request, protocol::TopicCreationResponse *response);
    void createTopicBatch(const protocol::TopicBatchCreationRequest *request,
                          protocol::TopicBatchCreationResponse *response);
    void modifyTopic(const protocol::TopicCreationRequest *request, protocol::TopicCreationResponse *response);
    void deleteTopic(const protocol::TopicDeletionRequest *request, protocol::TopicDeletionResponse *response);
    void deleteTopicBatch(const protocol::TopicBatchDeletionRequest *request,
                          protocol::TopicBatchDeletionResponse *response);
    void getSysInfo(const protocol::EmptyRequest *request, protocol::SysInfoResponse *response);
    void getTopicInfo(const protocol::TopicInfoRequest *request, protocol::TopicInfoResponse *response);
    void getAllTopicInfo(const protocol::EmptyRequest *request, protocol::AllTopicInfoResponse *response);
    void getAllTopicSimpleInfo(const protocol::EmptyRequest *request, protocol::AllTopicSimpleInfoResponse *response);
    void getAllTopicName(const protocol::EmptyRequest *request, protocol::TopicNameResponse *response);
    void getPartitionInfo(const protocol::PartitionInfoRequest *request, protocol::PartitionInfoResponse *response);
    void getRoleAddress(const protocol::RoleAddressRequest *request, protocol::RoleAddressResponse *response);
    void getLeaderInfo(const protocol::LeaderInfoRequest *request, protocol::LeaderInfoResponse *response);
    void getAllWorkerStatus(const protocol::EmptyRequest *request, protocol::WorkerStatusResponse *response);
    void getPartitionError(const protocol::ErrorRequest *request, protocol::PartitionErrorResponse *response);
    void getWorkerError(const protocol::ErrorRequest *request, protocol::WorkerErrorResponse *response);
    void updateBrokerConfig(const protocol::UpdateBrokerConfigRequest *request,
                            protocol::UpdateBrokerConfigResponse *response);
    void rollbackBrokerConfig(const protocol::RollbackBrokerConfigRequest *request,
                              protocol::RollbackBrokerConfigResponse *response);
    void transferPartition(const protocol::PartitionTransferRequest *request,
                           protocol::PartitionTransferResponse *response);
    void topicAclManage(const protocol::TopicAclRequest *request, protocol::TopicAclResponse *response);
    // virtual for test mock
    virtual void changeSlot(const protocol::ChangeSlotRequest *request, protocol::ChangeSlotResponse *response);
    void registerSchema(const protocol::RegisterSchemaRequest *request, protocol::RegisterSchemaResponse *response);
    void getSchema(const protocol::GetSchemaRequest *request, protocol::GetSchemaResponse *response);
    void reportBrokerStatus(const protocol::BrokerStatusRequest *request, protocol::BrokerStatusResponse *response);
    void getBrokerStatus(const protocol::GetBrokerStatusRequest *request, protocol::GetBrokerStatusResponse *response);
    void getTopicRWTime(const protocol::GetTopicRWTimeRequest *request, protocol::GetTopicRWTimeResponse *response);
    void reportMissTopic(const protocol::MissTopicRequest *request, protocol::MissTopicResponse *response);
    void getLastDeletedNoUseTopic(const protocol::EmptyRequest *request,
                                  protocol::LastDeletedNoUseTopicResponse *response);
    void getDeletedNoUseTopic(const protocol::DeletedNoUseTopicRequest *request,
                              protocol::DeletedNoUseTopicResponse *response);
    void getDeletedNoUseTopicFiles(const protocol::EmptyRequest *request,
                                   protocol::DeletedNoUseTopicFilesResponse *response);
    void turnToMaster(const protocol::TurnToMasterRequest *request, protocol::TurnToMasterResponse *response);
    void turnToSlave(const protocol::TurnToSlaveRequest *request, protocol::TurnToSlaveResponse *response);
    void getMasterInfo(const protocol::EmptyRequest *request, protocol::MasterInfoResponse *response);
    void updateWriterVersion(const protocol::UpdateWriterVersionRequest *request,
                             protocol::UpdateWriterVersionResponse *response);
    TopicTable *getTopicTable() { return &_topicTable; }
    WorkerTable *getWorkerTable() { return &_workerTable; }
    monitor::AdminMetricsReporter *getMetricsReporter() { return _reporter; }
    AdminZkDataAccessorPtr getAdminZkDataAccessor() { return _zkDataAccessor; }
    TopicInStatusManager *getTopicInStatusManager() { return &_topicInStatusManager; }
    uint64_t getMasterVersion() { return _selfMasterVersion; }
    auth::RequestAuthenticatorPtr getRequestAuthenticator();
    // public for module invoke, TODO refactor
public:
    bool isMaster() const;
    void syncMetaInfo();
    void removeOldData();
    void changeTopicPartCnt();
    void syncLeaderInfo();
    void deleteExpiredTopic();
    void doExecute();
    protocol::TopicBatchDeletionResponse doDeleteTopicBatch(const std::set<std::string> &delTopics, bool deleteData);
    bool loadMetaInfo();
    void reportMetrics();

private:
    void updatePartitionBufferSize(protocol::TopicCreationRequest &meta);
    void updateCreateTime(protocol::TopicCreationRequest &meta);
    void resetDeleteTopicDataFlag(protocol::TopicCreationRequest &meta);

private: // system control
    bool updateDfsRoot(protocol::TopicMetas &topicMetas);
    bool loadTopicInfo(bool isFollower = false);
    bool loadTopicSchema();
    bool loadChangePartCntTask();
    bool loadMasterVersion();
    bool initHeartbeatMonitor();
    void execute();
    void updateTopicStatus(TopicMap &topics, TopicMap &logicTopics);
    bool updateOneTopicStatus(TopicInfo *topicInfo, protocol::TopicPartitionInfo *topicPartitionInfo);
    void updateBrokerWorkerStatus(WorkerMap &workers);
    // virtual for test
    virtual void updateBrokerWorkerStatusForEmptyTarget(WorkerMap &workers);
    void doTransferPartition();
    bool loadBrokerVersionInfos();
    void allocBrokerTasks(const config::ConfigVersionManager &versionManager);
    void releaseDeadWorkers(WorkerMap &workers);
    bool syncMasterVersion();
    bool doDiffTopics(const TopicMap &topicMap,
                      const protocol::AllTopicInfoResponse &allTopicInfoResponse,
                      protocol::TopicBatchCreationRequest &newTopics,
                      protocol::TopicBatchDeletionRequest &deletedTopics);
    bool topicInfoChanged(const protocol::TopicInfo &tinfo, const protocol::TopicCreationRequest &meta);

private: // response construct
    void constructResponseByRoleStatus(const protocol::RoleType roleType,
                                       const protocol::RoleStatus roleStatus,
                                       const WorkerMap &allWorker,
                                       protocol::RoleAddressResponse *response);
    void constructAddressGroup(const protocol::RoleType roleType,
                               const protocol::RoleStatus roleStatus,
                               const WorkerMap &addressMap,
                               protocol::AddressGroup *addressGroup);
    void fillStatus(const WorkerMap &worker, protocol::WorkerStatusResponse &response) const;

    template <typename T>
    void fillTopicInfo(TopicInfoPtr topicInfoPtr, protocol::TopicInfo *ti, const T *request);
    void fillTopicMeta(const protocol::TopicInfo &tinfo, protocol::TopicCreationRequest &meta);
    void fillTopicSimpleInfo(TopicInfoPtr topicInfoPtr, protocol::TopicSimpleInfo *ti);
    void doGetPartitionInfo(const protocol::PartitionInfoRequest *request,
                            TopicInfoPtr &topicInfoPtr,
                            protocol::PartitionInfoResponse *response);
    void fillTaskInfo(protocol::TaskInfo *ti, protocol::TopicCreationRequest &topicMeta);
    bool clearOldTopicData(const std::string &topicName);
    template <typename T>
    void handleError(T *response, protocol::ErrorCode ec, const std::string &msgStr = "");
    protocol::ErrorCode adjustTopicParams(const protocol::TopicCreationRequest *request,
                                          protocol::TopicCreationRequest &retMeta);
    bool checkTopicExist(const protocol::TopicCreationRequest *request);
    bool checkSameTopic(const protocol::TopicBatchCreationRequest *request, std::string &sameTopicName);

private:
    void updateSysStatus();
    void setPrimary(bool isPrimary);
    bool isPrimary() const;
    void setMaster(bool isMaster);

private:
    void toMasterLeader();
    void toSlaveLeader();
    void toMasterFollower();
    void toSlaveFollower();

private: // leader
    bool getLeaderStatus() const;
    void setLeaderStatus(bool isLeader);
    bool initSyncLeaderInfoThread();
    bool readLeaderInfo(const std::string &path, bool isJson, protocol::LeaderInfo &leaderInfo);
    // virtual for test
    virtual bool checkLeaderInfoDiff(const std::string &path, bool isJson = false);
    void writeLeaderInfo(const std::string &path, const std::string &content);
    bool updateMasterStatus(const std::string &inlineFilePath, bool needUpdateInline = true);
    void
    writeLeaderInfoWithInline(const std::string &filePath, const std::string &inlinePath, const std::string &content);

private: // heart beat
    void receiveHeartbeat(const protocol::HeartbeatInfo &heartbeatInfo);
    void setHeartbeatMonitorStatus(cm_basic::ZkWrapper::ZkStatus status);
    bool getHeartbeatMonitorStatus() const;
    void updateHeartBeatStatus(cm_basic::ZkWrapper::ZkStatus status);

private:
    void clearWorkerTask(const std::string &workerAddress);
    WorkerMap filterWorkers(const WorkerMap &worker, bool alive);

private:
    bool initBrokerConfig();
    void updateAppPlan(const config::ConfigVersionManager &versionManager);
    void prepareWorkerTable(const config::ConfigVersionManager &versionManager);
    bool finishUpgrade(const config::ConfigVersionManager &versionManager);
    void makeDecision(const config::ConfigVersionManager &versionManager,
                      const protocol::RoleType roleType,
                      TopicMap &topicMap,
                      WorkerMap &brokerWorkers);
    bool isUpdateVersionFinish(const protocol::RoleType &roleType, const config::ConfigVersionManager &versionManager);
    WorkerInfo::TaskSet getAllCurrentPartition(const WorkerMap &worker);
    WorkerInfo::TaskMap getAllCurrentPartitionWithAddress(const WorkerMap &worker);
    WorkerInfo::TaskSet getNotTimeoutCurrentPartition(const WorkerMap &worker, int64_t timeout);
    WorkerInfo::TaskSet getAllTargetPartition(const WorkerMap &worker);
    WorkerMap filterWorkerByVersion(const WorkerMap &worker, const std::string &version);
    void getRoleNames(const config::ConfigVersionManager &versionManager,
                      const protocol::RoleType &roleType,
                      std::vector<std::string> &roleNames);
    bool isRoleEnoughToMakeDecision(const std::string &configPath,
                                    const std::string &roleVersion,
                                    WorkerMap &aliveBrokerWorkers);
    bool canBrokerMakeDecision(const config::ConfigVersionManager &versionManager, WorkerMap &aliveBrokerWorkers);
    void updatePartitionResource(TopicMap &topicMap);
    void adjustPartitionLimit(TopicMap &topicMap, const WorkerMap &aliveBrokerWorkers);

private:
    virtual bool doUpdateLeaderInfo(const std::vector<protocol::AdminInfo> &currentAdminInfoVec);
    void updateLeaderInfo();
    void publishLeaderInfo();
    bool leaderInfoChanged(const std::vector<protocol::AdminInfo> &currentAdminInfoVec);
    bool forceSyncLeaderInfo();

private:
    void removeOldZkData();
    void removeOldTopicData();
    void doRemoveOldTopicData(const std::string &dfsRoot, const std::string &topicName, double reserveDataByHour);
    void removeOldHealthCheckData();
    bool doRemoveOldHealthCheckData(const std::string &path,
                                    const std::string &currentVersion,
                                    const std::string &targetVersion);
    void removeOldWriterVersionData();

    bool getTopicLastModifyTime(const std::string &topicName, const std::string dfsRoot, uint64_t &lastModifyTime);
    bool canSealedTopicModify(const protocol::TopicCreationRequest &meta,
                              const protocol::TopicCreationRequest *request) const;
    void getCurrentAndTargetVersion(std::string &brokerCurrent, std::string &brokerTarget);
    void doRemoveOldZkData(const std::vector<std::string> &paths,
                           const protocol::RoleType &roleType,
                           const std::string &currentVersion,
                           const std::string &targetVersion);
    bool backTopicMetas();
    void updateTopicRWTime();
    void deleteObsoleteMetaFiles(const std::string &filePath, uint32_t reserveCount);
    bool checkLogicTopicModify(const protocol::TopicCreationRequest &target,
                               const protocol::TopicCreationRequest &current);
    void updateSealTopicStatus(std::unordered_set<std::string> &sealedTopicSet);
    void updateLoadedTopic(std::unordered_set<std::string> &topicSet);

    protocol::ErrorCode getPhysicMetaFromLogic(const protocol::TopicCreationRequest &logicMeta,
                                               int32_t physicIdx,
                                               protocol::TopicCreationRequest &physicMeta);
    protocol::ErrorCode addPhysicMetasForLogicTopic(protocol::TopicBatchCreationRequest &topicMetas,
                                                    protocol::TopicBatchCreationResponse *response);
    bool canDeal(const std::vector<std::string> &workers);
    void removeCleanAtDeleteTopicData();
    bool initModuleManager();
    bool initAdminStatusManager();
    bool statusUpdateHandler(Status lastStatus, Status currentStatus);
    template <typename ModuleClass>
    std::shared_ptr<ModuleClass> getModule();
    void fillTopicOpControl(protocol::TopicInfoResponse *response, const protocol::TopicAccessInfo &accessInfo) const;

private:
    config::AdminConfig *_adminConfig;
    AdminZkDataAccessorPtr _zkDataAccessor;
    heartbeat::HeartbeatMonitor _heartbeatMonitor;
    BrokerDecisionMaker _brokerDecisionMaker;
    TopicTable _topicTable;
    WorkerTable _workerTable;
    WorkerManagerPtr _workerManager;
    ErrorHandler _errorHandler;
    autil::ThreadPtr _transitionThread;

    autil::ThreadMutex _lock; // for sys
    autil::ThreadMutex _syncTopicLock;
    autil::ThreadCond _mainCond; // for main thread

    bool _goodLeader;
    mutable autil::ThreadMutex _leaderStatusLock;
    bool _goodHeartbeat;
    mutable autil::ThreadMutex _heartBeatStatusLock;

    volatile bool _started;
    volatile bool _stop;
    volatile bool _isStopped;

    autil::ThreadMutex _leaderInfoLock;
    protocol::LeaderInfo _leaderInfo;

    monitor::AdminMetricsReporter *_reporter;
    config::ConfigVersionManager _versionManager;
    std::map<std::string, float> _adjustWorkerResourceMap;
    int64_t _adjustBeginTime;
    bool _clearCurrentTask;
    std::string _transferGroupName;
    std::map<std::string, protocol::TopicInfoResponse> _topicInfoResponseCache;
    std::map<std::pair<std::string, uint32_t>, protocol::PartitionInfoResponse> _partInfoResponseCache;
    std::map<std::string, int64_t> _deletedTopicMap;
    autil::ThreadMutex _deletedTopicMapLock;
    uint32_t _obsoleteMetaDeleteFrequency;
    std::string _latestMeta;
    TopicInStatusManager _topicInStatusManager;

    bool _switchCanStart;
    std::atomic<uint64_t> _selfMasterVersion;
    int64_t _forceSyncLeaderInfoTimestamp;
    AdminStatusManager _adminStatusManager;
    std::unique_ptr<ModuleManager> _moduleManager;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SysController);

} // namespace admin
} // namespace swift
