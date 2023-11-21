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
#include "swift/admin/SysController.h"

#include <algorithm>
#include <assert.h>
#include <cmath>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stddef.h>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/bitmap.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/result/Result.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "master_framework/AppPlan.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "swift/admin/AdminRequestChecker.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/BrokerDecisionMaker.h"
#include "swift/admin/DecisionMaker.h"
#include "swift/admin/ErrorHandler.h"
#include "swift/admin/ModuleManager.h"
#include "swift/admin/RoleInfo.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/WorkerManager.h"
#include "swift/admin/WorkerManagerLocal.h"
#include "swift/admin/WorkerTable.h"
#include "swift/admin/modules/CleanDataModule.h"
#include "swift/admin/modules/MultiThreadTaskDispatcherModule.h"
#include "swift/admin/modules/NoUseTopicModule.h"
#include "swift/admin/modules/TopicAclManageModule.h"
#include "swift/admin/modules/TopicDataModule.h"
#include "swift/admin/modules/WorkerStatusModule.h"
#include "swift/auth/RequestAuthenticator.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/ConfigVersionManager.h"
#include "swift/heartbeat/HeartbeatMonitor.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/util/LogicTopicHelper.h"
#include "swift/util/PanguInlineFileUtil.h"
#include "swift/util/ProtoUtil.h"
#include "swift/util/TargetRecorder.h"

using namespace swift::util;
using namespace swift::auth;
using namespace swift::common;
using namespace swift::protocol;
using namespace swift::config;
using namespace swift::admin;
using namespace swift::network;

using namespace std;
using namespace cm_basic;
using namespace autil;
using namespace autil::legacy;
using namespace fslib::fs;
using namespace master_framework::simple_master;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, SysController);

const int32_t MAX_ALLOW_SCHEMA_NUM = 10;
const string SRC_API = "api";

#define CHECK_IS_LEADER(response)                                                                                      \
    do {                                                                                                               \
        if (!isPrimary()) {                                                                                            \
            ErrorInfo *ei = response->mutable_errorinfo();                                                             \
            ei->set_errcode(ERROR_ADMIN_NOT_LEADER);                                                                   \
            ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                             \
            return;                                                                                                    \
        }                                                                                                              \
    } while (0);

#define SET_OK(resource)                                                                                               \
    do {                                                                                                               \
        ErrorInfo *ei = response->mutable_errorinfo();                                                                 \
        ei->set_errcode(ERROR_NONE);                                                                                   \
        ei->set_errmsg(ErrorCode_Name(ei->errcode()));                                                                 \
    } while (0);

SysController::SysController(config::AdminConfig *config, monitor::AdminMetricsReporter *reporter)
    : _adminConfig(config)
    , _goodLeader(false)
    , _goodHeartbeat(false)
    , _started(false)
    , _stop(false)
    , _isStopped(true)
    , _reporter(reporter)
    , _adjustBeginTime(0)
    , _clearCurrentTask(false)
    , _obsoleteMetaDeleteFrequency(0)
    , _switchCanStart(false)
    , _selfMasterVersion(0)
    , _forceSyncLeaderInfoTimestamp(-1) {}

SysController::~SysController() {
    _workerManager.reset();
    _zkDataAccessor.reset();
}

bool SysController::init() {
    if (_adminConfig == nullptr) {
        return false;
    }
    setMaster(!_adminConfig->enableBackup());
    if (_adminConfig->enableBackup() && !_adminConfig->enableFastRecover()) {
        AUTIL_LOG(ERROR, "fast recover config error, backup mode depends on fast recover");
        return false;
    }
    _zkDataAccessor = std::make_shared<AdminZkDataAccessor>();
    _zkDataAccessor->setRecordLocalFile(_adminConfig->getRecordLocalFile());
    _workerTable.setDataAccessor(_zkDataAccessor);
    _workerTable.setBrokerUnknownTimeout(_adminConfig->getBrokerUnknownTimeout());
    if (!_adminConfig->isLocalMode()) {
        PanguInlineFileUtil::setInlineFileMock(false);
        _workerManager.reset(new WorkerManager());
        AUTIL_LOG(INFO, "worker manage use normal mode");
    } else {
        PanguInlineFileUtil::setInlineFileMock(true);
        _workerManager.reset(new WorkerManagerLocal(
            _adminConfig->getWorkDir(), _adminConfig->getLogConfFile(), _adminConfig->getMaxRestartCountInLocal()));
        AUTIL_LOG(INFO, "worker manage use local mode");
    }
    return true;
}

bool SysController::start(ZkWrapper *zkWrapper, LeaderChecker *leaderChecker) {
    ScopedLock lock(_lock);
    if (_started) {
        return true;
    }
    _started = true;
    _stop = false;
    _isStopped = false;
    if (!_zkDataAccessor->init(zkWrapper, _adminConfig->getZkRoot())) {
        AUTIL_LOG(ERROR, "Failed to init zk data accessor");
        return false;
    }
    if (!_workerManager->init(_adminConfig->getHippoRoot(), _adminConfig->getApplicationId(), leaderChecker)) {
        return false;
    }
    if (!initModuleManager()) {
        AUTIL_LOG(ERROR, "init module manager failed.");
        return false;
    }
    if (!initAdminStatusManager()) {
        AUTIL_LOG(ERROR, "init admin status manager failed.");
        return false;
    }
    return true;
}

bool SysController::turnToLeader(const string &address, const string &httpAddress) {
    LeaderInfo li;
    li.set_address(address);
    li.set_httpaddress(httpAddress);
    li.set_starttimesec(TimeUtility::currentTimeInSeconds());
    li.set_sysstop(false);
    // TODO: remove LeaderInfo, use leader election info to get admin addr
    if (!_zkDataAccessor->setLeaderInfo(li)) {
        return false;
    }
    {
        ScopedLock lock(_leaderInfoLock);
        _leaderInfo = li;
    }
    _zkDataAccessor->addLeaderInfoToHistory(li);
    setLeaderStatus(true);
    updateSysStatus();
    return initHeartbeatMonitor();
}

void SysController::turnToFollower() {
    setLeaderStatus(false);
    updateSysStatus();
}

void SysController::stop() {
    AUTIL_LOG(INFO, "sysController stop");
    if (_isStopped && _workerManager == NULL) {
        return;
    }
    setPrimary(false);
    {
        ScopedLock lock(_mainCond);
        _stop = true;
        _mainCond.broadcast();
    }
    _adminStatusManager.stop();
    _heartbeatMonitor.stop();
    if (_moduleManager) {
        _moduleManager->stop();
        _moduleManager.reset();
    }
    _workerManager.reset(); // need delete SimpleMasterScheduler in worker manager for leader checker will be deleted
    _isStopped = true;
}

bool SysController::isStopped() const { return _isStopped; }

void SysController::syncMetaInfo() {
    if (isPrimary()) {
        backTopicMetas();
        updateTopicRWTime();
        return;
    }
    setPrimary(false);
    ScopedLock lock(_lock);
    if (!loadTopicInfo(true)) {
        AUTIL_LOG(WARN, "sync topic info failed!");
    }
    if (!loadTopicSchema()) {
        AUTIL_LOG(WARN, "sync topic schema failed!");
    }
    if (!loadBrokerVersionInfos()) {
        AUTIL_LOG(WARN, "sync broker versioninfo failed!");
    }
    auto noUseTopicModule = getModule<NoUseTopicModule>();
    if (noUseTopicModule && !noUseTopicModule->loadLastDeletedNoUseTopics()) {
        AUTIL_LOG(WARN, "sync deleted not use topic failed!");
    }
    if (!syncMasterVersion()) {
        AUTIL_LOG(WARN, "update master version failed!");
    }
}

bool SysController::updateDfsRoot(protocol::TopicMetas &topicMetas) {
    bool updated = false;
    const string &dfsRoot = _adminConfig->getDfsRoot();
    const string &extendDfsRoot = _adminConfig->getExtendDfsRoot();
    const string &toDeleteDfsRoot = _adminConfig->getTodelDfsRoot();
    int64_t topicCount = topicMetas.topicmetas_size();
    for (int64_t i = 0; i < topicCount; i++) {
        TopicCreationRequest *meta = topicMetas.mutable_topicmetas(i);
        if (!extendDfsRoot.empty()) {
            if (extendDfsRoot == meta->dfsroot() ||
                (meta->dfsroot().empty() && meta->topicmode() != TOPIC_MODE_MEMORY_ONLY)) {
                AUTIL_LOG(INFO, "[%s] set dfsroot empty", meta->topicname().c_str());
                meta->set_dfsroot("");
                updated = true;
                bool found = false;
                for (int64_t idx = 0; idx < meta->extenddfsroot_size(); ++idx) {
                    if (extendDfsRoot == meta->extenddfsroot(idx)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    AUTIL_LOG(INFO, "[%s] add extendDfsRoot [%s]", meta->topicname().c_str(), extendDfsRoot.c_str())
                    *meta->add_extenddfsroot() = extendDfsRoot;
                }
            }
        }

        if (!toDeleteDfsRoot.empty()) {
            if (toDeleteDfsRoot == meta->dfsroot()) {
                AUTIL_LOG(INFO, "[%s] set dfsroot empty", meta->topicname().c_str());
                meta->set_dfsroot("");
                updated = true;
            }
            if (0 != meta->extenddfsroot_size()) {
                vector<string> extendDfsRootResult;
                for (int64_t idx = 0; idx < meta->extenddfsroot_size(); ++idx) {
                    if (toDeleteDfsRoot != meta->extenddfsroot(idx)) {
                        extendDfsRootResult.push_back(meta->extenddfsroot(idx));
                    } else {
                        AUTIL_LOG(INFO,
                                  "[%s] delete extendDfsRoot [%s]",
                                  meta->topicname().c_str(),
                                  meta->extenddfsroot(idx).c_str());
                    }
                }
                if (extendDfsRootResult.size() != size_t(meta->extenddfsroot_size())) {
                    updated = true;
                    meta->clear_extenddfsroot();
                    for (size_t idx = 0; idx < extendDfsRootResult.size(); ++idx) {
                        *meta->add_extenddfsroot() = extendDfsRootResult[idx];
                    }
                }
            }
        }

        if (!dfsRoot.empty()) {
            if (meta->dfsroot().empty() && meta->topicmode() != TOPIC_MODE_MEMORY_ONLY) {
                AUTIL_LOG(INFO,
                          "[%s] set dfsroot from [%s] to [%s]",
                          meta->topicname().c_str(),
                          meta->dfsroot().c_str(),
                          dfsRoot.c_str());
                meta->set_dfsroot(dfsRoot);
                updated = true;
            }
        }

        // check same
        if (!meta->dfsroot().empty() && meta->extenddfsroot_size() > 0) {
            vector<string> extendDfsRootResult;
            for (int64_t idx = 0; idx < meta->extenddfsroot_size(); ++idx) {
                if (meta->dfsroot() != meta->extenddfsroot(idx)) {
                    extendDfsRootResult.push_back(meta->extenddfsroot(idx));
                } else {
                    AUTIL_LOG(INFO,
                              "[%s] delete extendDfsRoot [%s], same with dfsroot",
                              meta->topicname().c_str(),
                              meta->dfsroot().c_str());
                }
            }
            if (extendDfsRootResult.size() != size_t(meta->extenddfsroot_size())) {
                updated = true;
                meta->clear_extenddfsroot();
                for (size_t idx = 0; idx < extendDfsRootResult.size(); ++idx) {
                    *meta->add_extenddfsroot() = extendDfsRootResult[idx];
                }
            }
        }
    }
    return updated;
}

bool SysController::loadTopicInfo(bool isFollower) {
    protocol::TopicMetas topicMetas;
    protocol::TopicPartitionInfos topicPartitionInfos;
    if (!_zkDataAccessor->loadTopicInfos(topicMetas, topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "deserialize topicMeta  failed");
        if (_reporter) {
            _reporter->incLoadTopicFailed();
        }
        return false;
    }
    if (!isFollower) {
        if (updateDfsRoot(topicMetas)) {
            if (!_zkDataAccessor->writeTopicMetas(topicMetas)) {
                AUTIL_LOG(WARN, "write topic metas faild.");
                return false;
            } else {
                _zkDataAccessor->setTopicMetas(topicMetas);
            }
        }
    }
    ScopedLock lock(_syncTopicLock);
    _topicTable.clear();
    _topicInfoResponseCache.clear();
    _partInfoResponseCache.clear();
    int topicCount = topicMetas.topicmetas_size();
    int topicPartInfoCount = topicPartitionInfos.topicpartitioninfos_size();
    bool needRecover = false;
    if (topicCount != topicPartInfoCount) {
        AUTIL_LOG(WARN, "meta count[%d] does not equal topic partition count[%d]", topicCount, topicPartInfoCount);
        for (int i = 0; i < topicCount; i++) {
            TopicCreationRequest meta = topicMetas.topicmetas(i);
            updatePartitionBufferSize(meta);
            updateCreateTime(meta);
            resetDeleteTopicDataFlag(meta);
            _topicTable.addTopic(&meta);
        }
        needRecover = true;
        if (isFollower) {
            for (int i = 0; i < topicPartInfoCount; i++) {
                TopicPartitionInfo topicPartInfo = topicPartitionInfos.topicpartitioninfos(i);
                string topicName = topicPartInfo.topicname();
                TopicInfoPtr topicInfoPtr = _topicTable.findTopic(topicName);
                if (topicInfoPtr == NULL) {
                    AUTIL_LOG(INFO, "topic [%s] not found in topic table", topicName.c_str());
                    continue;
                }
                if ((int)topicInfoPtr->getPartitionCount() != topicPartInfo.partitioninfos_size()) {
                    AUTIL_LOG(INFO,
                              "topic [%s] partition count [%d] is not equal [%d]",
                              topicName.c_str(),
                              (int)topicInfoPtr->getPartitionCount(),
                              (int)topicPartInfo.partitioninfos_size());
                    continue;
                }
                for (int j = 0; j < topicPartInfo.partitioninfos_size(); ++j) {
                    const PartitionInfo &partInfo = topicPartInfo.partitioninfos(j);
                    string roleAddress = PathDefine::getRoleAddress(partInfo.rolename(), partInfo.brokeraddress());
                    topicInfoPtr->setCurrentRoleAddress(partInfo.id(), roleAddress);
                    topicInfoPtr->setStatus(partInfo.id(), partInfo.status());
                }
            }
        }
    } else {
        for (int i = 0; i < topicCount; i++) {
            TopicCreationRequest meta = topicMetas.topicmetas(i);
            updatePartitionBufferSize(meta);
            updateCreateTime(meta);
            resetDeleteTopicDataFlag(meta);
            _topicTable.addTopic(&meta);
            TopicPartitionInfo topicPartInfo = topicPartitionInfos.topicpartitioninfos(i);
            if (meta.topicname() != topicPartInfo.topicname() ||
                meta.partitioncount() != (uint32_t)topicPartInfo.partitioninfos_size()) {
                AUTIL_LOG(ERROR, "meta and partition info not consistent for topic[%s]", meta.topicname().c_str());
                needRecover = true;
                continue; // should break
            }
            TopicInfoPtr topicInfoPtr = _topicTable.findTopic(meta.topicname());
            for (int j = 0; j < topicPartInfo.partitioninfos_size(); ++j) {
                const PartitionInfo &partInfo = topicPartInfo.partitioninfos(j);
                const string &roleAddress = PathDefine::getRoleAddress(partInfo.rolename(), partInfo.brokeraddress());
                topicInfoPtr->setCurrentRoleAddress(partInfo.id(), roleAddress);
                if (isFollower || TOPIC_TYPE_LOGIC == topicInfoPtr->getTopicType()) {
                    topicInfoPtr->setStatus(partInfo.id(), partInfo.status());
                }
            }
        }
    }
    if (needRecover && !isFollower) { // follow leader needn't recover
        if (!_zkDataAccessor->recoverPartitionInfos(topicMetas, topicPartitionInfos)) {
            AUTIL_LOG(WARN, "recover topic partition infos failed.");
        }
        AUTIL_LOG(INFO, "recover topic partition infos success.");
    }

    TopicRWInfos rwInfos;
    if (!_zkDataAccessor->loadTopicRWTime(rwInfos)) {
        AUTIL_LOG(WARN, "load topic write read time faild");
    } else {
        _topicInStatusManager.updateTopicWriteReadTime(rwInfos);
    }

    AUTIL_LOG(INFO, "Finish loading topic info. count [%d]", topicCount);
    return true;
}

bool SysController::loadTopicSchema() {
    if (!_zkDataAccessor->loadTopicSchemas()) {
        AUTIL_LOG(WARN, "deserialize topic schema failed");
        if (_reporter) {
            _reporter->incLoadSchemaFailed();
        }
        return false;
    }
    AUTIL_LOG(INFO, "Finish loading topic schema");
    return true;
}

bool SysController::loadChangePartCntTask() {
    if (!_zkDataAccessor->loadChangePartCntTask()) {
        AUTIL_LOG(WARN, "load change part count task failed");
        return false;
    }
    return true;
}

bool SysController::loadMasterVersion() {
    // read master_info
    uint64_t zkMasterVersion = 0;
    if (!_zkDataAccessor->readMasterVersion(zkMasterVersion)) {
        AUTIL_LOG(ERROR, "read master version from zk failed");
        return false;
    }
    auto initialMasterVersion = _adminConfig->getInitialMasterVersion();
    if (zkMasterVersion < initialMasterVersion) {
        AUTIL_LOG(INFO,
                  "load initial master version from config, zk version [%lu] config version [%lu]",
                  zkMasterVersion,
                  initialMasterVersion);
        zkMasterVersion = initialMasterVersion;
    }
    _selfMasterVersion = zkMasterVersion;
    return true;
}

bool SysController::syncMasterVersion() {
    if (!_adminConfig->enableBackup()) {
        return true;
    }
    loadMasterVersion();
    const auto &paths = _adminConfig->getSyncAdminInfoPath();
    if (paths.empty()) {
        return false;
    }
    for (const auto &path : paths) {
        auto inlineFilePath = PathDefine::getPublishInlineFile(path);
        updateMasterStatus(inlineFilePath, false);
    }
    return true;
}

void SysController::updatePartitionBufferSize(TopicCreationRequest &meta) {
    // compatible old version
    static const int64_t DEFAULT_MIN_BUFFER_SIZE = 8;
    static const int64_t DEFAULT_MAX_BUFFER_SIZE = 128;
    if (meta.has_partitionbuffersize()) {
        uint32_t partBufferSize = meta.partitionbuffersize();
        if (meta.partitionmaxbuffersize() == DEFAULT_MAX_BUFFER_SIZE &&
            meta.partitionminbuffersize() == DEFAULT_MIN_BUFFER_SIZE) {
            uint32_t partMinBufferSize = (partBufferSize + 1) / 2;
            uint32_t partMaxBufferSize = partBufferSize * 2;
            if (partMinBufferSize < 1) {
                partMinBufferSize = 1;
            }
            meta.set_partitionminbuffersize(partMinBufferSize);
            meta.set_partitionmaxbuffersize(partMaxBufferSize);
        }
    } else {
        uint32_t partMinBufferSize = meta.partitionminbuffersize();
        uint32_t partMaxBufferSize = meta.partitionmaxbuffersize();
        meta.set_partitionbuffersize((partMinBufferSize + partMaxBufferSize) / 2);
    }
}

void SysController::updateCreateTime(TopicCreationRequest &meta) {
    if (!meta.has_createtime() || meta.createtime() == -1) {
        meta.set_createtime(TimeUtility::currentTime());
    }
    if (!meta.has_modifytime() || meta.modifytime() == -1) {
        meta.set_modifytime(meta.createtime());
    }
}

void SysController::resetDeleteTopicDataFlag(TopicCreationRequest &meta) {
    if (meta.deletetopicdata()) {
        meta.set_deletetopicdata(false);
    }
}

bool SysController::initHeartbeatMonitor() {
    const string &path = PathDefine::heartbeatMonitorAddress(_adminConfig->getZkRoot());
    if (!_heartbeatMonitor.setParameter(path)) {
        AUTIL_LOG(ERROR, "Failed to init heartbeat monitor.");
        return false;
    }
    _heartbeatMonitor.setHandler(std::bind(&SysController::receiveHeartbeat, this, std::placeholders::_1));

    _heartbeatMonitor.setStatusChangeHandler(
        std::bind(&SysController::updateHeartBeatStatus, this, std::placeholders::_1));

    if (!_heartbeatMonitor.start(_adminConfig->getSyncHeartbeatInterval())) {
        AUTIL_LOG(ERROR, "Failed to start heartbeat monitor");
        return false;
    }
    AUTIL_LOG(INFO,
              "Finish initializing heartbeat monitor, admin count[%d], broker count[%d]",
              (int)_workerTable.getAdminWorkers().size(),
              (int)_workerTable.getBrokerWorkers().size());
    return true;
}

void SysController::receiveHeartbeat(const HeartbeatInfo &heartbeatInfo) {
    _workerTable.updateWorker(heartbeatInfo);
    _errorHandler.extractError(heartbeatInfo);
}

bool SysController::updateOneTopicStatus(TopicInfo *topicInfo, TopicPartitionInfo *topicPartitionInfo) {
    uint32_t pcount = topicInfo->getPartitionCount();
    assert(pcount == (uint32_t)topicPartitionInfo->partitioninfos_size());
    bool changed = false;
    util::InlineVersion inlineVersion(_selfMasterVersion, TimeUtility::currentTime());
    string inlineString = inlineVersion.serialize();
    string verChgMsg;
    for (uint32_t pid = 0; pid < pcount; pid++) {
        if (!topicInfo->needChange(pid, _adminConfig->enableFastRecover(), _selfMasterVersion)) {
            continue;
        }
        changed = true;
        const string &roleAddress = topicInfo->getTargetRoleAddress(pid);
        string roleName, address;
        PathDefine::parseRoleAddress(roleAddress, roleName, address);
        PartitionInfo *pi = topicPartitionInfo->mutable_partitioninfos(pid);
        pi->set_id(pid);
        pi->set_rolename(roleName);
        pi->set_brokeraddress(address);
        auto status = address.empty() ? PARTITION_STATUS_WAITING : topicInfo->getStatus(pid);
        pi->set_status(status);
        if (_adminConfig->enableFastRecover()) {
            util::InlineVersion tpInlineVersion;
            tpInlineVersion.fromProto(topicInfo->getInlineVersion(pid));
            if (!tpInlineVersion.valid() || topicInfo->brokerChanged(pid)) {
                uint64_t newPartVersion = max(tpInlineVersion.getPartVersion() + 1, inlineVersion.getPartVersion());
                verChgMsg = StringUtil::formatString(", inline version[%lu_%lu->%lu_%lu]",
                                                     tpInlineVersion.getMasterVersion(),
                                                     tpInlineVersion.getPartVersion(),
                                                     _selfMasterVersion.load(),
                                                     newPartVersion);
                if (newPartVersion != inlineVersion.getPartVersion()) {
                    topicInfo->setInlineVersion(pid, util::InlineVersion(_selfMasterVersion, newPartVersion).toProto());
                } else {
                    topicInfo->setInlineVersion(pid, inlineVersion.toProto());
                }
            } else {
                if (_selfMasterVersion != tpInlineVersion.getMasterVersion()) {
                    verChgMsg = StringUtil::formatString(", inline version[%lu_%lu->%lu_%lu]",
                                                         tpInlineVersion.getMasterVersion(),
                                                         tpInlineVersion.getPartVersion(),
                                                         _selfMasterVersion.load(),
                                                         tpInlineVersion.getPartVersion());
                    tpInlineVersion.setMasterVersion(_selfMasterVersion);
                    topicInfo->setInlineVersion(pid, tpInlineVersion.toProto());
                }
            }
        }
        if (address.empty()) {
            topicInfo->setStatus(pid, status);
        }
        topicInfo->setCurrentRoleAddress(pid, roleAddress);
        AUTIL_LOG(INFO,
                  "[%s %u] update broker[%s], status[%d]%s",
                  topicInfo->getTopicName().c_str(),
                  pid,
                  roleAddress.c_str(),
                  status,
                  verChgMsg.c_str());
    }
    return changed;
}

void SysController::updateTopicStatus(TopicMap &topics, TopicMap &logicTopics) {
    AUTIL_LOG(DEBUG, "Start to update topic status");
    if (topics.empty()) {
        return;
    }
    TopicPartitionInfos topicPartitionInfos = _zkDataAccessor->getTopicPartitionInfos();
    bool changed = false;
    for (int i = 0; i < topicPartitionInfos.topicpartitioninfos_size(); i++) {
        TopicPartitionInfo *topicPartitionInfo = topicPartitionInfos.mutable_topicpartitioninfos(i);
        const string &topicName = topicPartitionInfo->topicname();
        TopicMap::iterator it = topics.find(topicName);
        if (it == topics.end()) {
            if (logicTopics.find(topicName) == logicTopics.end()) {
                AUTIL_LOG(WARN, "topic[%s] does not exist in topic table!", topicName.c_str());
            }
            continue;
        }
        if (it->second->getPartitionCount() != (uint32_t)topicPartitionInfo->partitioninfos_size()) {
            AUTIL_LOG(WARN,
                      "Topic[%s] memory partition count[%d] != meta partition count[%d]",
                      topicName.c_str(),
                      (int)it->second->getPartitionCount(),
                      (int)topicPartitionInfo->partitioninfos_size());
            continue;
        }
        bool ret = updateOneTopicStatus(it->second.get(), topicPartitionInfo);
        changed = changed || ret;
    }

    if (!changed) {
        return;
    }
    AUTIL_LOG(INFO, "update partition status, partiton size[%d]", topicPartitionInfos.topicpartitioninfos_size());
    if (!_zkDataAccessor->setTopicPartitionInfos(topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "serialize topic partition info failed");
        return;
    }
}

void SysController::clearWorkerTask(const string &roleName) {
    AUTIL_LOG(DEBUG, "Start to clear worker task[%s]", roleName.c_str());
    DispatchInfo di;
    di.set_rolename(roleName);
    if (!_zkDataAccessor->setDispatchedTask(di)) {
        AUTIL_LOG(ERROR, "Failed to clear dispatch task on broker[%s].", di.rolename().c_str());
    }
}

void SysController::updateBrokerWorkerStatusForEmptyTarget(WorkerMap &workers) {
    vector<pair<WorkerInfoPtr, DispatchInfo>> allDispatchInfos;
    for (auto wIt = workers.begin(); wIt != workers.end(); ++wIt) {
        const WorkerInfoPtr &workerPtr = wIt->second;
        const WorkerInfo::TaskSet &target = workerPtr->getTargetTask();
        if (target.size() != 0) {
            continue;
        }
        if (!workerPtr->needChange(_topicTable, _adminConfig->enableFastRecover())) {
            continue;
        }
        DispatchInfo di;
        di.set_brokeraddress(workerPtr->getRoleInfo().getAddress());
        di.set_rolename(workerPtr->getRoleInfo().roleName);
        di.set_sessionid(workerPtr->getCurrentSessionId());
        if (!_adminConfig->enableMultiThreadDispatchTask()) {
            AUTIL_LOG(INFO, "Dispatch task to %s", ProtoUtil::getDispatchStr(di).c_str());
            if (!_zkDataAccessor->setDispatchedTask(di)) {
                AUTIL_LOG(ERROR, "Failed to dispatch task on broker[%s].", di.rolename().c_str());
            } else {
                workerPtr->updateLastSessionId();
            }
        } else {
            allDispatchInfos.emplace_back(std::make_pair(workerPtr, di));
        }
    }
    auto multiThreadTaskDispatcher = getModule<MultiThreadTaskDispatcherModule>();
    if (multiThreadTaskDispatcher) {
        multiThreadTaskDispatcher->dispatchTasks(allDispatchInfos);
    }
}

void SysController::updateBrokerWorkerStatus(WorkerMap &workers) {
    AUTIL_LOG(DEBUG, "Start to update broker worker status");
    if (!isPrimary()) {
        return;
    }
    updateBrokerWorkerStatusForEmptyTarget(workers); // speedup set empty target to old version broker
    WorkerMap::iterator wIt;
    int64_t timeout = _adminConfig->getForceScheduleTimeSecond() * 1000000;
    const WorkerInfo::TaskSet &allCurrentTasks = getNotTimeoutCurrentPartition(workers, timeout);
    WorkerInfo::TaskMap allCurrentTaskMap = getAllCurrentPartitionWithAddress(workers);
    RoleVersionInfos versionInfos;
    vector<pair<WorkerInfoPtr, DispatchInfo>> allDispatchInfos;
    for (wIt = workers.begin(); wIt != workers.end(); ++wIt) {
        const WorkerInfoPtr &workerPtr = wIt->second;
        RoleVersionInfo roleVersionInfo;
        roleVersionInfo.set_rolename(wIt->first);
        *roleVersionInfo.mutable_versioninfo() = workerPtr->getBrokerVersionInfo();
        *versionInfos.add_versioninfos() = roleVersionInfo;
        if (!workerPtr->needChange(_topicTable, _adminConfig->enableFastRecover())) {
            continue;
        }
        const WorkerInfo::TaskSet &target = workerPtr->getTargetTask();
        WorkerInfo::TaskSet otherCurrentTasks = allCurrentTasks;
        const WorkerInfo::TaskSet &curTasks = workerPtr->getCurrentTask();
        WorkerInfo::TaskSet::const_iterator tIt;
        for (tIt = curTasks.begin(); tIt != curTasks.end(); tIt++) {
            otherCurrentTasks.erase(*tIt);
        }
        DispatchInfo di;
        di.set_brokeraddress(workerPtr->getRoleInfo().getAddress());
        di.set_rolename(workerPtr->getRoleInfo().roleName);
        di.set_sessionid(workerPtr->getCurrentSessionId());
        const string &targetRoleAddress = workerPtr->getTargetRoleAddress();

        for (tIt = target.begin(); tIt != target.end(); ++tIt) {
            // check if broker address same as topic map.
            TopicInfoPtr tip = _topicTable.findTopic(tIt->first);
            if (!tip) {
                AUTIL_LOG(ERROR, "Failed to find topic %s", tIt->first.c_str());
                continue;
            }
            const string &roleAddress = tip->getCurrentRoleAddress(tIt->second);
            if (roleAddress != targetRoleAddress) {
                AUTIL_LOG(DEBUG,
                          "topic [%s, %u] current role address [%s], "
                          "target role address[%s]",
                          tIt->first.c_str(),
                          tIt->second,
                          roleAddress.c_str(),
                          targetRoleAddress.c_str());
                continue;
            }
            // unload partition before load
            if (otherCurrentTasks.count(*tIt) > 0) {
                AUTIL_LOG(WARN,
                          "topic [%s, %u] still load by other brokers, address[%s]",
                          tIt->first.c_str(),
                          tIt->second,
                          allCurrentTaskMap[*tIt].c_str());
                continue;
            }
            TaskInfo *ti = di.add_taskinfos();
            uint32_t pid = tIt->second;
            PartitionId *partId = ti->mutable_partitionid();
            uint32_t from, to;
            partId->set_topicname(tIt->first);
            partId->set_id(pid);
            partId->set_topicgroup(tip->getTopicGroup());
            partId->set_rangecount(tip->getRangeCountInPartition());
            partId->set_partitioncount(tip->getPartitionCount());
            partId->set_version(tip->getModifyTime());
            auto *inlineVersion = partId->mutable_inlineversion();
            *inlineVersion = tip->getInlineVersion(pid);
            if (tip->getRangeInfo(pid, from, to)) {
                partId->set_from(from);
                partId->set_to(to);
            }
            protocol::TopicCreationRequest topicMeta = tip->getTopicMeta();
            fillTaskInfo(ti, topicMeta);
        }

        if (!_adminConfig->enableMultiThreadDispatchTask()) {
            AUTIL_LOG(INFO, "Dispatch broker task to %s", ProtoUtil::getDispatchStr(di).c_str());
            if (!_zkDataAccessor->setDispatchedTask(di)) {
                AUTIL_LOG(ERROR, "Failed to dispatch task on broker[%s].", di.rolename().c_str());
            } else {
                workerPtr->updateLastSessionId();
            }
        } else {
            allDispatchInfos.emplace_back(std::make_pair(workerPtr, di));
        }
    }
    auto multiThreadTaskDispatcher = getModule<MultiThreadTaskDispatcherModule>();
    if (multiThreadTaskDispatcher) {
        multiThreadTaskDispatcher->dispatchTasks(allDispatchInfos);
    }
    if (!_zkDataAccessor->setBrokerVersionInfos(versionInfos)) {
        AUTIL_LOG(WARN, "Failed to write broker versioninfos.");
    }
}

void SysController::fillTaskInfo(TaskInfo *ti, TopicCreationRequest &topicMeta) {
    ti->set_minbuffersize(topicMeta.partitionminbuffersize());
    ti->set_maxbuffersize(topicMeta.partitionmaxbuffersize());
    ti->set_topicmode(topicMeta.topicmode());
    ti->set_needfieldfilter(topicMeta.needfieldfilter());
    ti->set_obsoletefiletimeinterval(topicMeta.obsoletefiletimeinterval());
    ti->set_reservedfilecount(topicMeta.reservedfilecount());
    ti->set_maxwaittimeforsecuritycommit(topicMeta.maxwaittimeforsecuritycommit());
    ti->set_maxdatasizeforsecuritycommit(topicMeta.maxdatasizeforsecuritycommit());
    ti->set_compressmsg(topicMeta.compressmsg());
    ti->set_compressthres(topicMeta.compressthres());
    ti->set_enablettldel(topicMeta.enablettldel());
    ti->set_readsizelimitsec(topicMeta.readsizelimitsec());
    ti->set_enablelongpolling(topicMeta.enablelongpolling());
    if (topicMeta.has_readnotcommittedmsg()) {
        ti->set_readnotcommittedmsg(topicMeta.readnotcommittedmsg());
    }
    if (topicMeta.has_sealed() && topicMeta.sealed()) {
        ti->set_sealed(true);
    }
    if (topicMeta.dfsroot() != "") {
        ti->set_dfsroot(topicMeta.dfsroot());
    }
    if (topicMeta.has_versioncontrol() && topicMeta.versioncontrol()) {
        ti->set_versioncontrol(true);
    }
    if (topicMeta.extenddfsroot_size() > 0) {
        for (int i = 0; i < topicMeta.extenddfsroot_size(); i++) {
            string *dfsRoot = ti->add_extenddfsroot();
            *dfsRoot = topicMeta.extenddfsroot(i);
        }
    }
}

void SysController::execute() {
    int64_t begTime = TimeUtility::currentTime();
    doExecute();
    int64_t endTime = TimeUtility::currentTime();
    if (_reporter) {
        _reporter->reportScheduleTime(endTime - begTime);
    }
    if (endTime - begTime > 500 * 1000) { // 500ms
        AUTIL_LOG(INFO, "schedule too slow, used[%ldms]", (endTime - begTime) / 1000);
    }
}

void SysController::doExecute() {
    ScopedLock lock(_lock);
    ConfigVersionManager versionManager = _versionManager;
    if (finishUpgrade(versionManager)) {
        return;
    }
    updateAppPlan(versionManager);
    prepareWorkerTable(versionManager);
    allocBrokerTasks(versionManager);
}

void SysController::allocBrokerTasks(const ConfigVersionManager &versionManager) {
    WorkerMap brokerWorkers = _workerTable.getBrokerWorkers();
    WorkerMap aliveBrokerWorkers = filterWorkers(brokerWorkers, true);
    if (canBrokerMakeDecision(versionManager, aliveBrokerWorkers)) {
        doTransferPartition();
        TopicMap topicMap;
        TopicMap logicTopicMap;
        _topicTable.prepareDecision();
        _topicTable.splitTopicMapBySchedule(topicMap, logicTopicMap);
        if (!versionManager.isUpgrading(ROLE_TYPE_BROKER)) {
            adjustPartitionLimit(topicMap, aliveBrokerWorkers);
        }
        if (_adminConfig->autoUpdatePartResource()) {
            updatePartitionResource(topicMap);
        }
        makeDecision(versionManager, ROLE_TYPE_BROKER, topicMap, aliveBrokerWorkers);
        updateTopicStatus(topicMap, logicTopicMap);
        updateBrokerWorkerStatus(brokerWorkers);
    }
    if (versionManager.currentBrokerRoleVersion == versionManager.targetBrokerRoleVersion) {
        releaseDeadWorkers(brokerWorkers);
        _switchCanStart = false;
    }
}

bool SysController::isRoleEnoughToMakeDecision(const string &configPath,
                                               const string &roleVersion,
                                               WorkerMap &aliveBrokerWorkers) {
    WorkerMap currentBrokers = filterWorkerByVersion(aliveBrokerWorkers, roleVersion);
    uint32_t roleCount = _workerManager->getRoleCount(configPath, roleVersion, ROLE_TYPE_BROKER);
    float threshold = _adminConfig->getDecsionThreshold();
    if (currentBrokers.size() >= roleCount * threshold) {
        aliveBrokerWorkers = currentBrokers; // only current role can load partition
        _switchCanStart = false;
        return true;
    } else {
        AUTIL_LOG(WARN,
                  "can't make decision for alive broker worker count [%u],"
                  " expect count [%u], threshold [%f]",
                  (uint32_t)currentBrokers.size(),
                  roleCount,
                  threshold);
        return false;
    }
}

bool SysController::canBrokerMakeDecision(const ConfigVersionManager &versionManager, WorkerMap &aliveBrokerWorkers) {
    if (versionManager.currentBrokerRoleVersion != versionManager.targetBrokerRoleVersion) {
        WorkerMap targetBrokers = filterWorkerByVersion(aliveBrokerWorkers, versionManager.targetBrokerRoleVersion);
        uint32_t roleCount = _workerManager->getRoleCount(
            versionManager.targetBrokerConfigPath, versionManager.targetBrokerRoleVersion, ROLE_TYPE_BROKER);
        if (targetBrokers.size() == roleCount) {
            _switchCanStart = true;
            AUTIL_LOG(INFO,
                      "broker all target version[%s] role[%u] ready, can switch",
                      versionManager.targetBrokerRoleVersion.c_str(),
                      roleCount);
            return true;
        }
        if (_switchCanStart) {
            float threshold = _adminConfig->getDecsionThreshold();
            if (targetBrokers.size() >= roleCount * threshold) {
                AUTIL_LOG(INFO,
                          "broker target version[%s] role count[%u] >= threshold[%f], go on switch",
                          versionManager.targetBrokerRoleVersion.c_str(),
                          roleCount,
                          roleCount * threshold);
                return true;
            }
        }
        AUTIL_LOG(WARN,
                  "can't make decision, target version[%s] alive broker count[%u], expect count[%u]",
                  versionManager.targetBrokerRoleVersion.c_str(),
                  (uint32_t)targetBrokers.size(),
                  roleCount);
        return isRoleEnoughToMakeDecision(
            versionManager.currentBrokerConfigPath, versionManager.currentBrokerRoleVersion, aliveBrokerWorkers);
    } else if (versionManager.currentBrokerConfigPath == versionManager.targetBrokerConfigPath) {
        return isRoleEnoughToMakeDecision(
            versionManager.currentBrokerConfigPath, versionManager.currentBrokerRoleVersion, aliveBrokerWorkers);
    }
    return true;
}

void SysController::releaseDeadWorkers(WorkerMap &workers) {
    int64_t timeout = _adminConfig->getReleaseDeadWorkerTimeSecond() * 1000000;
    WorkerMap::const_iterator it = workers.begin();
    int64_t currentTime = TimeUtility::currentTime();
    vector<string> roleNames;
    for (; it != workers.end(); ++it) {
        if (it->second->isDead() && it->second->getUnknownTime() > timeout) {
            if (currentTime - it->second->getLastReleaseTime() > timeout) {
                roleNames.push_back(it->first);
                AUTIL_LOG(INFO,
                          "will release roles[%s], unknown time [%ld], last release time [%ld]",
                          it->first.c_str(),
                          it->second->getUnknownTime(),
                          it->second->getLastReleaseTime());
                it->second->setLastReleaseTime(currentTime);
            }
        }
    }
    if (roleNames.size() > 0) {
        _workerManager->releaseSlotsPref(roleNames);
    }
}

void SysController::doTransferPartition() {
    if (_clearCurrentTask) {
        _workerTable.clearCurrentTask(_transferGroupName);
        _topicTable.clearCurrentTask(_transferGroupName);
        AUTIL_LOG(INFO, "transfer group[%s] partition now ", _transferGroupName.c_str());
    } else if (_adjustWorkerResourceMap.size() > 0) {
        AUTIL_LOG(INFO, "adjust worker resource now, size[%d]", (int)_adjustWorkerResourceMap.size());
        _workerTable.adjustWorkerResource(_adjustWorkerResourceMap);
    }
    int64_t currentTime = TimeUtility::currentTime();
    int64_t time = _adjustBeginTime + _adminConfig->getAdjustResourceDuration() * 1000;
    if (time < currentTime) {
        _adjustWorkerResourceMap.clear();
        _clearCurrentTask = false;
    }
}

void SysController::getRoleNames(const ConfigVersionManager &versionManager,
                                 const RoleType &roleType,
                                 vector<string> &roleNames) {
    _workerManager->getRoleNames(versionManager.getRoleCurrentPath(roleType),
                                 versionManager.getRoleCurrentVersion(roleType),
                                 roleType,
                                 roleNames);
    if (versionManager.getRoleCurrentVersion(roleType) != versionManager.getRoleTargetVersion(roleType)) {
        vector<string> targetNames;
        _workerManager->getRoleNames(versionManager.getRoleTargetPath(roleType),
                                     versionManager.getRoleTargetVersion(roleType),
                                     roleType,
                                     targetNames);
        roleNames.insert(roleNames.end(), targetNames.begin(), targetNames.end());
        AUTIL_LOG(INFO,
                  "add new role[%s] count [%u] from version [%s]",
                  RoleType_Name(roleType).c_str(),
                  (uint32_t)targetNames.size(),
                  versionManager.getRoleTargetPath(roleType).c_str());
    }
    AUTIL_LOG(DEBUG, "total role count [%u]", (uint32_t)roleNames.size());
}

void SysController::updateAppPlan(const ConfigVersionManager &versionManager) {
    AppPlan appPlan;
    _workerManager->fillBrokerPlan(versionManager, appPlan);
    _workerManager->updateAppPlan(appPlan);
}

void SysController::prepareWorkerTable(const ConfigVersionManager &versionManager) {
    vector<string> roleNames;
    getRoleNames(versionManager, ROLE_TYPE_BROKER, roleNames);
    _workerTable.updateBrokerRoles(roleNames);
    _workerTable.updateAdminRoles();
    _workerTable.prepareDecision();
}

bool SysController::finishUpgrade(const ConfigVersionManager &versionManager) {
    bool brokerFinished = false;
    ConfigVersionManager newVersionManager = versionManager;
    if (versionManager.isUpgrading(ROLE_TYPE_BROKER) && isUpdateVersionFinish(ROLE_TYPE_BROKER, versionManager)) {
        newVersionManager.currentBrokerConfigPath = newVersionManager.targetBrokerConfigPath;
        newVersionManager.currentBrokerRoleVersion = newVersionManager.targetBrokerRoleVersion;
        newVersionManager.brokerRollback = false;
        const string &versionStr = ToJsonString(newVersionManager);
        if (!_zkDataAccessor->writeConfigVersion(versionStr)) {
            AUTIL_LOG(WARN, "write config version file failed, content[%s]", versionStr.c_str());
            return false;
        }
        brokerFinished = true;
        AUTIL_LOG(INFO,
                  "set versionManager[%s %s -> %s %s]",
                  _versionManager.currentBrokerRoleVersion.c_str(),
                  _versionManager.targetBrokerRoleVersion.c_str(),
                  newVersionManager.currentBrokerRoleVersion.c_str(),
                  newVersionManager.targetBrokerRoleVersion.c_str());
    }
    _versionManager = newVersionManager;
    return brokerFinished;
}

void SysController::makeDecision(const ConfigVersionManager &versionManager,
                                 const RoleType roleType,
                                 TopicMap &topicMap,
                                 WorkerMap &workers) {
    const auto &currentRoleVersion = versionManager.getRoleCurrentVersion(roleType);
    const auto &targetRoleVersion = versionManager.getRoleTargetVersion(roleType);
    const auto &currentConfigPath = versionManager.getRoleCurrentPath(roleType);
    const auto &targetConfigPath = versionManager.getRoleTargetPath(roleType);
    DecisionMakerPtr decisionMaker;
    decisionMaker.reset(
        new BrokerDecisionMaker(_adminConfig->getTopicScdType(), _adminConfig->getVeticalGroupBrokerCountMap()));
    TopicMap emptyTopicMap;
    auto *targetTopicMap = isMaster() ? &topicMap : &emptyTopicMap;
    if (currentRoleVersion != targetRoleVersion) {
        WorkerMap currentWorkers = filterWorkerByVersion(workers, currentRoleVersion);
        WorkerMap targetWorkers = filterWorkerByVersion(workers, targetRoleVersion);
        if (targetWorkers.size() > 0) {
            AUTIL_LOG(INFO,
                      "begin role[%s] switch, send empty to current[%s], all topic to target[%s]",
                      RoleType_Name(roleType).c_str(),
                      currentRoleVersion.c_str(),
                      targetRoleVersion.c_str());
            decisionMaker->makeDecision(emptyTopicMap, currentWorkers);
            decisionMaker->makeDecision(*targetTopicMap, targetWorkers);
        } else {
            decisionMaker->makeDecision(*targetTopicMap, workers);
        }
    } else if (currentConfigPath != targetConfigPath) {
        vector<string> roleNames;
        _workerManager->getRoleNames(targetConfigPath, targetRoleVersion, roleType, roleNames);
        WorkerMap::iterator iter;
        WorkerMap targetWorkers;
        for (size_t i = 0; i < roleNames.size(); i++) {
            iter = workers.find(roleNames[i]);
            if (iter != workers.end()) {
                targetWorkers[iter->first] = iter->second;
            }
        }
        WorkerMap deleteWorkers = workers;
        for (iter = targetWorkers.begin(); iter != targetWorkers.end(); iter++) {
            deleteWorkers.erase(iter->first);
        }
        if (deleteWorkers.size() > 0) {
            AUTIL_LOG(INFO,
                      "begin role[%s] switch, send empty to current[%s], all topic to target[%s]",
                      RoleType_Name(roleType).c_str(),
                      currentConfigPath.c_str(),
                      targetConfigPath.c_str());
            decisionMaker->makeDecision(emptyTopicMap, deleteWorkers);
            decisionMaker->makeDecision(*targetTopicMap, targetWorkers);
        } else {
            decisionMaker->makeDecision(*targetTopicMap, targetWorkers);
        }
    } else {
        decisionMaker->makeDecision(*targetTopicMap, workers);
    }
}

bool SysController::isUpdateVersionFinish(const protocol::RoleType &roleType,
                                          const ConfigVersionManager &versionManager) {
    TopicMap topicMap;
    _topicTable.getTopicMap(topicMap);
    auto currentRoleVersion = versionManager.getRoleCurrentVersion(roleType);
    auto targetConfigPath = versionManager.getRoleTargetPath(roleType);
    auto targetRoleVersion = versionManager.getRoleTargetVersion(roleType);
    if (topicMap.size() == 0) {
        uint32_t total = 0;
        uint32_t ready = 0;
        _workerManager->getReadyRoleCount(roleType, targetConfigPath, targetRoleVersion, total, ready);
        AUTIL_LOG(INFO,
                  "target[%s] topic size is 0, total roles[%u], ready roles[%u]",
                  targetRoleVersion.c_str(),
                  total,
                  ready);
        return total == ready;
    } else {
        WorkerMap workers = _workerTable.getBrokerWorkers();
        if (currentRoleVersion != targetRoleVersion) {
            WorkerMap currentVersionWorkers = filterWorkerByVersion(workers, currentRoleVersion);
            WorkerInfo::TaskSet currentTasks = getAllCurrentPartition(currentVersionWorkers);
            WorkerInfo::TaskSet targetTasks = getAllTargetPartition(currentVersionWorkers);
            AUTIL_LOG(INFO,
                      "current[%s] worker count[%ld] current task size[%u], target task size[%u]",
                      currentRoleVersion.c_str(),
                      currentVersionWorkers.size(),
                      (uint32_t)currentTasks.size(),
                      (uint32_t)targetTasks.size());
            return currentTasks.size() == 0 && targetTasks.size() == 0;
        } else {
            vector<string> roleNames;
            _workerManager->getRoleNames(targetConfigPath, targetRoleVersion, roleType, roleNames);
            WorkerMap allWorkers = filterWorkerByVersion(workers, currentRoleVersion);
            WorkerMap::iterator iter;
            WorkerMap targetWorkers;
            for (size_t i = 0; i < roleNames.size(); i++) {
                iter = allWorkers.find(roleNames[i]);
                if (iter != allWorkers.end()) {
                    targetWorkers[iter->first] = iter->second;
                }
            }
            for (iter = targetWorkers.begin(); iter != targetWorkers.end(); iter++) {
                allWorkers.erase(iter->first);
            }
            WorkerInfo::TaskSet currentTasks = getAllCurrentPartition(allWorkers);
            WorkerInfo::TaskSet targetTasks = getAllTargetPartition(allWorkers);
            AUTIL_LOG(INFO,
                      "current[%s] task size[%u], target task size[%u]",
                      currentRoleVersion.c_str(),
                      (uint32_t)currentTasks.size(),
                      (uint32_t)targetTasks.size());
            return currentTasks.size() == 0 && targetTasks.size() == 0;
        }
    }
}

WorkerInfo::TaskSet SysController::getAllCurrentPartition(const WorkerMap &worker) {
    WorkerInfo::TaskSet taskSet;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        WorkerInfo::TaskSet tasks = it->second->getCurrentTask();
        taskSet.insert(tasks.begin(), tasks.end());
    }
    return taskSet;
}

WorkerInfo::TaskMap SysController::getAllCurrentPartitionWithAddress(const WorkerMap &worker) {
    WorkerInfo::TaskMap taskMap;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        const WorkerInfo::TaskMap &tasks = it->second->getCurrentTaskMap();
        taskMap.insert(tasks.begin(), tasks.end());
    }
    return taskMap;
}

WorkerInfo::TaskSet SysController::getNotTimeoutCurrentPartition(const WorkerMap &worker, int64_t timeout) {
    WorkerInfo::TaskSet taskSet;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        if (timeout > 0 && it->second->isDeadInBrokerStatus(_adminConfig->getDeadBrokerTimeoutSec())) {
            continue;
        }
        if (timeout <= 0 || it->second->getUnknownTime() < timeout) {
            const WorkerInfo::TaskSet &tasks = it->second->getCurrentTask();
            taskSet.insert(tasks.begin(), tasks.end());
        }
    }
    return taskSet;
}

WorkerInfo::TaskSet SysController::getAllTargetPartition(const WorkerMap &worker) {
    WorkerInfo::TaskSet taskSet;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        WorkerInfo::TaskSet tasks = it->second->getTargetTask();
        taskSet.insert(tasks.begin(), tasks.end());
    }
    return taskSet;
}

WorkerMap SysController::filterWorkerByVersion(const WorkerMap &worker, const string &version) {
    WorkerMap resultWorkers;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        if (StringUtil::endsWith(it->first, version)) {
            resultWorkers[it->first] = it->second;
        }
    }
    return resultWorkers;
}

WorkerMap SysController::filterWorkers(const WorkerMap &worker, bool alive) {
    WorkerMap resultWorkers;
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        if (!it->second->isDead() && alive) {
            if (_adminConfig->getForceScheduleTimeSecond() < 0 ||
                !it->second->isDeadInBrokerStatus(_adminConfig->getDeadBrokerTimeoutSec())) {
                resultWorkers[it->first] = it->second;
            }
        }
        if (it->second->isDead() && !alive) {
            resultWorkers[it->first] = it->second;
        }
    }
    return resultWorkers;
}

bool SysController::loadMetaInfo() {
    AUTIL_LOG(INFO, "loadMetaInfo");
    ScopedLock lock(_lock);
    return loadTopicInfo() && loadTopicSchema() && initBrokerConfig() && loadChangePartCntTask();
}

bool SysController::statusUpdateHandler(Status lastStatus, Status currentStatus) {
    // for compatible: stop admin worker when become to follower
    if (lastStatus.lfStatus == LF_LEADER && currentStatus.lfStatus == LF_FOLLOWER) {
        setPrimary(false);
        _stop = true;
        _isStopped = true;
    }
    if (_moduleManager) {
        _moduleManager->update(currentStatus);
    }
    return true;
}

void SysController::createTopic(const TopicCreationRequest *request, TopicCreationResponse *response) {
    TopicBatchCreationRequest batchRequest;
    TopicBatchCreationResponse batchResponse;
    *batchRequest.add_topicrequests() = *request;
    batchRequest.set_ignoreexist(false);
    createTopicBatch(&batchRequest, &batchResponse);
    *response->mutable_errorinfo() = batchResponse.errorinfo();
}

bool SysController::checkTopicExist(const TopicCreationRequest *request) {
    const string &topicName = request->topicname();
    if (NULL == _topicTable.findTopic(topicName)) {
        return false;
    } else {
        return true;
    }
}

bool SysController::checkSameTopic(const TopicBatchCreationRequest *request, string &sameTopicName) {
    set<string> topicSet;
    for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
        const TopicCreationRequest &singleReq = request->topicrequests(idx);
        auto iter = topicSet.find(singleReq.topicname());
        if (iter != topicSet.end()) {
            sameTopicName = singleReq.topicname();
            return false;
        } else {
            topicSet.insert(singleReq.topicname());
        }
    }
    return true;
}

ErrorCode SysController::adjustTopicParams(const TopicCreationRequest *request, TopicCreationRequest &retMeta) {
    // compatible old version
    retMeta = *request;
    if (retMeta.has_partitionbuffersize()) {
        uint32_t partBufferSize = retMeta.partitionbuffersize();
        uint32_t partMinBufferSize = (partBufferSize + 1) / 2;
        if (partMinBufferSize == 0) {
            partMinBufferSize = 1;
        }
        uint32_t partMaxBufferSize = partBufferSize * 2;
        retMeta.set_partitionminbuffersize(partMinBufferSize);
        retMeta.set_partitionmaxbuffersize(partMaxBufferSize);
    }
    updateCreateTime(retMeta);
    if (retMeta.dfsroot().empty() && retMeta.topicmode() != TOPIC_MODE_MEMORY_ONLY) {
        retMeta.set_dfsroot(_adminConfig->getDfsRoot());
    }
    const string &topicName = request->topicname();
    if (_adminConfig->getMinMaxPartitionBufferSizeMb() > retMeta.partitionmaxbuffersize()) {
        AUTIL_LOG(INFO,
                  "adjust[%s] max partition buffer[%d -> %ld]",
                  topicName.c_str(),
                  retMeta.partitionmaxbuffersize(),
                  _adminConfig->getMinMaxPartitionBufferSizeMb());
        retMeta.set_partitionmaxbuffersize(_adminConfig->getMinMaxPartitionBufferSizeMb());
    }
    if (retMeta.has_rangecountinpartition() && 1 != retMeta.rangecountinpartition()) {
        AUTIL_LOG(
            INFO, "adjust[%s] rangeCountInPartition[%d -> 1]", topicName.c_str(), retMeta.rangecountinpartition());
        retMeta.set_rangecountinpartition(1);
    }
    const auto &topicGroupVec = _adminConfig->getTopicGroupVec();
    if (!topicGroupVec.empty()) {
        for (auto kv : topicGroupVec) {
            if (topicName.find(kv.first) != string::npos) {
                AUTIL_LOG(INFO,
                          "adjust[%s] group[%s -> %s]",
                          topicName.c_str(),
                          retMeta.topicgroup().c_str(),
                          kv.second.c_str());
                retMeta.set_topicgroup(kv.second);
                break;
            }
        }
    }
    const auto &topicOwnerVec = _adminConfig->getTopicOwnerVec();
    if (!topicOwnerVec.empty() && 0 == retMeta.owners_size()) {
        for (auto kv : topicOwnerVec) {
            if (topicName.find(kv.first) != string::npos) {
                AUTIL_LOG(
                    INFO, "adjust[%s] owners to[%s]", topicName.c_str(), StringUtil::toString(kv.second, ",").c_str());
                for (const auto &owner : kv.second) {
                    retMeta.add_owners(owner);
                }
                break;
            }
        }
    }
    if (_adminConfig->getWorkPartitionLimit() > 0) {
        map<string, uint32_t> brokerCntMap = _adminConfig->getGroupBrokerCountMap();
        uint32_t count = brokerCntMap[retMeta.topicgroup()];
        if (count > 0) {
            uint32_t partLimit = ceil(retMeta.partitioncount() * 1.0 / count);
            AUTIL_LOG(INFO,
                      "adjust[%s] partition limit[%d -> %d]",
                      retMeta.topicname().c_str(),
                      retMeta.partitionlimit(),
                      partLimit);
            retMeta.set_partitionlimit(partLimit);
        }
    }
    if (_adminConfig->getTopicResourceLimit() > 0) {
        if (retMeta.resource() > _adminConfig->getTopicResourceLimit()) {
            retMeta.set_resource(_adminConfig->getTopicResourceLimit());
        }
    }
    protocol::TopicInfo topicInfo;
    topicInfo.set_name(topicName);
    topicInfo.set_status(TOPIC_STATUS_NONE);
    for (uint32_t i = 0; i < retMeta.partitioncount(); ++i) {
        PartitionInfo *pi = topicInfo.add_partitioninfos();
        pi->set_id(i);
        pi->set_brokeraddress("");
        pi->set_status(PARTITION_STATUS_NONE);
    }
    if (retMeta.deletetopicdata() && isMaster() && !clearOldTopicData(topicName)) {
        return ERROR_ADMIN_CLEAR_OLD_TOPIC_DATA_FAILED;
    }
    if (retMeta.deletetopicdata()) {
        AUTIL_LOG(INFO, "set topic [%s] delete topic data flag false.", topicName.c_str());
        retMeta.set_deletetopicdata(false);
    }
    return ERROR_NONE;
}

void SysController::createTopicBatch(const TopicBatchCreationRequest *request, TopicBatchCreationResponse *response) {
    CHECK_IS_LEADER(response);
    if (0 == request->topicrequests_size()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }
    for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
        const TopicCreationRequest &meta = request->topicrequests(idx);
        Result<bool> result = AdminRequestChecker::checkTopicCreationRequest(&meta, _adminConfig, true);
        if (result.is_err()) {
            handleError(response, ERROR_ADMIN_INVALID_PARAMETER, result.get_error().message());
            return;
        }
        auto cleanDataModule = getModule<CleanDataModule>();
        if (cleanDataModule && cleanDataModule->isCleaningTopic(meta.topicname())) {
            handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "topic [" + meta.topicname() + "] is cleaning");
            return;
        }
    }
    string sameTopicName;
    if (!checkSameTopic(request, sameTopicName)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "topic name[" + sameTopicName + "] repeated");
        return;
    }
    auto requestAuthenticator = getRequestAuthenticator();
    if (requestAuthenticator) {
        vector<string> topicNames;
        for (int i = 0; i < request->topicrequests_size(); i++) {
            const string &topicName = request->topicrequests(i).topicname();
            topicNames.emplace_back(topicName);
        }
        auto ret = requestAuthenticator->createTopicAclItems(topicNames);
        if (!ret.is_ok()) {
            handleError(response,
                        ERROR_ADMIN_OPERATION_FAILED,
                        "createTopic failed by serialize topic access info :" + ret.get_error().message());
            return;
        }
    }

    ScopedLock lock(_lock);
    vector<string> existTopics;
    vector<int> newTopicIndex;
    for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
        const TopicCreationRequest &singleReq = request->topicrequests(idx);
        if (checkTopicExist(&singleReq)) {
            existTopics.emplace_back(singleReq.topicname());
        } else {
            newTopicIndex.emplace_back(idx);
        }
    }
    TopicBatchCreationRequest topicMetas;
    if (existTopics.size() > 0) {
        const string &existMsg = StringUtil::toString(existTopics, ";");
        if (request->has_ignoreexist() && request->ignoreexist()) {
            for (int idx : newTopicIndex) {
                TopicCreationRequest *meta = topicMetas.add_topicrequests();
                ErrorCode ec = adjustTopicParams(&request->topicrequests(idx), *meta);
                if (ERROR_NONE != ec) {
                    AUTIL_LOG(ERROR,
                              "adjust topic[%s] params fail, error[%s]",
                              request->topicrequests(idx).topicname().c_str(),
                              ErrorCode_Name(ec).c_str());
                    handleError(response, ec, request->topicrequests(idx).topicname());
                    return;
                }
            }
            AUTIL_LOG(INFO, "topic[%s] exist, ignore", existMsg.c_str());
        } else {
            handleError(response, ERROR_ADMIN_TOPIC_HAS_EXISTED, existMsg);
            return;
        }
    }
    // existTopicSize == 0 or ignoreExist == true
    if (0 == newTopicIndex.size()) {
        AUTIL_LOG(INFO, "no topic need create, return");
        SET_OK(response);
        return;
    }
    if (0 == topicMetas.topicrequests_size()) {
        for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
            TopicCreationRequest *meta = topicMetas.add_topicrequests();
            ErrorCode ec = adjustTopicParams(&request->topicrequests(idx), *meta);
            if (ERROR_NONE != ec) {
                AUTIL_LOG(ERROR,
                          "adjust topic[%s] params fail, error[%s]",
                          request->topicrequests(idx).topicname().c_str(),
                          ErrorCode_Name(ec).c_str());
                handleError(response, ec, request->topicrequests(idx).topicname());
                return;
            }
        }
    }
    // logic topic add physic topics
    if (ERROR_NONE != addPhysicMetasForLogicTopic(topicMetas, response)) {
        return;
    }
    if (!_zkDataAccessor->addTopics(topicMetas)) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "createTopic failed by serialize topicInfo!");
        return;
    }

    for (int idx = 0; idx < topicMetas.topicrequests_size(); ++idx) {
        const auto &meta = topicMetas.topicrequests(idx);
        _topicTable.addTopic(&meta);
        {
            ScopedLock dellock(_deletedTopicMapLock);
            _deletedTopicMap.erase(meta.topicname());
        }
    }
    SET_OK(response);
}

bool SysController::clearOldTopicData(const string &topicName) {
    string dfsRoot = _adminConfig->getDfsRoot();
    if (dfsRoot.empty()) {
        return true;
    }
    string topicDataPath = FileSystem::joinFilePath(dfsRoot, topicName);
    if (fslib::fs::FileSystem::isExist(topicDataPath) == fslib::EC_TRUE) {
        string backupPath =
            FileSystem::joinFilePath(dfsRoot, topicName + "_backup_" + TimeUtility::currentTimeString());
        AUTIL_LOG(INFO,
                  "delete topic data[%s], move [%s -> %s]",
                  topicName.c_str(),
                  topicDataPath.c_str(),
                  backupPath.c_str());
        fslib::ErrorCode ec = FileSystem::move(topicDataPath, backupPath);
        return ec == fslib::EC_OK;
    }

    return true;
}

void SysController::modifyTopic(const TopicCreationRequest *request, TopicCreationResponse *response) {
    CHECK_IS_LEADER(response);
    const Result<bool> &result = AdminRequestChecker::checkTopicCreationRequest(request, _adminConfig, false);
    if (result.is_err()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, result.get_error().message());
        return;
    }
    ScopedLock lock(_lock);
    const string &topicName = request->topicname();
    TopicInfoPtr topicInfoPtr = _topicTable.findTopic(topicName);
    if (topicInfoPtr == NULL) {
        handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
        return;
    }
    TopicCreationRequest newTopicMeta = topicInfoPtr->getTopicMeta();
    if (!canSealedTopicModify(newTopicMeta, request)) {
        handleError(response, ERROR_ADMIN_SEALED_TOPIC_CANNOT_MODIFY, request->ShortDebugString());
        return;
    }
    if (TOPIC_TYPE_LOGIC == newTopicMeta.topictype() || TOPIC_TYPE_LOGIC_PHYSIC == newTopicMeta.topictype()) {
        if (request->has_sealed() && request->sealed() != newTopicMeta.sealed()) {
            const string &errMsg = StringUtil::formatString("cannot seal logic topic, "
                                                            "request[%s]",
                                                            request->ShortDebugString().c_str());
            handleError(response, ERROR_ADMIN_INVALID_PARAMETER, errMsg);
            return;
        }
    }
    bool changed = false;
    bool needChangeVersion = false;
    TopicCreationRequest tmpRequest; // get default value
#define SET_TOPIC_CREATION_REQUEST(item)                                                                               \
    if (request->has_##item() && request->item() != newTopicMeta.item()) {                                             \
        if (!tmpRequest.has_##item() || tmpRequest.item() != request->item()) {                                        \
            newTopicMeta.set_##item(request->item());                                                                  \
            changed = true;                                                                                            \
            needChangeVersion = true;                                                                                  \
        }                                                                                                              \
    }
    SET_TOPIC_CREATION_REQUEST(resource);
    SET_TOPIC_CREATION_REQUEST(partitionlimit);
    SET_TOPIC_CREATION_REQUEST(topicmode);
    SET_TOPIC_CREATION_REQUEST(obsoletefiletimeinterval);
    SET_TOPIC_CREATION_REQUEST(reservedfilecount);
    SET_TOPIC_CREATION_REQUEST(partitionminbuffersize);
    SET_TOPIC_CREATION_REQUEST(partitionmaxbuffersize);
    SET_TOPIC_CREATION_REQUEST(maxwaittimeforsecuritycommit);
    SET_TOPIC_CREATION_REQUEST(maxdatasizeforsecuritycommit);
    SET_TOPIC_CREATION_REQUEST(compressmsg);
    SET_TOPIC_CREATION_REQUEST(compressthres);
    SET_TOPIC_CREATION_REQUEST(dfsroot);
    SET_TOPIC_CREATION_REQUEST(topicgroup);
    SET_TOPIC_CREATION_REQUEST(topicexpiredtime);
    SET_TOPIC_CREATION_REQUEST(rangecountinpartition);
    SET_TOPIC_CREATION_REQUEST(sealed);
    SET_TOPIC_CREATION_REQUEST(topictype);
    SET_TOPIC_CREATION_REQUEST(enablettldel);
    SET_TOPIC_CREATION_REQUEST(readsizelimitsec);
    SET_TOPIC_CREATION_REQUEST(enablelongpolling);
    SET_TOPIC_CREATION_REQUEST(enablemergedata);
    SET_TOPIC_CREATION_REQUEST(versioncontrol);
    SET_TOPIC_CREATION_REQUEST(readnotcommittedmsg);
#undef SET_TOPIC_CREATION_REQUEST
#define REPEATED_FIELD_CHANGE(field, changeVersion)                                                                    \
    if (0 != request->field##_size()) {                                                                                \
        bool rptFldChg = false;                                                                                        \
        if (request->field##_size() != newTopicMeta.field##_size()) {                                                  \
            rptFldChg = true;                                                                                          \
        } else {                                                                                                       \
            for (int i = 0; i < request->field##_size(); ++i) {                                                        \
                if (request->field(i) != newTopicMeta.field(i)) {                                                      \
                    rptFldChg = true;                                                                                  \
                    break;                                                                                             \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
        if (rptFldChg) {                                                                                               \
            newTopicMeta.mutable_##field()->Clear();                                                                   \
            bool clearAll = false;                                                                                     \
            for (int idx = 0; idx < request->field##_size(); ++idx) {                                                  \
                const string &val = request->field(idx);                                                               \
                if (!val.empty()) {                                                                                    \
                    *newTopicMeta.add_##field() = val;                                                                 \
                    if (val == "*") {                                                                                  \
                        clearAll = true;                                                                               \
                        break;                                                                                         \
                    }                                                                                                  \
                }                                                                                                      \
            }                                                                                                          \
            if (clearAll) {                                                                                            \
                newTopicMeta.mutable_##field()->Clear();                                                               \
            }                                                                                                          \
            changed = true;                                                                                            \
            if (changeVersion) {                                                                                       \
                needChangeVersion = changeVersion;                                                                     \
            }                                                                                                          \
        }                                                                                                              \
    }
    REPEATED_FIELD_CHANGE(extenddfsroot, true);
    REPEATED_FIELD_CHANGE(owners, false);
#undef REPEATED_FIELD_CHANGE
    uint32_t curPartCnt = topicInfoPtr->getPartitionCount();
    if (request->has_partitioncount()) {
        if (TOPIC_TYPE_LOGIC_PHYSIC == topicInfoPtr->getTopicType() && topicInfoPtr->physicTopicLstSize() > 0) {
            const string &lastPhysic = topicInfoPtr->getLastPhysicTopicName();
            uint32_t lastPartCnt = curPartCnt;
            if (LogicTopicHelper::getPhysicPartCnt(lastPhysic, lastPartCnt)) {
                curPartCnt = lastPartCnt;
                if (request->partitioncount() != lastPartCnt) {
                    newTopicMeta.set_partitioncount(request->partitioncount());
                    needChangeVersion = true;
                    changed = true;
                }
            }
        } else {
            if (request->partitioncount() != newTopicMeta.partitioncount()) {
                newTopicMeta.set_partitioncount(request->partitioncount());
                changed = true;
                needChangeVersion = true;
            }
        }
    }
    if (changed) {
        if (needChangeVersion) {
            newTopicMeta.set_modifytime(TimeUtility::currentTime());
        }
        if (TOPIC_TYPE_NORMAL != newTopicMeta.topictype() || TOPIC_TYPE_NORMAL != topicInfoPtr->getTopicType()) {
            const TopicCreationRequest &curMeta = topicInfoPtr->getTopicMeta();
            if (!AdminRequestChecker::checkLogicTopicModify(newTopicMeta, curMeta)) {
                const string &errMsg =
                    StringUtil::formatString("logic topic[%s] "
                                             "modify param invalid %s",
                                             curMeta.topicname().c_str(),
                                             ProtoUtil::plainDiffStr(&curMeta, &newTopicMeta).c_str());
                handleError(response, ERROR_ADMIN_INVALID_PARAMETER, errMsg);
                return;
            }
            if (curPartCnt != newTopicMeta.partitioncount()) {
                ErrorCode ec = _zkDataAccessor->sendChangePartCntTask(newTopicMeta);
                if (ERROR_NONE != ec) {
                    const string &errMsg = StringUtil::formatString("send logic topic"
                                                                    "modify partition task failed[%s], part[%d -> %d]",
                                                                    curMeta.ShortDebugString().c_str(),
                                                                    curPartCnt,
                                                                    newTopicMeta.partitioncount());
                    handleError(response, ec, errMsg);
                }
                AUTIL_LOG(INFO,
                          "send modify partition task[%s], part[%d -> %d]",
                          curMeta.ShortDebugString().c_str(),
                          curPartCnt,
                          newTopicMeta.partitioncount());
                SET_OK(response);
                return;
            } else { // L & LP not change current part count
                newTopicMeta.set_partitioncount(topicInfoPtr->getPartitionCount());
            }
        }
        if (!_zkDataAccessor->modifyTopic(newTopicMeta)) {
            handleError(response, ERROR_ADMIN_OPERATION_FAILED, "set topic meta failed!");
        } else {
            if (newTopicMeta.partitioncount() == topicInfoPtr->getPartitionCount()) {
                topicInfoPtr->setTopicMeta(newTopicMeta);
            } else {
                _topicTable.delTopic(topicName);
                _topicTable.addTopic(&newTopicMeta);
            }
            SET_OK(response);
        }
    } else {
        AUTIL_LOG(INFO, "topic[%s] params not change, do nothing", request->topicname().c_str());
        SET_OK(response);
    }
}

void SysController::deleteTopic(const TopicDeletionRequest *request, TopicDeletionResponse *response) {
    CHECK_IS_LEADER(response);
    if (!AdminRequestChecker::checkTopicDeletionRequest(request)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }
    set<string> delTopics;
    delTopics.insert(request->topicname());
    bool deleteData = request->has_deletedata() ? request->deletedata() : false;
    auto batchResponse = doDeleteTopicBatch(delTopics, deleteData);
    *(response->mutable_errorinfo()) = batchResponse.errorinfo();
}

void SysController::deleteTopicBatch(const TopicBatchDeletionRequest *request, TopicBatchDeletionResponse *response) {
    CHECK_IS_LEADER(response);
    if (!AdminRequestChecker::checkTopicBatchDeletionRequest(request)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }
    set<string> delTopics;
    for (int i = 0; i < request->topicnames_size(); i++) {
        if (!request->topicnames(i).empty()) {
            delTopics.insert(request->topicnames(i));
        }
    }
    bool deleteData = request->has_deletedata() ? request->deletedata() : false;
    *response = doDeleteTopicBatch(delTopics, deleteData);
}

TopicBatchDeletionResponse SysController::doDeleteTopicBatch(const set<string> &delTopics, bool deleteData) {
    TopicBatchDeletionResponse response;
    set<string> todelTopics;
    vector<string> notExistTopics;
    vector<string> delPhysicTopics;

    ScopedLock lock(_lock);
    for (auto &topicName : delTopics) {
        TopicInfoPtr tip = _topicTable.findTopic(topicName);
        if (!tip) {
            notExistTopics.push_back(topicName);
        } else {
            if (TOPIC_TYPE_PHYSIC == tip->getTopicType()) {
                delPhysicTopics.push_back(topicName);
                continue;
            }
            todelTopics.insert(topicName);
            if (tip->hasSubPhysicTopics()) {
                const vector<string> &phyLst = tip->physicTopicLst();
                todelTopics.insert(phyLst.begin(), phyLst.end());
            }
        }
    }
    const string &physicTopicsStr = StringUtil::toString(delPhysicTopics.begin(), delPhysicTopics.end(), ",");
    if (todelTopics.size() == 0 && physicTopicsStr.empty()) {
        handleError(
            &response, ERROR_ADMIN_TOPIC_NOT_EXISTED, StringUtil::toString(delTopics.begin(), delTopics.end(), ","));
        return response;
    }
    if (todelTopics.size() == 0 && !physicTopicsStr.empty()) {
        handleError(&response,
                    ERROR_ADMIN_INVALID_PARAMETER,
                    StringUtil::formatString("physic topic[%s] cannot delete", physicTopicsStr.c_str()));
        return response;
    }
    if (!_zkDataAccessor->deleteTopic(todelTopics)) {
        handleError(
            &response, ERROR_ADMIN_OPERATION_FAILED, StringUtil::toString(todelTopics.begin(), todelTopics.end(), ","));
        return response;
    }
    auto requestAuthenticator = getRequestAuthenticator();
    if (requestAuthenticator) {
        vector<string> toDelAcltopicNames(todelTopics.begin(), todelTopics.end());
        auto ret = requestAuthenticator->deleteTopicAclItems(toDelAcltopicNames);
        if (!ret.is_ok()) {
            handleError(&response,
                        ERROR_ADMIN_OPERATION_FAILED,
                        "deleteTopic failed by serialize topic access info :" + ret.get_error().message());
            return response;
        }
    }
    ScopedLock dellock(_deletedTopicMapLock);
    int64_t curTime = TimeUtility::currentTimeInSeconds();
    auto cleanDataModule = getModule<CleanDataModule>();
    for (auto &topicName : todelTopics) {
        if (cleanDataModule && cleanDataModule->needCleanDataAtOnce(topicName)) {
            const TopicCreationRequest &toCleanTopicMeta = _topicTable.findTopic(topicName)->getTopicMeta();
            cleanDataModule->pushCleanAtDeleteTopic(topicName, toCleanTopicMeta);
            _topicTable.delTopic(topicName, deleteData);
            continue;
        }
        _topicTable.delTopic(topicName, deleteData);
        _deletedTopicMap[topicName] = curTime;
    }
    if (!notExistTopics.empty()) {
        AUTIL_LOG(INFO, "delete not existed topics [%s].", StringUtil::toString(notExistTopics, ",").c_str());
    }
    if (!physicTopicsStr.empty()) {
        const string &successDelt = StringUtil::toString(todelTopics.begin(), todelTopics.end(), ",");
        handleError(&response,
                    ERROR_NONE,
                    StringUtil::formatString("topics[%s] delete success, physic topics[%s] cannot delete",
                                             successDelt.c_str(),
                                             physicTopicsStr.c_str()));
        return response;
    }
    ErrorInfo *ei = response.mutable_errorinfo();
    ei->set_errcode(ERROR_NONE);
    ei->set_errmsg(ErrorCode_Name(ei->errcode()));
    return response;
}

void SysController::getSysInfo(const EmptyRequest *request, SysInfoResponse *response) {
    (void)request;
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    response->set_dfsroot(_adminConfig->getDfsRoot());
    response->set_mincopy(0);
    response->set_maxcopy(0);

    SET_OK(response);
}
template <typename T>
void SysController::fillTopicInfo(TopicInfoPtr topicInfoPtr, protocol::TopicInfo *ti, const T *request) {
    assert(topicInfoPtr);
    assert(ti);
    protocol::TopicCreationRequest topicMeta = topicInfoPtr->getTopicMeta();
    // meta
    ti->set_name(topicMeta.topicname());
    ti->set_topicmode(topicMeta.topicmode());
    ti->set_needfieldfilter(topicMeta.needfieldfilter());
    ti->set_obsoletefiletimeinterval(topicMeta.obsoletefiletimeinterval());
    ti->set_reservedfilecount(topicMeta.reservedfilecount());
    ti->set_partitionminbuffersize(topicMeta.partitionminbuffersize());
    ti->set_partitionmaxbuffersize(topicMeta.partitionmaxbuffersize());
    ti->set_resource(topicMeta.resource());
    ti->set_partitionlimit(topicMeta.partitionlimit());
    ti->set_deletetopicdata(topicMeta.deletetopicdata());
    ti->set_maxwaittimeforsecuritycommit(topicMeta.maxwaittimeforsecuritycommit());
    ti->set_maxdatasizeforsecuritycommit(topicMeta.maxdatasizeforsecuritycommit());
    ti->set_compressmsg(topicMeta.compressmsg());
    ti->set_compressthres(topicMeta.compressthres());
    ti->set_createtime(topicMeta.createtime());
    ti->set_dfsroot(topicMeta.dfsroot());
    ti->set_topicgroup(topicMeta.topicgroup());
    ti->set_rangecountinpartition(topicMeta.rangecountinpartition());
    ti->set_modifytime(topicMeta.modifytime());
    ti->set_sealed(topicMeta.sealed());
    ti->set_topictype(topicMeta.topictype());
    ti->set_enablettldel(topicMeta.enablettldel());
    ti->set_readsizelimitsec(topicMeta.readsizelimitsec());
    ti->set_enablelongpolling(topicMeta.enablelongpolling());
    ti->set_versioncontrol(topicMeta.versioncontrol());
    ti->set_enablemergedata(topicMeta.enablemergedata());
    ti->set_readnotcommittedmsg(topicMeta.readnotcommittedmsg());

    if (topicMeta.extenddfsroot_size() > 0) {
        for (int i = 0; i < topicMeta.extenddfsroot_size(); i++) {
            string *dfsRoot = ti->add_extenddfsroot();
            *dfsRoot = topicMeta.extenddfsroot(i);
        }
    }
    ti->set_topicexpiredtime(topicMeta.topicexpiredtime());
    for (int32_t idx = 0; idx < topicMeta.owners_size(); ++idx) {
        ti->add_owners(topicMeta.owners(idx));
    }

    ti->set_needschema(topicMeta.needschema());
    for (int32_t idx = 0; idx < topicMeta.schemaversions_size(); ++idx) {
        ti->add_schemaversions(topicMeta.schemaversions(idx));
    }
    // status
    ti->set_status(topicInfoPtr->getTopicStatus());
    uint32_t pcount = topicMeta.partitioncount();
    ti->set_partitioncount(pcount);
    for (uint32_t i = 0; i != pcount; ++i) {
        PartitionInfo *pi = ti->add_partitioninfos();
        pi->set_id(i);
        string roleAddress = topicInfoPtr->getCurrentRoleAddress(i);
        string roleName, address;
        PathDefine::parseRoleAddress(roleAddress, roleName, address);
        *(pi->mutable_versioninfo()) = _workerTable.getBrokerVersionInfo(roleName);
        pi->set_rolename(roleName);
        pi->set_brokeraddress(address);
        PartitionStatus partitionStatus = topicInfoPtr->getStatus(i);
        pi->set_status(partitionStatus);
        pi->set_sessionid(topicInfoPtr->getSessionId(i));
    }
    for (int32_t idx = 0; idx < topicMeta.physictopiclst_size(); ++idx) {
        ti->add_physictopiclst(topicMeta.physictopiclst(idx));
    }
}

void SysController::fillTopicSimpleInfo(TopicInfoPtr topicInfoPtr, protocol::TopicSimpleInfo *ti) {
    assert(topicInfoPtr);
    assert(ti);
    protocol::TopicCreationRequest topicMeta = topicInfoPtr->getTopicMeta();
    ti->set_name(topicMeta.topicname());
    uint32_t pcount = topicMeta.partitioncount();
    ti->set_partitioncount(pcount);
    uint32_t run = 0, wait = 0, start = 0;
    topicInfoPtr->getPartitionRunInfo(run, wait, start);
    ti->set_runningcount(run);
    for (int32_t idx = 0; idx < topicMeta.owners_size(); ++idx) {
        ti->add_owners(topicMeta.owners(idx));
    }
}

void SysController::getTopicInfo(const TopicInfoRequest *request, TopicInfoResponse *response) {
    // CHECK_IS_LEADER(response);
    if (!AdminRequestChecker::checkTopicInfoRequest(request)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }
    TopicInfoPtr topicInfoPtr;
    const string &topicName = request->topicname();
    string src = request->has_src() ? request->src() : "";
    if (!isMaster() && SRC_API != src) {
        handleError(response, ERROR_ADMIN_NOT_LEADER, topicName);
        return;
    }
    if (isPrimary()) {
        topicInfoPtr = _topicTable.findTopic(topicName);
        if (!topicInfoPtr) {
            handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
            return;
        }
        protocol::TopicInfo *ti = response->mutable_topicinfo();
        fillTopicInfo(topicInfoPtr, ti, request);
        SET_OK(response);
    } else {
        ScopedLock lock(_syncTopicLock);
        map<string, TopicInfoResponse>::iterator iter = _topicInfoResponseCache.find(topicName);
        if (iter != _topicInfoResponseCache.end()) {
            response->CopyFrom(iter->second);
            return;
        }
        topicInfoPtr = _topicTable.findTopic(topicName);
        if (!topicInfoPtr) {
            handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
            return;
        }
        protocol::TopicInfo *ti = response->mutable_topicinfo();
        fillTopicInfo(topicInfoPtr, ti, request);
        SET_OK(response);
        _topicInfoResponseCache[topicName] = *response;
    }
    auto requestAuthenticator = getRequestAuthenticator();
    if (!requestAuthenticator) {
        return;
    }
    const TopicAccessInfo &accessInfo = requestAuthenticator->getTopicAccessInfo(topicName, request->authentication());
    fillTopicOpControl(response, accessInfo);
}

void SysController::getAllTopicInfo(const EmptyRequest *request, AllTopicInfoResponse *response) {
    (void)request;
    // CHECK_IS_LEADER(response);
    TopicMap topicMap;
    if (isPrimary()) {
        _topicTable.getTopicMap(topicMap);
    } else {
        ScopedLock lock(_lock);
        _topicTable.getTopicMap(topicMap);
    }

    for (TopicMap::const_iterator it = topicMap.begin(); it != topicMap.end(); ++it) {
        protocol::TopicInfo *newTopicInfo = response->add_alltopicinfo();
        fillTopicInfo(it->second, newTopicInfo, request);
    }
    SET_OK(response);
}

void SysController::getAllTopicSimpleInfo(const EmptyRequest *request, AllTopicSimpleInfoResponse *response) {
    (void)request;
    // CHECK_IS_LEADER(response);
    TopicMap topicMap;
    if (isPrimary()) {
        _topicTable.getTopicMap(topicMap);
    } else {
        ScopedLock lock(_lock);
        _topicTable.getTopicMap(topicMap);
    }

    for (TopicMap::const_iterator it = topicMap.begin(); it != topicMap.end(); ++it) {
        protocol::TopicSimpleInfo *newTopicInfo = response->add_alltopicsimpleinfo();
        fillTopicSimpleInfo(it->second, newTopicInfo);
    }
    SET_OK(response);
}

void SysController::getAllTopicName(const EmptyRequest *request, TopicNameResponse *response) {
    (void)request;
    // CHECK_IS_LEADER(response);
    set<string> topicNames;
    if (isPrimary()) {
        _topicTable.getTopicNames(topicNames);
    } else {
        ScopedLock lock(_lock);
        _topicTable.getTopicNames(topicNames);
    }
    for (const auto &topic : topicNames) {
        response->add_names(topic);
    }

    SET_OK(response);
}

void SysController::getPartitionInfo(const PartitionInfoRequest *request, PartitionInfoResponse *response) {
    // CHECK_IS_LEADER(response);
    if (!AdminRequestChecker::checkPartitionInfoRequest(request)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }
    const string &topicName = request->topicname();
    TopicInfoPtr topicInfoPtr;
    if (isPrimary()) {
        topicInfoPtr = _topicTable.findTopic(topicName);
        if (!topicInfoPtr) {
            handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
            return;
        }
        doGetPartitionInfo(request, topicInfoPtr, response);
    } else {
        ScopedLock lock(_lock);
        if (request->partitionids_size() == 1) {
            pair<string, uint32_t> key = make_pair(topicName, request->partitionids(0));
            map<pair<string, uint32_t>, PartitionInfoResponse>::iterator iter = _partInfoResponseCache.find(key);
            if (iter != _partInfoResponseCache.end()) {
                response->CopyFrom(iter->second);
                return;
            }
            topicInfoPtr = _topicTable.findTopic(topicName);
            if (!topicInfoPtr) {
                handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
                return;
            }
            doGetPartitionInfo(request, topicInfoPtr, response);
            _partInfoResponseCache[key] = *response;
        } else {
            topicInfoPtr = _topicTable.findTopic(topicName);
            if (!topicInfoPtr) {
                handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
                return;
            }
            doGetPartitionInfo(request, topicInfoPtr, response);
        }
    }
}
void SysController::doGetPartitionInfo(const PartitionInfoRequest *request,
                                       TopicInfoPtr &topicInfoPtr,
                                       PartitionInfoResponse *response) {
    const string &topicName = request->topicname();
    uint32_t maxP = topicInfoPtr->getPartitionCount();
    int pCount = request->partitionids_size();
    uint32_t notExistPartitionId = uint32_t(-1);
    for (int i = 0; i < pCount; ++i) {
        uint32_t id = request->partitionids(i);
        if (id < maxP) {
            PartitionInfo *pi = response->add_partitioninfos();
            const string &roleAddress = topicInfoPtr->getCurrentRoleAddress(id);
            string roleName, address, ip;
            uint16_t port = 0;
            PathDefine::parseRoleAddress(roleAddress, roleName, address);
            PathDefine::parseAddress(address, ip, port);
            *(pi->mutable_versioninfo()) = _workerTable.getBrokerVersionInfo(roleName);
            pi->set_rolename(roleName);
            if (port != 0) {
                pi->set_brokeraddress(address);
            }
            pi->set_id(id);
            pi->set_status(topicInfoPtr->getStatus(i));
            pi->set_sessionid(topicInfoPtr->getSessionId(i));
        } else {
            notExistPartitionId = id;
        }
    }
    if (pCount != response->partitioninfos_size()) {
        std::string errorMsg = StringUtil::formatString(
            "topicName[%s], not exist partitionId[%lu]", topicName.c_str(), notExistPartitionId);
        handleError(response, ERROR_ADMIN_PARTITION_NOT_EXISTED, errorMsg);
    } else {
        SET_OK(response);
    }
}

void SysController::getLeaderInfo(const LeaderInfoRequest *request, LeaderInfoResponse *response) {
    (void)request;
    CHECK_IS_LEADER(response);
    ScopedLock lock(_leaderInfoLock);
    LeaderInfo *leaderInfo = response->mutable_leaderinfo();
    *leaderInfo = _leaderInfo;
    SET_OK(response);
}

void SysController::getRoleAddress(const RoleAddressRequest *request, RoleAddressResponse *response) {
    CHECK_IS_LEADER(response);
    if (!AdminRequestChecker::checkRoleAddressRequest(request)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
        return;
    }

    ScopedLock lock(_lock);
    RoleType roleType = request->role();
    RoleStatus roleStatus = request->status();

    WorkerMap adminWorkers = _workerTable.getAdminWorkers();
    WorkerMap brokerWorkers = _workerTable.getBrokerWorkers();

    switch (roleType) {
    case ROLE_TYPE_ADMIN:
        constructResponseByRoleStatus(ROLE_TYPE_ADMIN, roleStatus, adminWorkers, response);
        break;
    case ROLE_TYPE_BROKER:
        constructResponseByRoleStatus(ROLE_TYPE_BROKER, roleStatus, brokerWorkers, response);
        break;
    case ROLE_TYPE_ALL:
        constructResponseByRoleStatus(ROLE_TYPE_ADMIN, roleStatus, adminWorkers, response);
        constructResponseByRoleStatus(ROLE_TYPE_BROKER, roleStatus, brokerWorkers, response);
        break;
    default:
        break;
    }

    SET_OK(response);
}

void SysController::getAllWorkerStatus(const EmptyRequest *request, WorkerStatusResponse *response) {
    (void)request;
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    SET_OK(response);
    fillStatus(_workerTable.getBrokerWorkers(), *response);
    fillStatus(_workerTable.getAdminWorkers(), *response);
}

void SysController::fillStatus(const WorkerMap &worker, WorkerStatusResponse &response) const {
    for (WorkerMap::const_iterator it = worker.begin(); it != worker.end(); ++it) {
        assert(it->second);
        WorkerStatus *workerStatus = response.add_workers();
        workerStatus->set_isstrange(false);
        it->second->fillWorkerStatus(*workerStatus);
    }
}

void SysController::getPartitionError(const ErrorRequest *request, PartitionErrorResponse *response) {
    CHECK_IS_LEADER(response);
    SET_OK(response);
    ScopedLock lock(_lock);
    _errorHandler.getPartitionError(*request, *response);
}

void SysController::getWorkerError(const ErrorRequest *request, WorkerErrorResponse *response) {
    CHECK_IS_LEADER(response);
    SET_OK(response);
    ScopedLock lock(_lock);
    _errorHandler.getWorkerError(*request, *response);
}

void SysController::updateBrokerConfig(const UpdateBrokerConfigRequest *request, UpdateBrokerConfigResponse *response) {
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    string updateConfigPath = request->configpath();
    ErrorCode ec = _versionManager.validateUpdateVersion(ROLE_TYPE_BROKER, updateConfigPath);
    if (ec != ERROR_NONE) {
        handleError(response, ec, request->ShortDebugString());
        return;
    }
    ConfigVersionManager newVersionManager = _versionManager;
    newVersionManager.targetBrokerConfigPath = updateConfigPath;
    if (_versionManager.needUpgradeRoleVersion(ROLE_TYPE_BROKER, updateConfigPath)) {
        if (!newVersionManager.upgradeTargetRoleVersion(ROLE_TYPE_BROKER)) {
            handleError(response, ERROR_UPGRADE_ROLE_VERSION, request->ShortDebugString());
        }
    }
    string versionStr = ToJsonString(newVersionManager);
    if (!_zkDataAccessor->writeConfigVersion(versionStr)) {
        handleError(
            response, ERROR_WRITE_BROKER_TARGET_VERSION, request->ShortDebugString() + ", versionStr:" + versionStr);
        return;
    }
    _versionManager = newVersionManager;
    SET_OK(response);
}

void SysController::rollbackBrokerConfig(const RollbackBrokerConfigRequest *request,
                                         RollbackBrokerConfigResponse *response) {
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    ErrorCode ec = _versionManager.validateRollback(ROLE_TYPE_BROKER);
    if (ec != ERROR_NONE) {
        handleError(response, ec, request->ShortDebugString());
        return;
    }
    ConfigVersionManager newVersionManager = _versionManager;
    newVersionManager.brokerRollback = true;
    newVersionManager.currentBrokerConfigPath = _versionManager.targetBrokerConfigPath;
    newVersionManager.targetBrokerConfigPath = _versionManager.currentBrokerConfigPath;
    newVersionManager.currentBrokerRoleVersion = _versionManager.targetBrokerRoleVersion;
    newVersionManager.targetBrokerRoleVersion = _versionManager.currentBrokerRoleVersion;
    string versionStr = ToJsonString(newVersionManager);
    if (!_zkDataAccessor->writeConfigVersion(versionStr)) {
        handleError(
            response, ERROR_WRITE_BROKER_TARGET_VERSION, request->ShortDebugString() + ", versionStr:" + versionStr);
        return;
    }
    _versionManager = newVersionManager;
    SET_OK(response);
}

void SysController::transferPartition(const PartitionTransferRequest *request, PartitionTransferResponse *response) {
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    if (request->transferinfo_size() == 0) {
        _clearCurrentTask = true;
        _transferGroupName = request->groupname();
    } else {
        for (int i = 0; i < request->transferinfo_size(); i++) {
            const PartitionTransfer &transferInfo = request->transferinfo(i);
            float ratio = transferInfo.ratio();
            if (ratio >= 1.0 || ratio < 0) {
                AUTIL_LOG(WARN,
                          "adjust ratio must in [0, 1.0], role [%s], ratio[%f]",
                          transferInfo.brokerrolename().c_str(),
                          transferInfo.ratio());
                handleError(response, ERROR_ADMIN_INVALID_PARAMETER, request->ShortDebugString());
                return;
            }
            _adjustWorkerResourceMap[transferInfo.brokerrolename()] = transferInfo.ratio();
        }
    }
    _adjustBeginTime = TimeUtility::currentTime();
    SET_OK(response);
}

void SysController::topicAclManage(const protocol::TopicAclRequest *request, protocol::TopicAclResponse *response) {
    CHECK_IS_LEADER(response);
    const Result<bool> &result = RequestAuthenticator::validateTopicAclRequest(request);
    if (result.is_err()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, result.get_error().message());
        return;
    }

    const string &topicName = request->topicname();
    auto requestAuthenticator = getRequestAuthenticator();
    if (!requestAuthenticator) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, topicName);
        return;
    }
    if (request->accessop() != LIST_ALL_TOPIC_ACCESS) {
        ScopedLock lock(_lock);
        TopicInfoPtr topicInfoPtr = _topicTable.findTopic(topicName);
        if (!topicInfoPtr) {
            handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
            return;
        }
    }
    auto ret = requestAuthenticator->handleTopicAclRequest(request, response);
    if (!ret.is_ok()) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, ret.get_error().message());
    }
    SET_OK(response);
}

void SysController::changeSlot(const ChangeSlotRequest *request, ChangeSlotResponse *response) {
    CHECK_IS_LEADER(response);
    ScopedLock lock(_lock);
    if (request->rolenames_size() == 0 || NULL == _workerManager) {
        AUTIL_LOG(INFO, "empty roleNames or NULL wokerManager, do nothing");
        SET_OK(response);
        return;
    } else {
        vector<string> roleNames;
        for (int i = 0; i < request->rolenames_size(); i++) {
            roleNames.push_back(request->rolenames(i));
        }
        AUTIL_LOG(INFO, "will release roles[%s]", StringUtil::toString(roleNames, ",").c_str());
        _workerManager->releaseSlotsPref(roleNames);
    }
    SET_OK(response);
}

void SysController::registerSchema(const RegisterSchemaRequest *request, RegisterSchemaResponse *response) {
    CHECK_IS_LEADER(response);
    if (!request->has_topicname() || !request->has_schema() || 0 == request->schema().size()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "topic or schema is empty");
        return;
    }
    if (request->has_version() && 0 == request->version()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "version 0 is not allowed!");
        return;
    }

    const string &topicName = request->topicname();
    ScopedLock lock(_lock);
    TopicInfoPtr topicInfoPtr = _topicTable.findTopic(topicName);
    if (!topicInfoPtr) {
        handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
        return;
    }

    protocol::TopicCreationRequest meta = topicInfoPtr->getTopicMeta();
    int32_t version = 0;
    string schemaStr;
    if (meta.has_needschema() && meta.needschema()) {
        FieldSchema schema;
        if (!schema.fromJsonString(request->schema())) {
            handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "parse schema error");
            return;
        }
        version = request->has_version() ? request->version() : schema.calcVersion();
        schemaStr = schema.toJsonString();
    } else {
        schemaStr = request->schema();
        version = request->has_version() ? request->version() : static_cast<int32_t>(std::hash<string>{}(schemaStr));
    }

    for (int index = 0; index < meta.schemaversions_size(); ++index) {
        if (version == meta.schemaversions(index)) {
            response->set_version(version);
            handleError(response,
                        ERROR_ADMIN_SCHEMA_ALREADY_EXIST,
                        string("version [") + to_string(version) + "] schema already exist");
            return;
        }
    }
    if (meta.schemaversions_size() >= MAX_ALLOW_SCHEMA_NUM && (!request->has_cover() || !request->cover())) {
        handleError(response,
                    ERROR_ADMIN_SCHEMA_VERSION_EXCEED,
                    string("schema versions exceed limit[") + to_string(MAX_ALLOW_SCHEMA_NUM) + "]");
        return;
    }
    int32_t removedVersion = 0;
    if (!_zkDataAccessor->addTopicSchema(topicName, version, schemaStr, removedVersion, MAX_ALLOW_SCHEMA_NUM)) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, topicName + " write schema fail");
        return;
    }
    if (0 != removedVersion) {
        for (int index = 0; index < meta.schemaversions_size(); ++index) {
            if (removedVersion == meta.schemaversions(index)) {
                meta.set_schemaversions(index, version);
                break;
            }
        }
    } else {
        meta.add_schemaversions(version);
    }
    // modify topic
    if (!_zkDataAccessor->modifyTopic(meta)) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "set topic meta failed!");
        return;
    } else {
        topicInfoPtr->setTopicMeta(meta);
        response->set_version(version);
        SET_OK(response);
    }
}

void SysController::getSchema(const GetSchemaRequest *request, GetSchemaResponse *response) {
    if (!request->has_topicname() || !request->has_version()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "topic or version is empty");
        return;
    }
    const string &topicName = request->topicname();
    int32_t version = request->version();
    protocol::TopicCreationRequest meta;

#define GET_TOPIC_META()                                                                                               \
    do {                                                                                                               \
        TopicInfoPtr topicInfoPtr = _topicTable.findTopic(topicName);                                                  \
        if (!topicInfoPtr) {                                                                                           \
            handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);                                           \
            return;                                                                                                    \
        }                                                                                                              \
        meta = topicInfoPtr->getTopicMeta();                                                                           \
    } while (0);

    if (isPrimary()) {
        GET_TOPIC_META();
    } else {
        ScopedLock lock(_lock); // lock for topic table loaded
        GET_TOPIC_META();
    }
#undef GET_TOPIC_META

    if (0 == meta.schemaversions_size()) {
        handleError(response, ERROR_ADMIN_SCHEMA_NOT_FOUND, topicName + ", version:" + to_string(version));
        return;
    }
    SchemaInfo schemaInfo;
    if (!_zkDataAccessor->getSchema(topicName, version, schemaInfo)) {
        handleError(response, ERROR_ADMIN_SCHEMA_NOT_FOUND, topicName + ", version:" + to_string(version));
        return;
    }
    *response->mutable_schemainfo() = schemaInfo;
    SET_OK(response);
}

void SysController::reportBrokerStatus(const BrokerStatusRequest *request, BrokerStatusResponse *response) {
    CHECK_IS_LEADER(response);
    if (!request->has_status()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "status request invalid");
        return;
    }
    const BrokerInMetric &status = request->status();
    AUTIL_LOG(INFO,
              "receive status[%d %s %f %f %d]",
              status.reporttime(),
              status.rolename().c_str(),
              status.cpuratio(),
              status.memratio(),
              status.partinmetrics_size());
    auto workerStatusModule = getModule<WorkerStatusModule>();
    if (workerStatusModule && !workerStatusModule->reportBrokerStatus(request, response)) {
        handleError(response, ERROR_ADMIN_CALC_BROKER_STATUS, "push work item fail");
        return;
    }
    SET_OK(response);
}

void SysController::getBrokerStatus(const GetBrokerStatusRequest *request, GetBrokerStatusResponse *response) {
    string roleName;
    if (request->has_rolename()) {
        roleName = request->rolename();
    }
    vector<pair<string, BrokerInStatus>> roleStatus;
    if (_workerTable.getBrokerInStatus(roleName, roleStatus)) {
        for (const auto &status : roleStatus) {
            protocol::BrokerInStatus *newStatus = response->add_status();
            const BrokerInStatus &metric = status.second;
            newStatus->set_reporttime(metric.updateTime);
            newStatus->set_rolename(status.first);
            newStatus->set_cpuratio(metric.cpu);
            newStatus->set_memratio(metric.mem);
            newStatus->set_writerate1min(metric.writeRate1min);
            newStatus->set_writerate5min(metric.writeRate5min);
            newStatus->set_readrate1min(metric.readRate1min);
            newStatus->set_readrate5min(metric.readRate5min);
            newStatus->set_writerequest1min(metric.writeRequest1min);
            newStatus->set_writerequest5min(metric.writeRequest5min);
            newStatus->set_readrequest1min(metric.readRequest1min);
            newStatus->set_readrequest5min(metric.readRequest5min);
            newStatus->set_commitdelay(metric.commitDelay);
            newStatus->set_zfstimeout(metric.zfsTimeout);
        }
        SET_OK(response);
    } else {
        handleError(response, ERROR_ADMIN_ROLE_NOT_FOUND, request->rolename() + " not found");
    }
}

void SysController::getTopicRWTime(const GetTopicRWTimeRequest *request, GetTopicRWTimeResponse *response) {
    string topicName;
    if (request->has_topicname()) {
        topicName = request->topicname();
    }
    TopicRWInfos rwInfos;
    if (_topicInStatusManager.getTopicWriteReadTime(topicName, rwInfos)) {
        response->mutable_tinfos()->Swap(&rwInfos);
        SET_OK(response);
    } else {
        handleError(response, ERROR_ADMIN_TOPIC_NOT_EXISTED, topicName);
        return;
    }
}

void SysController::constructResponseByRoleStatus(const RoleType roleType,
                                                  const RoleStatus roleStatus,
                                                  const WorkerMap &allWorker,
                                                  RoleAddressResponse *response) {
    switch (roleStatus) {
    case ROLE_STATUS_ALL:
        constructAddressGroup(
            roleType, ROLE_STATUS_LIVING, filterWorkers(allWorker, true), response->add_addressgroup());
        constructAddressGroup(
            roleType, ROLE_STATUS_DEAD, filterWorkers(allWorker, false), response->add_addressgroup());
        break;
    case ROLE_STATUS_LIVING:
        constructAddressGroup(
            roleType, ROLE_STATUS_LIVING, filterWorkers(allWorker, true), response->add_addressgroup());
        break;
    case ROLE_STATUS_DEAD:
        constructAddressGroup(
            roleType, ROLE_STATUS_DEAD, filterWorkers(allWorker, false), response->add_addressgroup());
        break;
    default:
        AUTIL_LOG(ERROR, "Unknown role status %d", roleStatus);
        break;
    }
}

void SysController::constructAddressGroup(const RoleType roleType,
                                          const RoleStatus roleStatus,
                                          const WorkerMap &addressMap,
                                          AddressGroup *addressGroup) {
    addressGroup->set_role(roleType);
    addressGroup->set_status(roleStatus);
    auto iter = addressMap.begin();
    for (; iter != addressMap.end(); ++iter) {
        addressGroup->add_addresslist(iter->second->getTargetRoleAddress());
    }
}

void SysController::updateSysStatus() {
    if (!isPrimary()) {
        if (getLeaderStatus() && getHeartbeatMonitorStatus()) {
            AUTIL_LOG(INFO, "become primary admin.");
            setPrimary(true);
        }
    } else {
        if (!getLeaderStatus() || !getHeartbeatMonitorStatus()) {
            AUTIL_LOG(INFO, "become follow admin.");
            setPrimary(false);
        }
    }
}

bool SysController::isPrimary() const { return _adminStatusManager.isLeader(); }

void SysController::setPrimary(bool isPrimary) {
    _adminStatusManager.setLeader(isPrimary);
    AUTIL_LOG(INFO, "set primary status to %d", isPrimary);
}

bool SysController::isMaster() const { return _adminStatusManager.isMaster(); }

void SysController::setMaster(bool isMaster) {
    _adminStatusManager.setMaster(isMaster);
    AUTIL_LOG(DEBUG, "set master status to [%d]", isMaster);
}

bool SysController::getLeaderStatus() const {
    ScopedLock lock(_leaderStatusLock);
    return _goodLeader;
}

void SysController::setLeaderStatus(bool isLeader) {
    ScopedLock lock(_leaderStatusLock);
    _goodLeader = isLeader;
    AUTIL_LOG(INFO, "leader change status to %d", int(isLeader));
}

bool SysController::getHeartbeatMonitorStatus() const {
    ScopedLock lock(_heartBeatStatusLock);
    return _goodHeartbeat;
}

void SysController::setHeartbeatMonitorStatus(ZkWrapper::ZkStatus status) {
    ScopedLock lock(_heartBeatStatusLock);
    if (ZkWrapper::ZK_CONNECTED != status) {
        _goodHeartbeat = false;
    } else {
        _goodHeartbeat = true;
    }
    AUTIL_LOG(INFO, "Heartbeat Monitor change status to %d", status);
}

void SysController::updateHeartBeatStatus(ZkWrapper::ZkStatus status) {
    setHeartbeatMonitorStatus(status);
    updateSysStatus();
}

template <typename T>
void SysController::handleError(T *response, protocol::ErrorCode ec, const string &msgStr) {
    protocol::ErrorInfo *ei = response->mutable_errorinfo();
    ei->set_errcode(ec);
    string errMsg = ErrorCode_Name(ei->errcode());
    if (msgStr.size() > 0) {
        errMsg += "[" + msgStr + "]";
    }
    ei->set_errmsg(errMsg);
    if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) {
        auto noUseTopicModule = getModule<NoUseTopicModule>();
        if (noUseTopicModule) {
            noUseTopicModule->insertNotExistTopics(msgStr);
        }
        if (_reporter) {
            _reporter->incTopicNotExistQps(msgStr);
        }
    }
    AUTIL_LOG(WARN, "Error Message: %s", errMsg.c_str());
}

bool SysController::doUpdateLeaderInfo(const vector<AdminInfo> &currentAdminInfoVec) {
    _leaderInfo.mutable_admininfos()->Clear();
    auto oldVersion = _leaderInfo.selfmasterversion();
    _leaderInfo.set_selfmasterversion(_selfMasterVersion);
    for (size_t i = 0; i < currentAdminInfoVec.size(); i++) {
        AdminInfo *adminInfo = _leaderInfo.add_admininfos();
        adminInfo->set_address(currentAdminInfoVec[i].address());
        adminInfo->set_isprimary(currentAdminInfoVec[i].isprimary());
        adminInfo->set_isalive(currentAdminInfoVec[i].isalive());
    }
    AUTIL_LOG(INFO, "update leader info [%s]", _leaderInfo.ShortDebugString().c_str());
    if (!_zkDataAccessor->setLeaderInfo(_leaderInfo)) {
        AUTIL_LOG(WARN, "set leader info failed, will retry later");
        _leaderInfo.mutable_admininfos()->Clear();
        _leaderInfo.set_selfmasterversion(oldVersion);
        return false;
    }
    return true;
}

void SysController::syncLeaderInfo() {
    updateLeaderInfo();
    publishLeaderInfo();
}

void SysController::updateLeaderInfo() {
    WorkerMap adminWorkers;
    {
        ScopedLock lock(_lock);
        adminWorkers = _workerTable.getAdminWorkers();
    }
    ScopedLock lock(_leaderInfoLock);
    vector<AdminInfo> currentAdminInfoVec;
    WorkerMap::iterator wIt;
    for (wIt = adminWorkers.begin(); wIt != adminWorkers.end(); wIt++) {
        AdminInfo tmpAdminInfo;
        string role;
        string address;
        PathDefine::parseRoleAddress(wIt->second->getCurrentRoleAddress(), role, address);
        tmpAdminInfo.set_address(address);
        tmpAdminInfo.set_isalive(!wIt->second->isDead());
        tmpAdminInfo.set_isprimary(address == _leaderInfo.address());
        currentAdminInfoVec.push_back(tmpAdminInfo);
    }
    if (_selfMasterVersion == _leaderInfo.selfmasterversion() &&
        currentAdminInfoVec.size() == _leaderInfo.admininfos_size()) {
        if (forceSyncLeaderInfo() || leaderInfoChanged(currentAdminInfoVec)) {
            doUpdateLeaderInfo(currentAdminInfoVec);
        }
    } else {
        doUpdateLeaderInfo(currentAdminInfoVec);
    }
}

bool SysController::leaderInfoChanged(const vector<AdminInfo> &currentAdminInfoVec) {
    for (size_t i = 0; i < currentAdminInfoVec.size(); i++) {
        const AdminInfo &tmpInfo1 = *_leaderInfo.mutable_admininfos(i);
        const AdminInfo &tmpInfo2 = currentAdminInfoVec[i];
        if (!(tmpInfo1 == tmpInfo2)) {
            AUTIL_LOG(INFO,
                      "has diff leader info[%s %s]",
                      tmpInfo1.ShortDebugString().c_str(),
                      tmpInfo2.ShortDebugString().c_str());
            return true;
        }
    }
    return false;
}

bool SysController::forceSyncLeaderInfo() {
    if (-1 == _adminConfig->getForceSyncLeaderInfoInterval()) {
        return false;
    }
    int64_t curTime = TimeUtility::currentTime();
    if (curTime > _forceSyncLeaderInfoTimestamp && isPrimary()) {
        AUTIL_LOG(INFO, "force sync leader info, last[%s]", _leaderInfo.ShortDebugString().c_str());
        _forceSyncLeaderInfoTimestamp = curTime + _adminConfig->getForceSyncLeaderInfoInterval();
        return true;
    } else {
        return false;
    }
}

void SysController::publishLeaderInfo() {
    const auto &pathVec = _adminConfig->getSyncAdminInfoPath();
    if (!isPrimary() || pathVec.empty()) {
        return;
    }
    string leaderInfoStr, leaderInfoJsonStr;
    ScopedLock lock(_leaderInfoLock);
    for (const auto &path : pathVec) {
        string filePath = PathDefine::getLeaderInfoFile(path);
        string jsonPath = PathDefine::getLeaderInfoJsonFile(path);
        bool isFileDiff = checkLeaderInfoDiff(filePath, false);
        bool isJsonFileDiff = checkLeaderInfoDiff(jsonPath, true);
        if (_adminConfig->enableBackup()) {
            if (isFileDiff || isJsonFileDiff) {
                const string &inlineFilePath = PathDefine::getPublishInlineFile(path);
                if (!updateMasterStatus(inlineFilePath, true)) {
                    AUTIL_LOG(DEBUG, "inline file update failed, path [%s]", inlineFilePath.c_str());
                    continue;
                }
                _leaderInfo.SerializeToString(&leaderInfoStr);
                writeLeaderInfoWithInline(filePath, inlineFilePath, leaderInfoStr);
                leaderInfoJsonStr = http_arpc::ProtoJsonizer::toJsonString(_leaderInfo);
                writeLeaderInfoWithInline(jsonPath, inlineFilePath, leaderInfoJsonStr);
            }
        } else {
            if (isFileDiff) {
                _leaderInfo.SerializeToString(&leaderInfoStr);
                writeLeaderInfo(filePath, leaderInfoStr);
            }
            if (isJsonFileDiff) {
                leaderInfoJsonStr = http_arpc::ProtoJsonizer::toJsonString(_leaderInfo);
                writeLeaderInfo(jsonPath, leaderInfoJsonStr);
            }
        }
    }
}

bool SysController::readLeaderInfo(const std::string &path, bool isJson, LeaderInfo &leaderInfo) {
    std::string content;
    fslib::ErrorCode ec = FileSystem::readFile(path, content);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(WARN, "get leader info content failed. path[%s]", path.c_str());
        return false;
    }
    if (isJson) {
        if (!http_arpc::ProtoJsonizer::fromJsonString(content, &leaderInfo)) {
            AUTIL_LOG(WARN, "parse content from [%s] failed", content.c_str());
            return false;
        }
    } else {
        if (!leaderInfo.ParseFromString(content)) {
            AUTIL_LOG(WARN, "parse content from [%s] failed", path.c_str());
            return false;
        }
    }
    return true;
}

bool SysController::checkLeaderInfoDiff(const string &path, bool isJson) {
    LeaderInfo leaderInfo;
    if (!readLeaderInfo(path, isJson, leaderInfo)) {
        return true;
    }
    if (leaderInfo == _leaderInfo) {
        return false;
    }
    return true;
}

void SysController::writeLeaderInfo(const string &filePath, const string &content) {
    if (FileSystem::isExist(filePath) == fslib::EC_TRUE) {
        if (FileSystem::remove(filePath) != fslib::EC_OK) {
            AUTIL_LOG(WARN, "remove path [%s] failed!", filePath.c_str());
            return;
        } else {
            AUTIL_LOG(INFO, "remove path [%s] success!", filePath.c_str());
        }
    }
    if (fslib::EC_OK != FileSystem::writeFile(filePath, content)) {
        AUTIL_LOG(WARN, "sync admin leader info to [%s] failed, will retry!", filePath.c_str());
    } else {
        AUTIL_LOG(INFO, "sync admin leader info to [%s] success", filePath.c_str());
    }
}

bool SysController::updateMasterStatus(const string &inlineFilePath, bool needUpdateInline) {
    if (!_adminConfig->enableBackup()) {
        return false;
    }
    string inlineContent;
    if (fslib::EC_OK != PanguInlineFileUtil::getInlineFile(inlineFilePath, inlineContent)) {
        AUTIL_LOG(ERROR, "read inline[%s] fail", inlineFilePath.c_str());
        return false;
    }
    uint64_t curMasterVersion = 0;
    if (!inlineContent.empty()) {
        if (!StringUtil::strToUInt64(inlineContent.c_str(), curMasterVersion)) {
            AUTIL_LOG(ERROR, "covert inline version[%s] to uint64 fail", inlineContent.c_str());
            return false;
        }
    }
    if (curMasterVersion > _selfMasterVersion) {
        AUTIL_LOG(DEBUG,
                  "skip update inline version, curVersion[%lu] selfVersion[%lu]",
                  curMasterVersion,
                  _selfMasterVersion.load());
        setMaster(false);
        return false;
    }
    if (curMasterVersion < _selfMasterVersion && needUpdateInline) {
        AUTIL_LOG(
            INFO, "update inline version to pangu, version [%lu]->[%lu]", curMasterVersion, _selfMasterVersion.load());
        auto ec = PanguInlineFileUtil::updateInlineFile(inlineFilePath, curMasterVersion, _selfMasterVersion);
        if (fslib::EC_OK != ec) {
            AUTIL_LOG(ERROR,
                      "update master version inline file failed, path [%s] selfVersion[%lu] curVersion[%lu], "
                      "error[%d %s]",
                      inlineFilePath.c_str(),
                      _selfMasterVersion.load(),
                      curMasterVersion,
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return false;
        }
    }
    setMaster(true);
    return true;
}

void SysController::writeLeaderInfoWithInline(const string &filePath, const string &inlinePath, const string &content) {
    if (!_adminConfig->enableBackup()) {
        return;
    }
    fslib::ErrorCode ec = fslib::EC_OK;
    std::string inlineContent;
    if (_selfMasterVersion > 0) {
        inlineContent = std::to_string(_selfMasterVersion);
    }
    fslib::fs::File *pFile = PanguInlineFileUtil::openFileForWrite(filePath, inlinePath, inlineContent, ec);
    std::unique_ptr<fslib::fs::File> filePtr(pFile);
    if (!pFile || fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR,
                  "open leader file[%s] fail, error[%d %s]",
                  filePath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return;
    }
    ssize_t writeLen = pFile->write(content.c_str(), content.size());
    if (writeLen == -1) {
        ec = pFile->getLastError();
        AUTIL_LOG(ERROR,
                  "write file[%s] fail, fslib error[%d %s]",
                  pFile->getFileName(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    ec = pFile->close();
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(INFO,
                  "close file[%s] fslib error[%d %s]",
                  pFile->getFileName(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
}

void SysController::deleteExpiredTopic() {
    if (!isMaster()) {
        AUTIL_LOG(INFO, "slave skip delete expired topic loop");
        return;
    }
    AUTIL_LOG(INFO, "delete expired topic loop");
    TopicMap topicMap;
    ScopedLock lock(_lock);
    _topicTable.getTopicMap(topicMap);

    map<string, TopicCreationRequest> todelTopicMetas;
    for (TopicMap::const_iterator it = topicMap.begin(); it != topicMap.end(); ++it) {
        const TopicCreationRequest &request = it->second->getTopicMeta();
        string topicName = it->first;
        if (request.has_topicexpiredtime() && request.topicexpiredtime() != -1) {
            int64_t expireTime = request.topicexpiredtime() * 1000 * 1000;
            if (expireTime < request.createtime()) {
                expireTime += request.createtime();
            }
            if (expireTime < TimeUtility::currentTime()) {
                todelTopicMetas[topicName] = request;
                AUTIL_LOG(INFO,
                          "to delete topic[%s], expired time[%ld]",
                          request.topicname().c_str(),
                          request.topicexpiredtime());
            }
        }
    }
    if (todelTopicMetas.empty()) {
        return;
    }

    vector<TopicCreationRequest> updatedTopicMetas;
    set<string> deletedTopics;
    if (!_zkDataAccessor->deleteTopicsAllType(todelTopicMetas, deletedTopics, updatedTopicMetas)) {
        AUTIL_LOG(INFO,
                  "delete topics [%s] failed,",
                  StringUtil::toString(deletedTopics.begin(), deletedTopics.end(), ",").c_str());
        return;
    }

    for (const auto &topicName : deletedTopics) {
        _topicTable.delTopic(topicName);
    }
    for (const auto &item : updatedTopicMetas) {
        _topicTable.updateTopic(&item);
    }
    {
        ScopedLock dellock(_deletedTopicMapLock);
        int64_t curTime = TimeUtility::currentTimeInSeconds();
        for (auto &topicName : deletedTopics) {
            _deletedTopicMap[topicName] = curTime;
        }
    }
}

void SysController::removeOldData() {
    if (isMaster()) {
        removeOldZkData();
        removeOldTopicData();
        removeCleanAtDeleteTopicData();
        removeOldHealthCheckData();
        removeOldWriterVersionData();
    }
}

void SysController::removeOldZkData() {
    AUTIL_LOG(INFO, "begin clean old swift zk datas");
    string brokerCurrent, brokerTarget;
    getCurrentAndTargetVersion(brokerCurrent, brokerTarget);
    const string &brokerLockDir = PathDefine::getBrokerLockDir(_adminConfig->getZkRoot());
    vector<string> allLockPath;
    if (!fslib::util::FileUtil::listDirWithAbsolutePath(brokerLockDir, allLockPath, false)) {
        AUTIL_LOG(ERROR, "list %s failed", brokerLockDir.c_str());
        return;
    }
    doRemoveOldZkData(allLockPath, ROLE_TYPE_BROKER, brokerCurrent, brokerTarget);

    const string &taskDir = PathDefine::getTaskDir(_adminConfig->getZkRoot());
    vector<string> allTaskPath;
    if (!fslib::util::FileUtil::listDirWithAbsolutePath(taskDir, allTaskPath, false)) {
        AUTIL_LOG(ERROR, "list %s failed", taskDir.c_str());
        return;
    }
    doRemoveOldZkData(allTaskPath, ROLE_TYPE_BROKER, brokerCurrent, brokerTarget);
}

void SysController::doRemoveOldZkData(const vector<string> &paths,
                                      const RoleType &roleType,
                                      const string &currentVersion,
                                      const string &targetVersion) {
    for (const auto &rolePath : paths) {
        if (roleType == ROLE_TYPE_BROKER) {
            if (rolePath.find("broker") == string::npos) {
                continue;
            }
        }
        std::string path = rolePath;
        if (StringUtil::endsWith(path, "/")) {
            path = path.substr(0, path.size() - 1);
        }
        if (!(StringUtil::endsWith(path, "_" + currentVersion) || StringUtil::endsWith(path, "_" + targetVersion))) {
            if (fslib::EC_OK != FileSystem::remove(path)) {
                AUTIL_LOG(WARN, "remove zkpath [%s] failed.", path.c_str());
            } else {
                AUTIL_LOG(INFO, "remove zkpath [%s] success.", path.c_str());
            }
        }
    }
}

void SysController::removeOldTopicData() {
    string dfsRoot = _adminConfig->getDfsRoot();
    if (dfsRoot.empty()) {
        return;
    }
    AUTIL_LOG(INFO, "begin clean old swift hdfs datas");
    vector<string> allTopicNames;
    if (fslib::EC_OK != fslib::fs::FileSystem::listDir(dfsRoot, allTopicNames)) {
        AUTIL_LOG(ERROR, "list %s failed", dfsRoot.c_str());
        return;
    }
    AUTIL_LOG(INFO, "all topic name size [%d]", (int)allTopicNames.size());
    vector<string> deletedTopicNames;
    {
        ScopedLock lock(_lock);
        for (size_t i = 0; i < allTopicNames.size(); i++) {
            if (PathDefine::getHealthCheckDir("") == allTopicNames[i]) {
                continue;
            }
            TopicInfoPtr topicInfoPtr = _topicTable.findTopic(allTopicNames[i]);
            if (!topicInfoPtr || TOPIC_TYPE_LOGIC == topicInfoPtr->getTopicType()) {
                deletedTopicNames.push_back(allTopicNames[i]);
                AUTIL_LOG(INFO, "add need delete topic data name[%s]", allTopicNames[i].c_str());
            }
        }
    }
    for (size_t i = 0; i < deletedTopicNames.size(); i++) {
        doRemoveOldTopicData(dfsRoot, deletedTopicNames[i], _adminConfig->getReserveDataByHour());
        if (_stop || !isPrimary()) {
            AUTIL_LOG(INFO, "system is stopping[%d], primary[%d], stop clean old data", _stop, isPrimary());
            break;
        }
    }
    AUTIL_LOG(INFO, "end clean old swift datas");
}

void SysController::doRemoveOldTopicData(const string &dfsRoot, const string &topicName, double reserveDataByHour) {
    uint64_t lastModifyTime;
    if (!getTopicLastModifyTime(topicName, dfsRoot, lastModifyTime)) {
        AUTIL_LOG(WARN, "get last modify time of [%s] failed", topicName.c_str());
        return;
    }
    int64_t curTime = TimeUtility::currentTimeInSeconds();
    if (int64_t(lastModifyTime) < curTime - int64_t(reserveDataByHour * 3600)) {
        const string &topicDataPath = FileSystem::joinFilePath(dfsRoot, topicName);
        AUTIL_LOG(INFO, "clean [%lu] [%s] [%s]", lastModifyTime, topicName.c_str(), topicDataPath.c_str());
        if (fslib::EC_OK != FileSystem::remove(topicDataPath)) {
            AUTIL_LOG(WARN, "clean [%lu] [%s] [%s] failed.", lastModifyTime, topicName.c_str(), topicDataPath.c_str());
        } else {
            ScopedLock dellock(_deletedTopicMapLock);
            _deletedTopicMap.erase(topicName);
        }
    }
}

void SysController::removeOldHealthCheckData() {
    if (_versionManager.isUpgrading()) {
        return;
    }
    AUTIL_LOG(INFO, "removing old health check data");
    string brokerCurrent, brokerTarget;
    getCurrentAndTargetVersion(brokerCurrent, brokerTarget);
    string dfsRoot = _adminConfig->getDfsRoot();
    if (!dfsRoot.empty()) {
        if (!doRemoveOldHealthCheckData(dfsRoot, brokerCurrent, brokerTarget)) {
            AUTIL_LOG(WARN,
                      "do remove oldHealthCheckData fail[%s] brokerCurrent[%s], brokerTarget[%s]",
                      dfsRoot.c_str(),
                      brokerCurrent.c_str(),
                      brokerTarget.c_str());
        }
    }
    string zkPath = _adminConfig->getZkRoot();
    if (!zkPath.empty()) {
        if (!doRemoveOldHealthCheckData(zkPath, brokerCurrent, brokerTarget)) {
            AUTIL_LOG(WARN,
                      "do remove oldHealthCheckData fail[%s] brokerCurrent[%s], brokerTarget[%s]",
                      zkPath.c_str(),
                      brokerCurrent.c_str(),
                      brokerTarget.c_str());
        }
    }
}

bool SysController::doRemoveOldHealthCheckData(const string &path,
                                               const string &currentVersion,
                                               const string &targetVersion) {
    bool allSuccess = true;
    string healthCheckDir = PathDefine::getHealthCheckDir(path);
    vector<string> versions;
    fslib::ErrorCode ec = FileSystem::listDir(healthCheckDir, versions);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(WARN,
                  "listDir[%s] failed, error[%d %s]",
                  healthCheckDir.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    for (const auto &version : versions) {
        if (version != currentVersion && version != targetVersion) {
            string toRemoveVersion = PathDefine::getHealthCheckVersionDir(path, version);
            ec = FileSystem::remove(toRemoveVersion);
            if (fslib::EC_OK == ec) {
                AUTIL_LOG(INFO, "remove[%s] success", toRemoveVersion.c_str());
            } else {
                AUTIL_LOG(WARN,
                          "remove[%s] failed, error[%d %s]",
                          toRemoveVersion.c_str(),
                          ec,
                          FileSystem::getErrorString(ec).c_str());
                allSuccess = false;
            }
        }
    }
    return allSuccess;
}

bool SysController::getTopicLastModifyTime(const string &topicName, const string dfsRoot, uint64_t &lastModifyTime) {
    lastModifyTime = 0;
    {
        ScopedLock dellock(_deletedTopicMapLock);
        auto iter = _deletedTopicMap.find(topicName);
        if (iter != _deletedTopicMap.end()) {
            lastModifyTime = iter->second;
            return true;
        }
    }
    const string &topicPath = FileSystem::joinFilePath(dfsRoot, topicName);
    vector<string> partitions;
    if (!fslib::util::FileUtil::listDirWithAbsolutePath(topicPath, partitions, false)) {
        return false;
    }
    for (size_t i = 0; i < partitions.size(); i++) {
        vector<string> files;
        if (!fslib::util::FileUtil::listDirWithAbsolutePath(partitions[i], files, false)) {
            return false;
        }
        for (size_t i = 0; i < files.size(); i++) {
            if (StringUtil::endsWith(files[i], ".meta")) {
                fslib::FileMeta fileMeta;
                // in sec
                fslib::ErrorCode ec = FileSystem::getFileMeta(files[i], fileMeta);
                if (ec != fslib::EC_OK) {
                    return false;
                }
                lastModifyTime = max(fileMeta.lastModifyTime, lastModifyTime);
            }
            if (_stop) {
                AUTIL_LOG(INFO, "system is stopping, stop clean old data.");
                return false;
            }
        }
    }
    return true;
}

bool SysController::initBrokerConfig() {
    string versionStr;
    if (!_zkDataAccessor->readConfigVersion(versionStr)) {
        return false;
    }
    try {
        FromJsonString(_versionManager, versionStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "from json failed, content[%s], exception[%s]", versionStr.c_str(), e.what());
        return false;
    }
    return true;
}

void SysController::reportMetrics() {
    if (_reporter == NULL) {
        return;
    }
    monitor::SysControlMetricsCollector collector;
    if (isPrimary()) {
        collector.isLeader = 100;
        _topicTable.collectMetrics(collector);
        _workerTable.collectMetrics(collector, _adminConfig->getAdminCount());
        // detect zombie worker
        vector<string> zombieWorkers;
        vector<string> timeoutWorkers;
        _workerTable.findErrorBrokers(zombieWorkers,
                                      timeoutWorkers,
                                      _adminConfig->getDeadBrokerTimeoutSec(),
                                      _adminConfig->getZfsTimeout(),
                                      _adminConfig->getCommitDelayThreshold());
        _reporter->reportErrorBrokers(zombieWorkers, timeoutWorkers);

        TopicMap topicMap;
        _topicTable.getTopicMap(topicMap);
        _topicInStatusManager.updateTopics(topicMap);
        vector<pair<string, uint32_t>> resourceVec;
        for (const auto &iter : topicMap) {
            resourceVec.emplace_back(iter.first, iter.second->getResource());
        }
        if (resourceVec.size() > 0) {
            _reporter->reportPartitionResource(resourceVec);
        }
    } else {
        collector.isLeader = 0;
        ScopedLock lock(_lock);
        _topicTable.collectMetrics(collector);
        _workerTable.collectMetrics(collector, _adminConfig->getAdminCount());
    }
    collector.isMaster = isMaster() ? 100 : 0;
    _reporter->reportSysControlMetrics(collector);
}

void SysController::getCurrentAndTargetVersion(string &brokerCurrent, string &brokerTarget) {
    ScopedLock lock(_lock);
    brokerCurrent = _versionManager.currentBrokerRoleVersion;
    brokerTarget = _versionManager.targetBrokerRoleVersion;
}

bool SysController::loadBrokerVersionInfos() {
    RoleVersionInfos roleInfos;
    if (!_zkDataAccessor->getBrokerVersionInfos(roleInfos)) {
        return false;
    }
    _workerTable.updateBrokerVersionInfos(roleInfos);
    return true;
}

bool SysController::backTopicMetas() {
    const string &backMetaPath = _adminConfig->getBackMetaPath();
    if (backMetaPath.empty()) {
        return false;
    }
    string meta;
    if (!_zkDataAccessor->readTopicMetas(meta)) {
        return false;
    }
    if (_latestMeta == meta) {
        return false;
    }
    deleteObsoleteMetaFiles(backMetaPath, _adminConfig->getReserveBackMetaCount());
    string fileName("topic_meta_" + TimeUtility::currentTimeString());
    const string &filePath = FileSystem::joinFilePath(backMetaPath, fileName);
    fslib::ErrorCode ec = FileSystem::writeFile(filePath, meta);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "write topic meta[%s] failed, error[%d]!", filePath.c_str(), ec);
        return false;
    }
    _latestMeta = meta;
    AUTIL_LOG(INFO, "write topic meta[%s] success", filePath.c_str());
    return true;
}

void SysController::updateTopicRWTime() {
    TopicRWInfos rwInfos;
    _topicInStatusManager.getTopicWriteReadTime(string(), rwInfos);
    if (0 == rwInfos.infos_size()) {
        return;
    }
    if (!_zkDataAccessor->writeTopicRWTime(rwInfos)) {
        AUTIL_LOG(WARN, "write topic write read time fail, size:%d", rwInfos.infos_size());
    }
}

void SysController::deleteObsoleteMetaFiles(const string &filePath, uint32_t reserveCount) {
    ++_obsoleteMetaDeleteFrequency;
    if (0 != _obsoleteMetaDeleteFrequency % reserveCount) {
        return;
    } else {
        _obsoleteMetaDeleteFrequency = 0;
    }
    vector<string> metaFiles;
    if (fslib::EC_OK != FileSystem::listDir(filePath, metaFiles)) {
        AUTIL_LOG(ERROR, "list %s failed", filePath.c_str());
        return;
    }
    std::sort(metaFiles.begin(), metaFiles.end(), std::greater<string>());
    for (size_t index = reserveCount; index < metaFiles.size(); ++index) {
        const string &oneFile = FileSystem::joinFilePath(filePath, metaFiles[index]);
        if (fslib::EC_OK == FileSystem::remove(oneFile)) {
            AUTIL_LOG(INFO, "remove file [%s] success!", oneFile.c_str());
        } else {
            AUTIL_LOG(ERROR, "remove file [%s] failed", oneFile.c_str());
        }
    }
}

void SysController::updatePartitionResource(TopicMap &topicMap) {
    uint32_t newResource = 1, oldResource = 0;
    string changeLog;
    for (auto iter = topicMap.begin(); iter != topicMap.end(); ++iter) {
        if (_topicInStatusManager.getPartitionResource(iter->first, newResource)) {
            if (iter->second->setResource(newResource, oldResource)) {
                changeLog += iter->first + ":" + to_string(oldResource) + "->" + to_string(newResource) + ",";
            }
        }
    }
    if (changeLog.size() > 0) {
        AUTIL_LOG(INFO, "%s", changeLog.c_str());
    }
}

void SysController::reportMissTopic(const MissTopicRequest *request, MissTopicResponse *response) {
    if (!isPrimary()) {
        return;
    }
    auto noUseTopicModule = getModule<NoUseTopicModule>();
    if (noUseTopicModule) {
        noUseTopicModule->reportMissTopic(request, response);
    }
}

void SysController::getLastDeletedNoUseTopic(const EmptyRequest *request, LastDeletedNoUseTopicResponse *response) {
    (void)request;
    auto noUseTopicModule = getModule<NoUseTopicModule>();
    if (noUseTopicModule) {
        noUseTopicModule->getLastNoUseTopicsMeta(response);
    }
    SET_OK(response);
}

void SysController::getDeletedNoUseTopic(const DeletedNoUseTopicRequest *request, DeletedNoUseTopicResponse *response) {
    if (!request->has_filename()) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "miss fileName");
        return;
    }
    TopicMetas metas;
    if (!_zkDataAccessor->loadDeletedNoUseTopics(request->filename(), metas)) {
        const string &errorMsg = StringUtil::formatString("read zk file[%s] fail", request->filename().c_str());
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, errorMsg.c_str());
        return;
    }
    response->mutable_topicmetas()->Swap(metas.mutable_topicmetas());
    SET_OK(response);
}

void SysController::getDeletedNoUseTopicFiles(const EmptyRequest *request, DeletedNoUseTopicFilesResponse *response) {
    (void)request;
    vector<string> topicFiles;
    if (!_zkDataAccessor->loadDeletedNoUseTopicFiles(topicFiles)) {
        handleError(response, ERROR_ADMIN_INVALID_PARAMETER, "read zk fail");
        return;
    }
    for (const auto &tfile : topicFiles) {
        *response->add_filenames() = tfile;
    }
    SET_OK(response);
}

void SysController::turnToMaster(const TurnToMasterRequest *request, TurnToMasterResponse *response) {
    AUTIL_LOG(INFO, "turn to master, req [%s]", request->ShortDebugString().c_str());
    if (!_adminConfig->enableBackup()) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "backup disable");
        return;
    }
    if (!isPrimary()) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "is not leader");
        return;
    }
    if (isMaster()) {
        AUTIL_LOG(INFO, "already master update version");
    }
    auto targetVersion = request->targetversion();
    if (targetVersion <= _selfMasterVersion) {
        auto errMsg = StringUtil::formatString("request target version error, "
                                               "target[%lu] <= self[%lu]",
                                               targetVersion,
                                               _selfMasterVersion.load());
        handleError(response, ERROR_ADMIN_MASTER_VERSION_ERROR, errMsg);
        return;
    }
    const auto &paths = _adminConfig->getSyncAdminInfoPath();
    if (paths.empty()) {
        handleError(response, ERROR_ADMIN_MASTER_VERSION_ERROR, "publish sync admin info empty");
        return;
    }
    for (const auto &path : paths) {
        auto inlineFilePath = PathDefine::getPublishInlineFile(path);
        std::string inlineContent;
        if (fslib::EC_OK != PanguInlineFileUtil::getInlineFile(inlineFilePath, inlineContent)) {
            handleError(response, ERROR_ADMIN_OPERATION_FAILED, "turn failed, already master");
            AUTIL_LOG(ERROR, "read inline[%s] fail", inlineFilePath.c_str());
            return;
        }
        uint64_t curMasterVersion = 0;
        if (!inlineContent.empty()) {
            if (!StringUtil::strToUInt64(inlineContent.c_str(), curMasterVersion)) {
                const string &errMsg =
                    StringUtil::formatString("covert inline version[%s] to uint64 fail", inlineContent.c_str());
                handleError(response, ERROR_ADMIN_OPERATION_FAILED, errMsg);
                return;
            }
            if (targetVersion <= curMasterVersion) {
                auto errMsg = StringUtil::formatString("request target version error, "
                                                       "target[%lu] <= current[%lu]",
                                                       targetVersion,
                                                       curMasterVersion);
                handleError(response, ERROR_ADMIN_MASTER_VERSION_ERROR, errMsg);
                return;
            }
        }
    }
    if (!_zkDataAccessor->writeMasterVersion(targetVersion)) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "zk write master info failed");
        return;
    }
    _selfMasterVersion = targetVersion;
    SET_OK(response);
}

void SysController::turnToSlave(const TurnToSlaveRequest *request, TurnToSlaveResponse *response) {}

void SysController::getMasterInfo(const EmptyRequest *request, MasterInfoResponse *response) {
    if (!_adminConfig->enableBackup()) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "backup disable");
        return;
    }
    response->set_ismaster(isMaster());
    response->set_masterversion(_selfMasterVersion);
    SET_OK(response);
}

void SysController::updateWriterVersion(const UpdateWriterVersionRequest *request,
                                        UpdateWriterVersionResponse *response) {
    AUTIL_LOG(INFO, "update writer version request[%s]", request->ShortDebugString().c_str());
    CHECK_IS_LEADER(response);
    auto topicDataModule = getModule<TopicDataModule>();
    if (!topicDataModule) {
        handleError(response, ERROR_ADMIN_OPERATION_FAILED, "topicWriterController is empty");
        return;
    }
    topicDataModule->updateWriterVersion(request, response);
}

auth::RequestAuthenticatorPtr SysController::getRequestAuthenticator() {
    auto topicAclModule = getModule<TopicAclManageModule>();
    if (!topicAclModule) {
        return {};
    }
    return topicAclModule->getRequestAuthenticator();
}

bool SysController::canSealedTopicModify(const TopicCreationRequest &meta, const TopicCreationRequest *request) const {
    // 非seal的topic可以直接修改
    // LP的topic自己seal掉了，但代表的是最老的物理topic属性，其逻辑topic是可以修改的
    if (!meta.sealed() || TOPIC_TYPE_LOGIC_PHYSIC == meta.topictype()) {
        return true;
    }
#define CHECK_TOPIC_FIELD(item)                                                                                        \
    if (request->has_##item() && request->item() != meta.item()) {                                                     \
        return false;                                                                                                  \
    }

    CHECK_TOPIC_FIELD(partitioncount);
    CHECK_TOPIC_FIELD(topicmode);
    CHECK_TOPIC_FIELD(needfieldfilter);
    CHECK_TOPIC_FIELD(dfsroot);
    CHECK_TOPIC_FIELD(needschema);
    CHECK_TOPIC_FIELD(sealed);
    CHECK_TOPIC_FIELD(topictype);
#undef CHECK_TOPIC_FIELD
    if (request->extenddfsroot_size() > 0) {
        for (int idx = 0; idx < request->extenddfsroot_size(); ++idx) {
            const string &path = request->extenddfsroot(idx);
            bool find = false;
            for (int i = 0; i < meta.extenddfsroot_size(); ++i) {
                const string &md = meta.extenddfsroot(i);
                if (path == md) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                return false;
            }
        }
    }
    return true;
}

void SysController::changeTopicPartCnt() {
    if (!isPrimary()) {
        return;
    }
    const ChangeTopicPartCntTasks &changePartCntTasks = _zkDataAccessor->getChangePartCntTasks();
    if (0 == changePartCntTasks.tasks_size()) {
        return;
    }

    TopicMap topicMap;
    _topicTable.getTopicMap(topicMap);
    bool hasGetSealTopicStatus = false;
    unordered_set<string> sealedTopicSet;
    set<int64_t> finishedIds;

    for (int idx = 0; idx < changePartCntTasks.tasks_size(); ++idx) {
        const auto &task = changePartCntTasks.tasks(idx);
        // 1. 看是否跟当前最新物理topic part数一致，一致就不动
        auto iter = topicMap.find(task.meta().topicname());
        if (iter == topicMap.end()) {
            AUTIL_LOG(ERROR, "not find topic[%s], cannot change part, delete task", task.meta().topicname().c_str());
            finishedIds.insert(task.taskid());
            continue;
        }
        const string &physicTopicName = iter->second->getLastPhysicTopicName();
        if (physicTopicName.empty()) {
            AUTIL_LOG(ERROR, "topic[%s]'s physic topic empty, cannot change part", task.meta().topicname().c_str());
            continue;
        }
        auto subIter = topicMap.find(physicTopicName);
        if (subIter == topicMap.end()) {
            AUTIL_LOG(ERROR, "not find physic topic[%s], cannot change part", physicTopicName.c_str());
            continue;
        }
        auto lastPhTopicInfo = subIter->second;
        if (lastPhTopicInfo->getPartitionCount() == task.meta().partitioncount()) {
            finishedIds.insert(task.taskid());
            continue;
        }
        if (!hasGetSealTopicStatus) {
            updateSealTopicStatus(sealedTopicSet);
            hasGetSealTopicStatus = true;
        }

        // 2. 看最新的物理topic是否已经seal，seal成功了就create new topic
        if (sealedTopicSet.find(physicTopicName) != sealedTopicSet.end()) {
            // create new topic
            AUTIL_LOG(INFO, "add physic topic for[%s]", task.meta().topicname().c_str());
            bool ret = false;
            TopicCreationRequest retLogicMeta;
            TopicCreationRequest retLastPhysicMeta;
            TopicCreationRequest retNewPhysicTopicMeta;
            {
                ScopedLock lock(_lock);
                ret = _zkDataAccessor->addPhysicTopic(task.meta(),
                                                      _adminConfig->getBrokerCfgTTlSec(),
                                                      retLogicMeta,
                                                      retLastPhysicMeta,
                                                      retNewPhysicTopicMeta);
            }
            if (!ret) {
                AUTIL_LOG(ERROR, "[%s] add physic topic failed for write zk", task.meta().topicname().c_str());
                continue;
            }
            _topicTable.addTopic(&retNewPhysicTopicMeta);
            _topicTable.updateTopic(&retLogicMeta);
            _topicTable.updateTopic(&retLastPhysicMeta);
            {
                ScopedLock dellock(_deletedTopicMapLock);
                _deletedTopicMap.erase(retNewPhysicTopicMeta.topicname());
            }
            finishedIds.insert(task.taskid());
            continue;
        }

        // 3. 不一致也没没seal，先seal再创建
        TopicCreationRequest lastPhTopicMeta = lastPhTopicInfo->getTopicMeta();
        lastPhTopicMeta.set_sealed(true);
        lastPhTopicMeta.set_modifytime(TimeUtility::currentTime());
        bool modifySucess = false;
        {
            ScopedLock lock(_lock);
            AUTIL_LOG(INFO, "seal topic[%s]", lastPhTopicMeta.topicname().c_str());
            modifySucess = _zkDataAccessor->modifyTopic(lastPhTopicMeta);
        }
        if (!modifySucess) {
            AUTIL_LOG(ERROR, "zk seal last physic topic failed[%s]", lastPhTopicMeta.ShortDebugString().c_str());
        } else {
            _topicTable.updateTopic(&lastPhTopicMeta);
        }
    }

    // udpate tasks
    _zkDataAccessor->updateChangePartCntTasks(finishedIds);
}

void SysController::updateLoadedTopic(unordered_set<string> &topicSet) {
    topicSet.clear();
    const WorkerMap brokerWorkers = _workerTable.getBrokerWorkers();
    for (auto &worker : brokerWorkers) {
        const HeartbeatInfo &current = (worker.second)->getCurrentHeartbeat();
        for (int i = 0; i < current.tasks_size(); i++) {
            topicSet.insert(current.tasks(i).taskinfo().partitionid().topicname());
        }
    }
}

void SysController::updateSealTopicStatus(unordered_set<string> &sealedTopicSet) {
    sealedTopicSet.clear();
    TopicMap topicMap;
    _topicTable.getTopicMap(topicMap);
    std::unordered_map<std::string, Bitmap> topicPart;
    for (const auto &item : topicMap) {
        const auto &topicName = item.first;
        const auto &topicInfo = item.second;
        size_t count = topicInfo->getPartitionCount();
        topicPart.emplace(topicName, Bitmap(count));
    }
    const WorkerMap &brokerWorkers = _workerTable.getBrokerWorkers();
    for (auto &item : brokerWorkers) {
        const HeartbeatInfo &curHeartbeat = (item.second)->getCurrentHeartbeat();
        for (int i = 0; i < curHeartbeat.tasks_size(); ++i) {
            const TaskInfo &taskInfo = curHeartbeat.tasks(i).taskinfo();
            const string &topicName = taskInfo.partitionid().topicname();
            const auto &partId = taskInfo.partitionid().id();
            if (!taskInfo.sealed()) {
                continue;
            }
            auto iter = topicPart.find(topicName);
            if (iter == topicPart.end()) {
                continue;
            }
            if (partId < iter->second.GetItemCount()) {
                iter->second.Set(partId);
            }
            if (iter->second.GetUnsetCount() == 0) {
                AUTIL_LOG(INFO, "insert sealed topic[%s]", topicName.c_str());
                sealedTopicSet.insert(topicName);
            }
        }
    }
}

ErrorCode SysController::getPhysicMetaFromLogic(const TopicCreationRequest &logicMeta,
                                                int32_t physicIdx,
                                                TopicCreationRequest &physicMeta) {
    physicMeta = logicMeta;
    const string &physicName = logicMeta.physictopiclst(physicIdx);
    string logicName;
    int64_t timestamp;
    uint32_t partCnt;
    if (!LogicTopicHelper::parsePhysicTopicName(physicName, logicName, timestamp, partCnt)) {
        return ERROR_ADMIN_INVALID_PARAMETER;
    }
    physicMeta.set_topicname(physicName);
    physicMeta.set_partitioncount(partCnt);
    if (logicMeta.physictopiclst_size() - 1 != physicIdx) {
        physicMeta.set_sealed(true);
        const string &nextPhysicName = logicMeta.physictopiclst(physicIdx + 1);
        string nextLogicName;
        int64_t nextTimestamp;
        uint32_t nextPartCnt;
        if (!LogicTopicHelper::parsePhysicTopicName(nextPhysicName, nextLogicName, nextTimestamp, nextPartCnt)) {
            return ERROR_ADMIN_INVALID_PARAMETER;
        }
        int64_t topicExpiredTimeSec = LogicTopicHelper::getPhysicTopicExpiredTime(
            nextTimestamp, logicMeta.obsoletefiletimeinterval(), _adminConfig->getBrokerCfgTTlSec());
        physicMeta.set_topicexpiredtime(topicExpiredTimeSec);
    } else {
        physicMeta.set_sealed(false);
        physicMeta.set_topicexpiredtime(-1);
    }
    if (TOPIC_TYPE_LOGIC == logicMeta.topictype() && physicIdx == 0) {
        physicMeta.set_enablettldel(true);
    } else {
        physicMeta.set_enablettldel(false);
    }
    physicMeta.set_topictype(TOPIC_TYPE_PHYSIC);
    physicMeta.clear_physictopiclst();
    return ERROR_NONE;
}

ErrorCode SysController::addPhysicMetasForLogicTopic(TopicBatchCreationRequest &topicMetas,
                                                     TopicBatchCreationResponse *response) {
    TopicBatchCreationRequest physicMetas;
    for (int idx = 0; idx < topicMetas.topicrequests_size(); ++idx) {
        const TopicCreationRequest &logicMeta = topicMetas.topicrequests(idx);
        if (TOPIC_TYPE_LOGIC == logicMeta.topictype() || TOPIC_TYPE_LOGIC_PHYSIC == logicMeta.topictype()) {
            for (int i = 0; i < logicMeta.physictopiclst_size(); ++i) {
                TopicCreationRequest *physicMeta = physicMetas.add_topicrequests();
                ErrorCode ec = getPhysicMetaFromLogic(logicMeta, i, *physicMeta);
                if (ERROR_NONE != ec) {
                    string errMsg = StringUtil::formatString("[%s] physic topic[%d] "
                                                             "invalid",
                                                             logicMeta.ShortDebugString().c_str(),
                                                             i);
                    handleError(response, ERROR_ADMIN_INVALID_PARAMETER, errMsg);
                    return ec;
                }
            }
        }
    }
    // merge topic metas
    for (int idx = 0; idx < physicMetas.topicrequests_size(); ++idx) {
        *topicMetas.add_topicrequests() = physicMetas.topicrequests(idx);
    }
    return ERROR_NONE;
}

void SysController::adjustPartitionLimit(TopicMap &topicMap, const WorkerMap &aliveBrokerWorkers) {
    map<string, uint32_t> brokerGroupCnt;
    string grp, name;
    for (const auto &broker : aliveBrokerWorkers) {
        if (PathDefine::parseRoleGroup(broker.first, grp, name)) {
            ++brokerGroupCnt[grp];
        }
    }
    map<string, uint32_t> brokerCntMap = _adminConfig->getGroupBrokerCountMap();
    for (auto iter = topicMap.begin(); iter != topicMap.end(); ++iter) {
        auto &tinfo = iter->second;
        const auto &group = tinfo->getTopicGroup();
        uint32_t count = min(brokerCntMap[group], brokerGroupCnt[group]);
        if (count == 0 || count < brokerCntMap[group] * _adminConfig->getDecsionThreshold()) {
            continue;
        }
        uint32_t oldLimit = 1;
        uint32_t newLimit = ceil(tinfo->getPartitionCount() * 1.0 / count);
        if (tinfo->setPartitionLimit(newLimit, oldLimit)) {
            AUTIL_LOG(INFO, "adjust[%s] partition limit[%d -> %d]", iter->first.c_str(), oldLimit, newLimit);
        }
    }
}

bool SysController::canDeal(const vector<string> &workers) {
    if (0 == workers.size()) {
        return false;
    }
    uint32_t count = _adminConfig->getErrorBrokerDealRatio() * _workerTable.getBrokerCount();
    count = max(count, (uint32_t)1);
    return workers.size() <= count;
}

bool SysController::doDiffTopics(const TopicMap &topicMap,
                                 const AllTopicInfoResponse &allTopicInfoResponse,
                                 TopicBatchCreationRequest &newTopics,
                                 TopicBatchDeletionRequest &deletedTopics) {
    bool hasDiff = false;
    set<string> masterTopics;
    for (int index = 0; index < allTopicInfoResponse.alltopicinfo_size(); ++index) {
        const auto &tinfo = allTopicInfoResponse.alltopicinfo(index);
        masterTopics.insert(tinfo.name());
        const auto iter = topicMap.find(tinfo.name());
        if (topicMap.end() == iter) {
            TopicCreationRequest meta;
            fillTopicMeta(tinfo, meta);
            *newTopics.add_topicrequests() = meta;
            hasDiff = true;
            AUTIL_LOG(INFO, "topic[%s] not found in cur follower, add", tinfo.name().c_str());
        } else {
            if (topicInfoChanged(tinfo, iter->second->getTopicMeta())) {
                *deletedTopics.add_topicnames() = tinfo.name();
                TopicCreationRequest meta;
                fillTopicMeta(tinfo, meta);
                *newTopics.add_topicrequests() = meta;
                hasDiff = true;
                AUTIL_LOG(INFO, "topic[%s] has diff, delete then add", tinfo.name().c_str());
            }
            // else, same topic info, do nothing;
        }
    }
    // topic in follower but not in master
    for (const auto &item : topicMap) {
        if (masterTopics.find(item.first) == masterTopics.end()) {
            *deletedTopics.add_topicnames() = item.first;
            hasDiff = true;
            AUTIL_LOG(INFO, "topic[%s] not found in master, delete", item.first.c_str());
        }
    }
    return hasDiff;
}

void SysController::fillTopicMeta(const protocol::TopicInfo &tinfo, TopicCreationRequest &meta) {
    meta.set_topicname(tinfo.name());
    meta.set_partitioncount(tinfo.partitioncount());
    meta.set_topicmode(tinfo.topicmode());
    meta.set_needfieldfilter(tinfo.needfieldfilter());
    meta.set_obsoletefiletimeinterval(tinfo.obsoletefiletimeinterval());
    meta.set_reservedfilecount(tinfo.reservedfilecount());
    meta.set_partitionminbuffersize(tinfo.partitionminbuffersize());
    meta.set_partitionmaxbuffersize(tinfo.partitionmaxbuffersize());
    meta.set_resource(tinfo.resource());
    meta.set_partitionlimit(tinfo.partitionlimit());
    meta.set_deletetopicdata(tinfo.deletetopicdata());
    meta.set_maxwaittimeforsecuritycommit(tinfo.maxwaittimeforsecuritycommit());
    meta.set_maxdatasizeforsecuritycommit(tinfo.maxdatasizeforsecuritycommit());
    meta.set_compressmsg(tinfo.compressmsg());
    meta.set_compressthres(tinfo.compressthres());
    meta.set_createtime(tinfo.createtime());
    meta.set_dfsroot(tinfo.dfsroot());
    meta.set_topicgroup(tinfo.topicgroup());
    meta.set_rangecountinpartition(tinfo.rangecountinpartition());
    meta.set_modifytime(tinfo.modifytime());
    meta.set_sealed(tinfo.sealed());
    meta.set_topictype(tinfo.topictype());
    meta.set_enablettldel(tinfo.enablettldel());
    meta.set_readsizelimitsec(tinfo.readsizelimitsec());
    meta.set_enablelongpolling(tinfo.enablelongpolling());
    meta.set_versioncontrol(tinfo.versioncontrol());
    meta.set_enablemergedata(tinfo.enablemergedata());
    for (int32_t idx = 0; idx < tinfo.extenddfsroot_size(); ++idx) {
        *meta.add_extenddfsroot() = tinfo.extenddfsroot(idx);
    }
    meta.set_topicexpiredtime(tinfo.topicexpiredtime());
    for (int32_t idx = 0; idx < tinfo.owners_size(); ++idx) {
        meta.add_owners(tinfo.owners(idx));
    }
    meta.set_needschema(tinfo.needschema());
    // TODO. schema need copy file
    for (int32_t idx = 0; idx < tinfo.schemaversions_size(); ++idx) {
        meta.add_schemaversions(tinfo.schemaversions(idx));
    }
    for (int32_t idx = 0; idx < tinfo.physictopiclst_size(); ++idx) {
        meta.add_physictopiclst(tinfo.physictopiclst(idx));
    }
}

bool SysController::topicInfoChanged(const protocol::TopicInfo &tinfo, const TopicCreationRequest &meta) {
    bool isSame =
        tinfo.name() == meta.topicname() && tinfo.partitioncount() == meta.partitioncount() &&
        tinfo.topicmode() == meta.topicmode() && tinfo.needfieldfilter() == meta.needfieldfilter() &&
        tinfo.obsoletefiletimeinterval() == meta.obsoletefiletimeinterval() &&
        tinfo.reservedfilecount() == meta.reservedfilecount() &&
        tinfo.partitionminbuffersize() == meta.partitionminbuffersize() &&
        tinfo.partitionmaxbuffersize() == meta.partitionmaxbuffersize() &&
        tinfo.deletetopicdata() == meta.deletetopicdata() &&
        tinfo.maxwaittimeforsecuritycommit() == meta.maxwaittimeforsecuritycommit() &&
        tinfo.maxdatasizeforsecuritycommit() == meta.maxdatasizeforsecuritycommit() &&
        tinfo.compressmsg() == meta.compressmsg() && tinfo.dfsroot() == meta.dfsroot() &&
        tinfo.topicgroup() == meta.topicgroup() && tinfo.sealed() == meta.sealed() &&
        tinfo.topictype() == meta.topictype() && tinfo.enablettldel() == meta.enablettldel() &&
        tinfo.readsizelimitsec() == meta.readsizelimitsec() && tinfo.enablelongpolling() == meta.enablelongpolling() &&
        tinfo.versioncontrol() == meta.versioncontrol() && tinfo.enablemergedata() == meta.enablemergedata() &&
        tinfo.topicexpiredtime() == meta.topicexpiredtime() && tinfo.needschema() == meta.needschema();
    if (!isSame) {
        return true;
    }
#define CHECK_REPETED_FIELD(field)                                                                                     \
    if (meta.field##_size() != tinfo.field##_size()) {                                                                 \
        return true;                                                                                                   \
    }                                                                                                                  \
    for (int32_t idx = 0; idx < tinfo.field##_size(); ++idx) {                                                         \
        if (meta.field(idx) != tinfo.field(idx)) {                                                                     \
            return true;                                                                                               \
        }                                                                                                              \
    }

    CHECK_REPETED_FIELD(extenddfsroot);
    CHECK_REPETED_FIELD(owners);
    CHECK_REPETED_FIELD(schemaversions);
    CHECK_REPETED_FIELD(physictopiclst);
#undef CHECK_REPETED_FIELD

    return false;
}

void SysController::removeCleanAtDeleteTopicData() {
    unordered_set<string> loadedTopic;
    updateLoadedTopic(loadedTopic);
    auto cleanDataModule = getModule<CleanDataModule>();
    if (cleanDataModule) {
        cleanDataModule->removeCleanAtDeleteTopicData(loadedTopic);
    }
}

void SysController::removeOldWriterVersionData() {
    set<string> topicNames;
    _topicTable.getTopicNames(topicNames);
    _zkDataAccessor->removeOldWriterVersionData(topicNames);
}

bool SysController::initModuleManager() {
    _moduleManager = std::make_unique<ModuleManager>();
    if (!_moduleManager) {
        AUTIL_LOG(ERROR, "module manager init failed.");
        return false;
    }
    if (!_moduleManager->init(&_adminStatusManager, this, _adminConfig)) {
        AUTIL_LOG(ERROR, "init module manager failed.");
        return false;
    }
    if (!_moduleManager->start()) {
        AUTIL_LOG(ERROR, "start module manager failed.");
        return false;
    }
    return true;
}

bool SysController::initAdminStatusManager() {
    _adminStatusManager.setStatusUpdateHandler(
        std::bind(&SysController::statusUpdateHandler, this, std::placeholders::_1, std::placeholders::_2));
    if (!_adminStatusManager.start()) {
        AUTIL_LOG(ERROR, "admin status manager init failed.");
        return false;
    }
    return true;
}

template <typename ModuleClass>
std::shared_ptr<ModuleClass> SysController::getModule() {
    if (!_moduleManager) {
        return nullptr;
    }
    auto module = _moduleManager->getModule<ModuleClass>();
    if (!module || !module->isLoad()) {
        return nullptr;
    }
    return module;
}

void SysController::fillTopicOpControl(TopicInfoResponse *response, const TopicAccessInfo &accessInfo) const {
    TopicPermission *opControl = response->mutable_topicinfo()->mutable_opcontrol();
    if (accessInfo.accesstype() == TOPIC_ACCESS_READ_WRITE) {
        opControl->set_canread(true);
        opControl->set_canwrite(true);
    } else if (accessInfo.accesstype() == TOPIC_ACCESS_READ) {
        opControl->set_canread(true);
        opControl->set_canwrite(false);
    } else {
        opControl->set_canread(false);
        opControl->set_canwrite(false);
    }
}

} // namespace admin
} // namespace swift
