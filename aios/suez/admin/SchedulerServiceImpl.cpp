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
#include "suez/admin/SchedulerServiceImpl.h"

#include "autil/ClosureGuard.h"
#include "autil/StringConvertor.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SchedulerServiceImpl);

SchedulerServiceImpl::SchedulerServiceImpl(bool localCarbon,
                                           bool localCm2Mode,
                                           const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager)
    : _serviceReady(false), _localCarbon(localCarbon), _localCm2Mode(localCm2Mode), _roleManager(roleManager) {}

SchedulerServiceImpl::~SchedulerServiceImpl() = default;

bool SchedulerServiceImpl::start(const std::shared_ptr<catalog::CatalogControllerManager> &catalogControllerManager,
                                 const std::shared_ptr<ClusterManager> &clusterManager) {
    _targetGenerator = std::make_unique<TargetGenerator>(catalogControllerManager, clusterManager);
    _carbonAdapter = CarbonAdapter::create(_localCarbon, _roleManager, clusterManager);
    if (_carbonAdapter == nullptr) {
        AUTIL_LOG(ERROR, "create carbon adapter failed");
        return false;
    }

    if (_localCm2Mode) {
        _fetchLoopThread = autil::LoopThread::createLoopThread(
            std::bind(&SchedulerServiceImpl::fetchFromCarbon, this), 10 * 1000 * 1000, "AdminOpsFetch");
        if (!_fetchLoopThread) {
            AUTIL_LOG(ERROR, "create fetch from carbon thread failed");
            return false;
        }
    }

    _loopThread = autil::LoopThread::createLoopThread(
        std::bind(&SchedulerServiceImpl::commitToCarbon, this), 10 * 1000 * 1000, "AdminOpsCommit");

    if (!_loopThread) {
        return false;
    }

    _serviceReady = true;
    return true;
}

void SchedulerServiceImpl::stop() {
    _loopThread.reset();
    _fetchLoopThread.reset();
}

void SchedulerServiceImpl::getSystemInfo(::google::protobuf::RpcController *controller,
                                         const GetSystemInfoRequest *request,
                                         GetSystemInfoResponse *response,
                                         google::protobuf::Closure *done) {
    autil::ScopedReadLock lock(_lock);
    autil::ClosureGuard guard(done);
    if (!_serviceReady) {
        return;
    }

    map<string, carbon::GroupStatus> groupStatusMap;
    _roleManager->getGroupStatuses(
        std::vector<std::string>{request->clusternames().begin(), request->clusternames().end()}, groupStatusMap);
    simplifyGroupsStatus(groupStatusMap);
    reRangeGroupStatus(groupStatusMap);

    try {
        response->set_systeminfostr(ToJsonString(groupStatusMap));
    } catch (const autil::legacy::ExceptionBase &e) { AUTIL_LOG(ERROR, "to json failed, error: %s", e.what()); }
}

void SchedulerServiceImpl::simplifyGroupsStatus(map<string, carbon::GroupStatus> &groupStatusMap) {
    for (auto &groupStatusIter : groupStatusMap) {
        for (auto &roleIter : groupStatusIter.second.roleStatuses) {
            auto curInfo = roleIter.second.mutable_curinstanceinfo();
            if (curInfo != nullptr) {
                for (auto &node : *(curInfo->mutable_replicanodes())) {
                    node.clear_targetcustominfo();
                    node.clear_custominfo();
                }
            }
            auto nextInfo = roleIter.second.mutable_nextinstanceinfo();
            if (nextInfo != nullptr) {
                for (auto &node : *(nextInfo->mutable_replicanodes())) {
                    node.clear_targetcustominfo();
                    node.clear_custominfo();
                }
            }
        }
    }
}

void SchedulerServiceImpl::reRangeGroupStatus(map<string, carbon::GroupStatus> &groupStatusMap) {
    for (auto it = groupStatusMap.begin(); it != groupStatusMap.end();) {
        auto tokens = autil::StringUtil::split(it->first, ".");
        if (tokens.size() > 1) {
            groupStatusMap[tokens[0]].roleStatuses.insert(it->second.roleStatuses.begin(),
                                                          it->second.roleStatuses.end());
            it = groupStatusMap.erase(it);
        } else {
            it++;
        }
    }
}

void SchedulerServiceImpl::fetchFromCarbon() {
    std::map<std::string, carbon::GroupStatus> groupStatusMap;
    _roleManager->getGroupStatuses(groupStatusMap);
    JsonNodeRef::JsonMap status;
    extractStatusInfo(groupStatusMap, &status);

    auto lastStatus = getStatus();
    std::string statusStr = autil::legacy::ToJsonString(status);
    AUTIL_LOG(DEBUG, "fetch from carbon, extracted status [%s]", statusStr.c_str());
    if (statusStr != lastStatus.second) {
        setStatus(status, std::move(statusStr));
    }
}

void SchedulerServiceImpl::extractStatusInfo(const std::map<std::string, carbon::GroupStatus> groupStatusMap,
                                             JsonNodeRef::JsonMap *statusInfo) {
    for (const auto &[groupId, groupStatus] : groupStatusMap) {
        auto groupStatusInfo = JsonNodeRef::JsonMap();
        for (const auto &[roleId, roleStatus] : groupStatus.roleStatuses) {
            auto roleStatusInfo = JsonNodeRef::JsonArray();
            const auto &nodes = roleStatus.nextinstanceinfo().replicanodes();
            for (size_t i = 0; i < nodes.size(); i++) {
                const auto &node = nodes[i];
                if (!node.isbackup()) {
                    extractNodeStatusInfo(node, &roleStatusInfo);
                }
            }
            groupStatusInfo[roleId] = roleStatusInfo;
        }
        (*statusInfo)[groupId] = groupStatusInfo;
    }
}

void SchedulerServiceImpl::extractNodeStatusInfo(const carbon::proto::ReplicaNode &node,
                                                 JsonNodeRef::JsonArray *roleStatusInfo) {
    const std::string &serviceInfo = node.serviceinfo();
    std::string topoInfoStr = getCm2TopoStr(serviceInfo);
    if (topoInfoStr.empty()) {
        return;
    }
    const auto &topoStrVec = StringTokenizer::constTokenize(StringView(topoInfoStr.data(), topoInfoStr.length()), "|");
    for (const auto &topoStr : topoStrVec) {
        string bizName;
        int32_t partCnt = 0;
        int32_t partId = 0;
        int64_t version = 0;
        int32_t grpcPort = -1;
        bool supportHeartbeat = false;
        auto localTopoInfo = JsonNodeRef::JsonMap();
        if (parseCm2TopoStr(topoStr.to_string(), bizName, partCnt, partId, version, grpcPort, supportHeartbeat)) {
            localTopoInfo["ip"] = node.ip();
            localTopoInfo["biz_name"] = bizName;
            localTopoInfo["part_count"] = partCnt;
            localTopoInfo["part_id"] = partId;
            localTopoInfo["version"] = version;
            localTopoInfo["grpc_port"] = grpcPort; // -1 is invalid
            roleStatusInfo->push_back(localTopoInfo);
        }
    }
}

string SchedulerServiceImpl::getCm2TopoStr(const std::string &serviceInfo) {
    if (serviceInfo.empty()) {
        return "";
    }
    JsonNodeRef::JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, serviceInfo);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(WARN, "parse serviceInfo [%s] failed,  error[%s]", serviceInfo.c_str(), e.what());
        return "";
    }
    auto cm2Iter = jsonMap.find("cm2");
    if (cm2Iter == jsonMap.end()) {
        return "";
    }
    auto &cm2 = AnyCast<JsonNodeRef::JsonMap>(cm2Iter->second);
    auto topoInfoIter = cm2.find("topo_info");
    if (topoInfoIter == cm2.end()) {
        return "";
    }
    return AnyCast<string>(topoInfoIter->second);
}

bool SchedulerServiceImpl::parseCm2TopoStr(const std::string &topoStr,
                                           std::string &bizName,
                                           int32_t &partCnt,
                                           int32_t &partId,
                                           int64_t &version,
                                           int32_t &grpcPort,
                                           bool &supportHeartbeat) {
    const auto &topoInfoVec = StringTokenizer::constTokenize(autil::StringView(topoStr.data(), topoStr.length()), ":");
    if (topoInfoVec.size() < 8) {
        return false;
    }
    bizName = topoInfoVec[0].to_string();
    partCnt = StringConvertor::atoi<int32_t>(topoInfoVec[1].data(), topoInfoVec[1].length());
    partId = StringConvertor::atoi<int32_t>(topoInfoVec[2].data(), topoInfoVec[2].length());
    version = StringConvertor::atoi<int64_t>(topoInfoVec[3].data(), topoInfoVec[3].length());
    grpcPort = StringConvertor::atoi<int32_t>(topoInfoVec[6].data(), topoInfoVec[6].length());
    supportHeartbeat = false;
    if (!StringUtil::fromString<bool>(topoInfoVec[7].to_string(), supportHeartbeat)) {
        return false;
    }
    return true;
}

void SchedulerServiceImpl::commitToCarbon() {
    auto s = _targetGenerator->genTarget();
    if (!s.is_ok()) {
        AUTIL_LOG(WARN, "gen target failed, error msg: %s", s.get_error().message().c_str());
        return;
    }
    auto lastStatus = getStatus();
    JsonNodeRef::JsonMap *statusInfo = nullptr;
    if (_localCm2Mode) {
        statusInfo = &lastStatus.first;
    }

    if (_lastCommitTargetStr == autil::legacy::ToJsonString(s.get()) &&
        (!_localCm2Mode || _lastCommitStatusStr == autil::legacy::ToJsonString(statusInfo))) {
        return;
    }

    if (!_carbonAdapter->commitToCarbon(s.get(), statusInfo)) {
        AUTIL_LOG(WARN, "commit to carbon failed");
    } else {
        _lastCommitTargetStr = autil::legacy::ToJsonString(s.get());
        if (_localCm2Mode) {
            _lastCommitStatusStr = autil::legacy::ToJsonString(statusInfo);
        }
    }
}

void SchedulerServiceImpl::setStatus(const JsonNodeRef::JsonMap &status, std::string statusStr) {
    autil::ScopedWriteLock lock(_statusLock);
    _status = status;
    _lastStatusStr = std::move(statusStr);
}

std::pair<JsonNodeRef::JsonMap, std::string> SchedulerServiceImpl::getStatus() const {
    autil::ScopedReadLock lock(_statusLock);
    return {JsonNodeRef::copy(_status), _lastStatusStr};
}

} // namespace suez
