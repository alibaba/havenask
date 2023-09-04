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
#include "suez/admin/AdminServiceImpl.h"

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "autil/ClosureGuard.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "carbon/legacy/GroupStatusWrapper.h"
#include "carbon/proto/Carbon.pb.h"
#include "hippo/proto/Common.pb.h"
#include "suez/admin/AdminCmdLog.h"
#include "suez/admin/GroupDescGenerator.h"
#include "worker_framework/WorkerState.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, AdminServiceImpl);

const string AdminServiceImpl::SUEZ_STATUS = "suezStatus";
const string AdminServiceImpl::SYSTEM_INFO = "systemInfo";
const string AdminServiceImpl::ERROR = "error";

#define RPC_GUARD()                                                                                                    \
    autil::ClosureGuard guard(done);                                                                                   \
    do {                                                                                                               \
        if (!_serviceReady) {                                                                                          \
            string errMsg = "service not ready, drop rpc request";                                                     \
            response->set_errorinfo(errMsg);                                                                           \
            AUTIL_LOG(WARN, "%s", errMsg.c_str());                                                                     \
        }                                                                                                              \
    } while (0)

AdminServiceImpl::AdminServiceImpl() : _serviceReady(false), _roleManager(nullptr), _minimalGsMode(false) {
    _target["zones"] = JsonNodeRef::JsonMap();
}

AdminServiceImpl::~AdminServiceImpl() { stop(); }

bool AdminServiceImpl::start(bool isNewStart,
                             const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager,
                             std::unique_ptr<WorkerState> state) {
    _roleManager = roleManager;
    _state = std::move(state);

    if (!isNewStart && !deserialize(_target, _lastSerializedStr)) {
        _lastSerializedStr.clear();
        AUTIL_LOG(ERROR, "recover admin target state failed");
        return false;
    }

    _loopThread = LoopThread::createLoopThread(
        std::bind(&AdminServiceImpl::commitToCarbon, this), 10 * 1000 * 1000, "SuezCommit");

    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create commit to carbon thread failed");
        return false;
    }

    _serviceReady = true;
    _minimalGsMode = autil::EnvUtil::getEnv("minimalGsMode", false);

    return true;
}

void AdminServiceImpl::stop() {
    _serviceReady = false;
    _loopThread.reset();
}

void AdminServiceImpl::create(::google::protobuf::RpcController *controller,
                              const CreateRequest *request,
                              Response *response,
                              ::google::protobuf::Closure *done) {
    operate<CreateRequest>(request, response, &AdminServiceImpl::doCreate, done);
}

void AdminServiceImpl::update(::google::protobuf::RpcController *controller,
                              const UpdateRequest *request,
                              Response *response,
                              ::google::protobuf::Closure *done) {
    operate<UpdateRequest>(request, response, &AdminServiceImpl::doUpdate, done);
}

void AdminServiceImpl::del(::google::protobuf::RpcController *controller,
                           const DeleteRequest *request,
                           Response *response,
                           ::google::protobuf::Closure *done) {
    operate<DeleteRequest>(request, response, &AdminServiceImpl::doDel, done);
}

void AdminServiceImpl::batch(::google::protobuf::RpcController *controller,
                             const BatchRequest *request,
                             Response *response,
                             ::google::protobuf::Closure *done) {
    operate<BatchRequest>(request, response, &AdminServiceImpl::doBatch, done);
}

void AdminServiceImpl::read(::google::protobuf::RpcController *controller,
                            const ReadRequest *request,
                            ReadResponse *response,
                            ::google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    RPC_GUARD();

    if (!doRead(request, response)) {
        logAccess("read", false, request->ShortDebugString(), startTime);
    }
}

void AdminServiceImpl::releaseSlot(::google::protobuf::RpcController *controller,
                                   const ::suez::ReleaseSlotRequest *request,
                                   ::suez::Response *response,
                                   ::google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    RPC_GUARD();

    vector<hippo::proto::SlotId> slotIds;
    for (int i = 0; i < request->slotids_size(); i++) {
        slotIds.push_back(request->slotids(i));
    }
    hippo::proto::PreferenceDescription preference;
    preference.set_type(hippo::proto::PreferenceDescription::PREF_PROHIBIT);
    if (!_roleManager->releseSlots(slotIds, preference)) {
        string errMsg = "release slotIds failed.";
        response->set_errorinfo(errMsg);
        logAccess("releaseSlot", false, request->ShortDebugString(), startTime);
    } else {
        logAccess("releaseSlot", true, request->ShortDebugString(), startTime);
    }
}

void AdminServiceImpl::setLoggerLevel(::google::protobuf::RpcController *controller,
                                      const ::suez::SetLoggerLevelRequest *request,
                                      ::suez::SetLoggerLevelResponse *response,
                                      ::google::protobuf::Closure *done) {
    AUTIL_LOG(INFO, "set logger level, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    alog::Logger *logger = alog::Logger::getLogger(request->logger().c_str());
    logger->setLevel(request->level());
}

void AdminServiceImpl::reRangeGroupStatus(map<string, carbon::GroupStatus> &groupStatusMap) {
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

bool AdminServiceImpl::getSystemInfo(const string &path, JsonNodeRef::JsonMap &target, bool minimalGsMode) {
    vector<string> nodes = JsonNodeRef::split(path);
    if (nodes.empty()) {
        AUTIL_LOG(ERROR, "empty path %s", path.c_str());
        return false;
    }
    string rootNode = nodes[0];
    vector<string> groupName;
    if (nodes.size() > 1) {
        string zoneName = nodes[1];
        autil::ScopedReadLock lock(_targetLock);
        if (!GroupDescGenerator::getGroupNameForSystemInfo(getTargetNoLock(), zoneName, groupName)) {
            return false;
        }
    }
    map<string, carbon::GroupStatus> groupStatusMap;
    _roleManager->getGroupStatuses(groupName, groupStatusMap);
    if (minimalGsMode) {
        simplifyGroupsStatus(groupStatusMap);
    }

    reRangeGroupStatus(groupStatusMap);
    try {
        target[rootNode] = ToJson(groupStatusMap);
    } catch (const autil::legacy::ExceptionBase &e) {
        string errMsg = "to json failed";
        AUTIL_LOG(WARN, "%s, %s", errMsg.c_str(), e.what());
        return false;
    }
    return true;
}

void AdminServiceImpl::simplifyGroupsStatus(map<string, carbon::GroupStatus> &groupStatusMap) {
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

bool AdminServiceImpl::doRead(const ReadRequest *request, ReadResponse *response) {
    string path = request->nodepath();
    JsonNodeRef::JsonMap target;
    auto responseTarget = [&](const JsonNodeRef::JsonMap &target) -> bool {
        // NOTE: JsonNodeRef requires a writable JsonMap but the map is read-only actually.
        // And don't write JsonMap here. See JsonNodeRef::getParent
        JsonNodeRef ref(const_cast<JsonNodeRef::JsonMap &>(target));
        Any *value = ref.read(path);
        if (value == nullptr) {
            string errMsg = "read path [" + path + "] failed, error [" + ref.getLastError() + "]";
            AUTIL_LOG(WARN, "%s", errMsg.c_str());
            response->set_errorinfo(errMsg);
            return false;
        }
        string valueStr = FastToJsonString(*value);
        response->set_nodevalue(valueStr);
        return true;
    };
    if (0 == path.find(SYSTEM_INFO) || 0 == path.find(SUEZ_STATUS)) {
        bool minimalGsMode = _minimalGsMode;
        if (request->has_minimalgsmode()) {
            minimalGsMode = request->minimalgsmode();
        }
        if (!getSystemInfo(path, target, minimalGsMode)) {
            string errMsg = "read path [" + path + "] failed";
            AUTIL_LOG(WARN, "%s", errMsg.c_str());
            response->set_errorinfo(errMsg);
            return false;
        }
        if (0 == path.find(SUEZ_STATUS)) {
            response->set_format(true);
        }
    } else if (ERROR == path) {
        target[ERROR] = getCommitError();
    } else {
        autil::ScopedReadLock lock(_targetLock);
        const auto &target = getTargetNoLock();
        return responseTarget(target);
    }
    return responseTarget(target);
}

template <typename RequestType>
void AdminServiceImpl::operate(const RequestType *request,
                               Response *response,
                               const std::function<bool(const RequestType *, Response *, JsonNodeRef::JsonMap &)> &func,
                               ::google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    RPC_GUARD();
    bool ret = false;

    // make sure everything is serialize
    ScopedLock lock(_operateLock);
    auto lastTarget = getTarget();
    auto &target = lastTarget.first;
    const auto &lastTargetStr = lastTarget.second;
    if (func(request, response, target)) {
        auto targetStr = FastToJsonString(target);
        if (targetStr != lastTargetStr) {
            if (serialize(targetStr)) {
                _targetRecorder.newAdminTarget(targetStr);
                setTarget(target, std::move(targetStr));
                ret = true;
            } else {
                response->set_errorinfo("serialize to zk failed");
            }
        } else {
            AUTIL_LOG(DEBUG, "target unchanged");
            ret = true;
        }
    }
    logAccess("deal with " + request->GetTypeName(), ret, request->ShortDebugString(), startTime);
}

bool AdminServiceImpl::doUpdate(const UpdateRequest *request, Response *response, JsonNodeRef::JsonMap &target) {
    string path = request->nodepath();
    string value = request->nodevalue();
    Any json;
    try {
        FastFromJsonString(json, value);
    } catch (const autil::legacy::ExceptionBase &e) {
        string errMsg = "parse json failed [" + value + "]";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        response->set_errorinfo(errMsg);
        return false;
    }
    JsonNodeRef ref(target);
    if (!ref.update(path, json)) {
        string errMsg = "update path [" + path + "] failed, error [" + ref.getLastError() + "]";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        response->set_errorinfo(errMsg);
        return false;
    }
    return true;
}

bool AdminServiceImpl::doDel(const DeleteRequest *request, Response *response, JsonNodeRef::JsonMap &target) {
    string path = request->nodepath();
    JsonNodeRef ref(target);
    if (!ref.del(path)) {
        string errMsg = "delete path [" + path + "] failed, error [" + ref.getLastError() + "]";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        response->set_errorinfo(errMsg);
        return false;
    } else {
        AUTIL_LOG(INFO, "delete path [%s] success", path.c_str());
    }
    return true;
}

bool AdminServiceImpl::doCreate(const CreateRequest *request, Response *response, JsonNodeRef::JsonMap &target) {
    string path = request->nodepath();
    string value = request->nodevalue();
    bool recursive = request->recursive();
    Any json;
    try {
        FastFromJsonString(json, value);
    } catch (const autil::legacy::ExceptionBase &e) {
        string errMsg = "parse json failed [" + value + "]";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        response->set_errorinfo(errMsg);
        return false;
    }
    JsonNodeRef ref(target);
    if (!ref.create(path, json, recursive)) {
        string errMsg = "create path [" + path + "] failed, error [" + ref.getLastError() + "]";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        response->set_errorinfo(errMsg);
        return false;
    }
    return true;
}

bool AdminServiceImpl::doBatch(const BatchRequest *request, Response *response, JsonNodeRef::JsonMap &target) {
    for (int i = 0; i < request->createrequests_size(); i++) {
        if (!doCreate(&request->createrequests(i), response, target)) {
            return false;
        }
    }
    for (int i = 0; i < request->updaterequests_size(); i++) {
        if (!doUpdate(&request->updaterequests(i), response, target)) {
            return false;
        }
    }
    for (int i = 0; i < request->deleterequests_size(); i++) {
        if (!doDel(&request->deleterequests(i), response, target)) {
            return false;
        }
    }
    return true;
}

bool AdminServiceImpl::serialize(const std::string &targetStr) {
    std::string compressedTarget = targetStr;
    if (!compress(compressedTarget)) {
        AUTIL_LOG(ERROR, "compress target %s failed", compressedTarget.c_str());
        return false;
    }
    auto ec = _state->write(compressedTarget);
    if (ec == WorkerState::EC_FAIL) {
        AUTIL_LOG(ERROR, "serialize target %s failed", targetStr.c_str());
        return false;
    }
    return true;
}

bool AdminServiceImpl::deserialize(JsonNodeRef::JsonMap &target, std::string &targetStr) {
    std::string compressedTargetStr;
    auto ec = _state->read(compressedTargetStr);
    if (ec == WorkerState::EC_NOT_EXIST) {
        AUTIL_LOG(INFO, "admin status file does not exist");
        return true;
    } else if (ec == WorkerState::EC_FAIL) {
        AUTIL_LOG(INFO, "read from admin status file failed");
        return false;
    } else {
        if (!decompress(compressedTargetStr)) {
            AUTIL_LOG(ERROR, "decompress target %s failed", targetStr.c_str());
            return false;
        }
        targetStr = std::move(compressedTargetStr);
        Any any;
        try {
            FastFromJsonString(any, targetStr);
        } catch (const autil::legacy::ExceptionBase &e) {
            AUTIL_LOG(WARN, "parse admin status [%s] failed,  error[%s]", targetStr.c_str(), e.what());
            return false;
        }
        JsonNodeRef::JsonMap *jsonMap = AnyCast<JsonNodeRef::JsonMap>(&any);
        if (!jsonMap) {
            AUTIL_LOG(WARN, "cast admin status [%s] failed", targetStr.c_str());
            return false;
        }
        target = *jsonMap;
        return true;
    }
}

void AdminServiceImpl::commitToCarbon() {
    auto lastTarget = getTarget();
    if (lastTarget.second == _lastCommitStr) {
        return;
    }

    const auto &target = lastTarget.first;
    map<string, carbon::GroupDesc> groups;
    try {
        GroupDescGenerator::generateGroupDescs(target, groups);
    } catch (const autil::legacy::ExceptionBase &e) {
        setCommitError(string("generate group desc failed,  error ") + e.what());
        return;
    }

    if (!_roleManager->setGroups(groups)) {
        setCommitError("set carbon target fail");
        return;
    }
    AUTIL_LOG(INFO, "commit to carbon");
    _lastCommitStr = std::move(lastTarget.second);
    setCommitError(string());
}

void AdminServiceImpl::logAccess(const string &operate, bool success, const string &requestStr, int64_t startTime) {
    AdminCmdLog adminCmdLog;
    adminCmdLog.setOperateCmd(operate);
    adminCmdLog.setSuccess(success);
    adminCmdLog.setRequest(requestStr);
    int64_t endTime = TimeUtility::currentTime();
    adminCmdLog.setCost((endTime - startTime) / 1000);
}

void AdminServiceImpl::setCommitError(const std::string &err) {
    if (!err.empty()) {
        AUTIL_LOG(ERROR, "%s", err.c_str());
    }
    autil::ScopedLock lock(_commitErrorLock);
    _lastCommitError = err;
}

std::string AdminServiceImpl::getCommitError() const {
    autil::ScopedLock lock(_commitErrorLock);
    return _lastCommitError;
}

void AdminServiceImpl::setTarget(const JsonNodeRef::JsonMap &target, std::string targetStr) {
    autil::ScopedWriteLock lock(_targetLock);
    _target = target;
    _lastSerializedStr = std::move(targetStr);
}

std::pair<JsonNodeRef::JsonMap, std::string> AdminServiceImpl::getTarget() const {
    autil::ScopedReadLock lock(_targetLock);
    return {JsonNodeRef::copy(_target), _lastSerializedStr};
}

const JsonNodeRef::JsonMap &AdminServiceImpl::getTargetNoLock() const { return _target; }

bool AdminServiceImpl::compress(std::string &content) {
    autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
    string compressedData;
    if (!compressor.compress(content, compressedData)) {
        return false;
    }
    content = std::move(compressedData);
    return true;
}

bool AdminServiceImpl::decompress(std::string &content) {
    autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
    string decompressedData;
    if (!compressor.decompress(content, decompressedData)) {
        return false;
    }
    content = std::move(decompressedData);
    return true;
}

#undef RPC_GUARD

} // namespace suez
