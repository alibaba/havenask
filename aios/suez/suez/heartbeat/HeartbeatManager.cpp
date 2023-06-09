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
#include "suez/heartbeat/HeartbeatManager.h"

#include <cstddef>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/ClosureGuard.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "aios/network/http_arpc/HTTPRPCControllerBase.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SchedulerInfo.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace http_arpc;

namespace suez {

const string HeartbeatManager::CUSTOM_INFO = "customInfo";

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, HeartbeatManager);

HeartbeatManager::HeartbeatManager() : _preload(false) {}

HeartbeatManager::~HeartbeatManager() {}

bool HeartbeatManager::init(RpcServer *server,
                            const string &carbonIdentifier,
                            const string &procInstanceId,
                            const string &packageCheckSum) {
    _carbonIdentifier = carbonIdentifier;
    _procInstanceId = procInstanceId;
    _packageCheckSum = packageCheckSum;
    AUTIL_LOG(INFO, "WORKER_IDENTIFIER_FOR_CARBON [%s], invalid heartbeat will be rejected", _carbonIdentifier.c_str());
    return server->RegisterService(this);
}

void HeartbeatManager::setCurrent(const JsonMap &current, const JsonMap &serviceInfo, const string &signature) {
    string currentStr = ToJsonString(current);
    string serviceInfoStr = ToJsonString(serviceInfo);
    ScopedLock lock(_mutex);
    if (_currentSnapShot != currentStr) {
        _targetRecorder.newCurrent(currentStr);
    }
    _currentSnapShot = currentStr;
    _currentSignature = signature;
    _currentServiceInfo = serviceInfoStr;
}

bool HeartbeatManager::getTarget(HeartbeatTarget &heartbeatTarget,
                                 HeartbeatTarget &finalHeartbeatTarget,
                                 SchedulerInfo &schedulerInfo) const {
    string target, signature, finalTarget, schedulerInfoStr;
    bool preload = false;
    {
        ScopedLock lock(_mutex);
        target = _targetSnapShot;
        signature = _signature;
        finalTarget = _finalTargetSnapShot;
        schedulerInfoStr = _schedulerInfoStr;
        preload = _preload;
    }
    if (!heartbeatTarget.parseFrom(target, signature)) {
        AUTIL_LOG(WARN, "parse from target failed, target: [%s], signature [%s]", target.c_str(), signature.c_str());
        return false;
    }
    if (!finalHeartbeatTarget.parseFrom(finalTarget, "", preload)) {
        AUTIL_LOG(WARN, "parse from finale target failed, final target: [%s]", finalTarget.c_str());
        finalHeartbeatTarget = heartbeatTarget;
    }
    if (!schedulerInfo.parseFrom(schedulerInfoStr)) {
        AUTIL_LOG(WARN, "parse from schedulerInfoStr:[%s] failed", schedulerInfoStr.c_str());
        return false;
    }
    return true;
}

void HeartbeatManager::heartbeat(::google::protobuf::RpcController *controller,
                                 const TargetRequest *request,
                                 CurrentResponse *response,
                                 ::google::protobuf::Closure *done) {
    autil::ClosureGuard guard(done);
    auto processVersion = request->porcessversion();
    if (!processVersion.empty() && !processVersionMatch(processVersion)) {
        AUTIL_LOG(INFO,
                  "heartbeat rejected because process version not match."
                  " processVersion:%s, instanceId:%s, packageCheckSum:%s",
                  request->porcessversion().c_str(),
                  _procInstanceId.c_str(),
                  _packageCheckSum.c_str());
        return;
    }
    auto identifier = request->identifier();
    if (!_carbonIdentifier.empty() && identifier != _carbonIdentifier) {
        string adminAddress = "unknown";
        auto *httpController = dynamic_cast<HTTPRPCControllerBase *>(controller);
        if (httpController != NULL) {
            adminAddress = httpController->GetAddr();
        }
        AUTIL_LOG(INFO,
                  "heartbeat rejected, adminAddress: [%s], request [%s]",
                  adminAddress.c_str(),
                  ProtoJsonizer::toJsonString(*request).c_str());
        return;
    }
    ScopedLock lock(_mutex);
    if (_targetSnapShot != request->custominfo()) {
        _targetRecorder.newTarget(request->custominfo());
    }
    _targetSnapShot = request->custominfo();
    if (_schedulerInfoStr != request->schedulerinfo()) {
        _targetRecorder.newSchedulerInfo(request->schedulerinfo());
    }
    _schedulerInfoStr = request->schedulerinfo();

    // getGlobalCustomInfo's return value ignored
    getGlobalCustomInfo(request->globalcustominfo());
    _signature = request->signature();
    _preload = request->preload() == "true";
    response->set_custominfo(_currentSnapShot);
    response->set_signature(_currentSignature);
    response->set_serviceinfo(_currentServiceInfo);
}

void HeartbeatManager::setLoggerLevel(::google::protobuf::RpcController *controller,
                                      const ::suez::SetLoggerLevelRequest *request,
                                      ::suez::SetLoggerLevelResponse *response,
                                      ::google::protobuf::Closure *done) {
    autil::ClosureGuard guard(done);
    AUTIL_LOG(INFO, "set logger level, request:%s", request->ShortDebugString().c_str());
    alog::Logger *logger = alog::Logger::getLogger(request->logger().c_str());
    logger->setLevel(request->level());
}

bool HeartbeatManager::getGlobalCustomInfo(const string &globalCustomInfo) {
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, globalCustomInfo);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(
            ERROR, "e[%s], getGlobalCustomInfo failed, str[%s] jsonize failed", e.what(), globalCustomInfo.c_str());
        return false;
    }
    JsonMap::const_iterator it = jsonMap.find(CUSTOM_INFO);
    if (it == jsonMap.end()) {
        AUTIL_LOG(ERROR,
                  "getGlobalCustomInfo failed, globalCustomInfo [%s] has no field named [%s]",
                  globalCustomInfo.c_str(),
                  CUSTOM_INFO.c_str());
        return false;
    }
    const string *anyString = AnyCast<string>(&it->second);
    if (NULL == anyString) {
        AUTIL_LOG(ERROR,
                  "getGlobalCustomInfo failed, customInfo is not a string, globalCustomInfo[%s]",
                  globalCustomInfo.c_str());
        return false;
    }

    if (_finalTargetSnapShot != *anyString) {
        _targetRecorder.newFinalTarget(*anyString);
    }

    _finalTargetSnapShot = *anyString;
    AUTIL_LOG(DEBUG, "finalTarget [%s]", _finalTargetSnapShot.c_str());
    return true;
}

bool HeartbeatManager::processVersionMatch(const std::string &processVersion) {
    if (_packageCheckSum.empty() || _procInstanceId.empty()) {
        AUTIL_LOG(DEBUG, "packageCheckSum or procInstanceId is empty, not need check");
        return true;
    }
    // expect format: packageCheckSum:instanceId1#instanceId2....
    // expect packageCheckSum match and instanceId in list
    auto tokens = StringUtil::split(processVersion, ":");
    if (tokens.size() != 2) {
        AUTIL_LOG(WARN, "process version format is invalid");
        return false;
    }
    auto packageCheckSum = tokens[0];
    auto instanceIds = StringUtil::split(tokens[1], "#");
    if (_packageCheckSum != packageCheckSum) {
        AUTIL_LOG(WARN, "pakcage checksum not match");
        return false;
    }
    for (auto &id : instanceIds) {
        if (id == _procInstanceId) {
            return true;
        }
    }
    AUTIL_LOG(WARN, "instanceid not match");
    return false;
}

} // namespace suez
