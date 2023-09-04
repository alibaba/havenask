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
#include "suez/worker/DebugServiceImpl.h"

#include "autil/ClosureGuard.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/SchedulerInfo.h"
#include "suez/worker/TaskExecutor.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DebugServiceImpl);

DebugServiceImpl::DebugServiceImpl() {}

DebugServiceImpl::~DebugServiceImpl() {}

bool DebugServiceImpl::init(RpcServer *rpcServer, const std::shared_ptr<TaskExecutor> &executor) {
    AUTIL_LOG(INFO, "initing debug service");
    if (executor == nullptr) {
        return false;
    }
    _taskExecutor = executor;
    return rpcServer->RegisterService(this);
}

void DebugServiceImpl::nextStep(google::protobuf::RpcController *controller,
                                const TargetRequest *request,
                                NextStepResponse *response,
                                google::protobuf::Closure *done) {
    autil::ClosureGuard guard(done);
    AUTIL_LOG(INFO, "debug service get next step request");
    HeartbeatTarget target;
    HeartbeatTarget finalTarget;
    SchedulerInfo schedulerInfo;
    if (!parseFromTarget(request, target, finalTarget, schedulerInfo)) {
        response->set_errcode(DBS_ERROR_TARGET_PARSE_FAIL);
        return;
    }
    bool forceSync = true;
    if (request->has_forcesync() && !request->forcesync()) {
        forceSync = false;
    }
    auto result = _taskExecutor->nextStep(target, finalTarget, schedulerInfo, forceSync);
    setResponse(result, response);
}

void DebugServiceImpl::setResponse(std::pair<bool, JsonMap> &result, NextStepResponse *response) {
    response->set_reachtarget(result.first);
    auto current = result.second;
    response->mutable_current()->set_tableinfo(ToString(current["table_info"]));
    response->mutable_current()->set_bizinfo(ToString(current["biz_info"]));
    response->mutable_current()->set_appinfo(ToString(current["app_info"]));
    response->mutable_current()->set_serviceinfo(ToString(current["service_info"]));

    if (current.find("target_version") != current.end()) {
        response->mutable_current()->set_targetversion(ToString(current["target_version"]));
        response->set_errcode(DBS_ERROR_TARGET_VERSION);
        return;
    }
    if (current.find("error_info") != current.end()) {
        response->mutable_current()->set_errorinfo(ToString(current["error_info"]));
        response->set_errcode(DBS_ERROR_UNKNOWN);
        return;
    }
    response->set_errcode(DBS_ERROR_NONE);
}

bool DebugServiceImpl::parseFromTarget(const TargetRequest *request,
                                       HeartbeatTarget &heartbeatTarget,
                                       HeartbeatTarget &finalHeartbeatTarget,
                                       SchedulerInfo &schedulerInfo) {
    string signature = request->signature();
    string target = request->custominfo();
    string global = request->globalcustominfo();
    string schedulerInfoStr = request->schedulerinfo();
    string preload = request->preload();
    string finalTarget;

    getGlobalCustomInfo(global, finalTarget);
    if (!heartbeatTarget.parseFrom(target, signature)) {
        AUTIL_LOG(WARN, "parse from target failed, target: [%s], signature [%s]", target.c_str(), signature.c_str());
        return false;
    }
    if (!finalHeartbeatTarget.parseFrom(finalTarget, "", preload == "true")) {
        AUTIL_LOG(WARN, "parse from finale target failed, final target: [%s]", finalTarget.c_str());
        finalHeartbeatTarget = heartbeatTarget;
    }
    if (!schedulerInfo.parseFrom(schedulerInfoStr)) {
        AUTIL_LOG(WARN, "parse from schedulerInfoStr:[%s] failed", schedulerInfoStr.c_str());
        return false;
    }
    return true;
}

void DebugServiceImpl::getGlobalCustomInfo(const string &globalCustomInfo, string &finalTarget) {
    JsonMap jsonMap;
    try {
        FastFromJsonString(jsonMap, globalCustomInfo);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(
            ERROR, "e[%s], getGlobalCustomInfo failed, str[%s] jsonize failed", e.what(), globalCustomInfo.c_str());
        return;
    }
    JsonMap::const_iterator it = jsonMap.find("customInfo");
    if (it == jsonMap.end()) {
        AUTIL_LOG(ERROR,
                  "getGlobalCustomInfo failed, globalCustomInfo [%s] has no field named [customInfo]",
                  globalCustomInfo.c_str());
        return;
    }
    const string *anyString = AnyCast<string>(&it->second);
    if (NULL == anyString) {
        AUTIL_LOG(ERROR,
                  "getGlobalCustomInfo failed, customInfo is not a string, globalCustomInfo[%s]",
                  globalCustomInfo.c_str());
        return;
    }

    finalTarget = *anyString;
    AUTIL_LOG(DEBUG, "finalTarget [%s]", finalTarget.c_str());
}

} // namespace suez
