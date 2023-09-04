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
#include "service/CarbonMasterServiceImpl.h"
#include "carbon/ErrorDefine.h"
#include "autil/ClosureGuard.h"
#include "autil/legacy/jsonizable.h"
#include "monitor/MonitorUtil.h"

using namespace std;
USE_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(common);

BEGIN_CARBON_NAMESPACE(service);

CARBON_LOG_SETUP(service, CarbonMasterServiceImpl);

CarbonMasterServiceImpl::CarbonMasterServiceImpl(
        const CarbonDriverPtr &carbonDriverPtr)
{
    _carbonDriverPtr = carbonDriverPtr;
}

CarbonMasterServiceImpl::~CarbonMasterServiceImpl() {
}

#define CHECK_CARBON_MASTER_READY() do {                                \
        if (!_carbonDriverPtr->isInited()) {                            \
            ErrorInfo errorInfo;                                        \
            errorInfo.errorCode = EC_CARBON_MASTER_NOT_READY;           \
            errorInfo.errorMsg = "carbon master is not ready";          \
            CARBON_LOG(ERROR, "%s", errorInfo.errorMsg.c_str());        \
            *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
            return;                                                     \
        }                                                               \
    } while (0)

#define EXTRACT_PARAMS(p, field_name) {                                 \
        try {                                                           \
            autil::legacy::FromJsonString(p, request->field_name());    \
        } catch (const autil::legacy::ExceptionBase &e) {               \
            ErrorInfo errorInfo;                                        \
            errorInfo.errorCode = EC_DESERIALIZE_FAILED;                \
            errorInfo.errorMsg = "call fromJsonString failed";          \
            CARBON_LOG(ERROR, "extract params failed, error:%s", e.what()); \
            *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
            return;                                                     \
        }                                                               \
    }

#define CALL_PROXY(func, ...) {                                          \
        ErrorInfo errorInfo;                                            \
        _carbonDriverPtr->getProxy()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

#define CALL_WITH_RET(func, ret, ...) {                                           \
        ErrorInfo errorInfo;                                            \
        ret = _carbonDriverPtr->getProxy()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

void CarbonMasterServiceImpl::addGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::AddGroupRequest* request,
        ::carbon::proto::AddGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    carbon::monitor::TagMap tags {{"service", "addGroup"}};
    REPORT_USED_TIME(METRIC_SERVICE_LATENCY, tags);
    CARBON_LOG(INFO, "add group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    GroupPlan groupPlan;
    EXTRACT_PARAMS(groupPlan, groupplan);
    VersionInfo versionInfo;
    CALL_WITH_RET(addGroup, versionInfo.version, groupPlan);
}

void CarbonMasterServiceImpl::updateGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::UpdateGroupRequest* request,
        ::carbon::proto::UpdateGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    carbon::monitor::TagMap tags {{"service", "updateGroup"}};
    REPORT_USED_TIME(METRIC_SERVICE_LATENCY, tags);
    CARBON_LOG(INFO, "update group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    GroupPlan groupPlan;
    EXTRACT_PARAMS(groupPlan, groupplan);
    VersionInfo versionInfo;
    CALL_WITH_RET(updateGroup, versionInfo.version, groupPlan);
}

void CarbonMasterServiceImpl::deleteGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::DeleteGroupRequest* request,
        ::carbon::proto::DeleteGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    carbon::monitor::TagMap tags {{"service", "deleteGroup"}};
    REPORT_USED_TIME(METRIC_SERVICE_LATENCY, tags);
    CARBON_LOG(INFO, "delete group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string groupId = request->groupid();
    CALL_PROXY(delGroup, groupId);
}

void CarbonMasterServiceImpl::getGroupStatus(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::GetGroupStatusRequest* request,
        ::carbon::proto::GetGroupStatusResponse* response,
        ::google::protobuf::Closure* done)
{
    carbon::monitor::TagMap tags {{"service", "getGroupStatus"}};
    REPORT_USED_TIME(METRIC_SERVICE_LATENCY, tags);
    CARBON_LOG(INFO, "get group status, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

    vector<groupid_t> groupIds;
    for (int32_t i = 0; i < request->groupids_size(); i++) {
        groupIds.push_back(request->groupids(i));
    }
    
    vector<GroupStatus> groupStatusVect;
    _carbonDriverPtr->getProxy()->getGroupStatusList(groupIds, groupStatusVect);
    response->set_statusinfo(FastToJsonString(groupStatusVect, true)); 
}

void CarbonMasterServiceImpl::setLoggerLevel(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::SetLoggerLevelRequest* request,
        ::carbon::proto::SetLoggerLevelResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "set logger level, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    alog::Logger *logger = alog::Logger::getLogger(request->logger().c_str());
    logger->setLevel(request->level());
}

void CarbonMasterServiceImpl::getCarbonInfo(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::GetCarbonInfoRequest* request,
        ::carbon::proto::GetCarbonInfoResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "get carbon info.");
    autil::ClosureGuard closure(done);
    CarbonInfo carbonInfo = _carbonDriverPtr->getCarbonInfo();
    response->set_carboninfo(ToJsonString(carbonInfo, true));
}

void CarbonMasterServiceImpl::setGlobalConfig(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::SetGlobalConfigRequest *request,
        ::carbon::proto::SetGlobalConfigResponse *response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "set global config, request: %s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    
    GlobalConfig config;
    EXTRACT_PARAMS(config, config);

    ErrorInfo errorInfo;
    GlobalConfigManagerPtr globalConfigManagerPtr = _carbonDriverPtr->getGlobalConfigManager();
    if (globalConfigManagerPtr == NULL) {
        errorInfo.errorCode = EC_CARBON_MASTER_NOT_READY;
        errorInfo.errorMsg = "sys config manager not ready";
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
        CARBON_LOG(ERROR, "set global config failed, error: %s", errorInfo.errorMsg.c_str());
        return;
    }
    globalConfigManagerPtr->setConfig(config, &errorInfo);
    if (errorInfo.errorCode != EC_NONE) { 
        CARBON_LOG(ERROR, "set global config failed, error: %s", errorInfo.errorMsg.c_str());
    }
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
    return;
}

void CarbonMasterServiceImpl::getGlobalConfig(
    ::google::protobuf::RpcController* controller,
                        const ::carbon::proto::GetGlobalConfigRequest *request,
                        ::carbon::proto::GetGlobalConfigResponse *response,
                        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "get global config");
    autil::ClosureGuard closure(done);

    ErrorInfo errorInfo;
    GlobalConfigManagerPtr globalConfigManagerPtr = _carbonDriverPtr->getGlobalConfigManager();
    if (globalConfigManagerPtr == NULL) {
        errorInfo.errorCode = EC_CARBON_MASTER_NOT_READY;
        errorInfo.errorMsg = "sys config manager not ready";
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
        CARBON_LOG(ERROR, "set global config failed, error: %s", errorInfo.errorMsg.c_str());
        return;
    }
    const GlobalConfig &config = globalConfigManagerPtr->getConfig();
    response->set_config(ToJsonString(config, true));
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
    return;
}

END_CARBON_NAMESPACE(service);

