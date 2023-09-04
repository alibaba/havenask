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
#include "monitor/MonitorUtil.h"
#include "autil/legacy/jsonizable.h"

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

#define CALL(func, ...) {                                          \
        ErrorInfo errorInfo;                                            \
        _carbonDriverPtr->getGroupPlanManager()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

#define CALL_ROUTER(func, ...) {                                          \
        ErrorInfo errorInfo;                                            \
        _carbonDriverPtr->getRouter()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

#define CALL_WITH_RET(func, ret, ...) {                                           \
        ErrorInfo errorInfo;                                            \
        ret = _carbonDriverPtr->getRouter()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

void CarbonMasterServiceImpl::addGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::AddGroupRequest* request,
        ::carbon::proto::AddGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "add group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    GroupPlan groupPlan;
    EXTRACT_PARAMS(groupPlan, groupplan);
    VersionInfo versionInfo;
    CALL_WITH_RET(addGroup, versionInfo.version, groupPlan);
    *response->mutable_versioninfo() = autil::legacy::ToJsonString(versionInfo, true);
}

void CarbonMasterServiceImpl::updateGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::UpdateGroupRequest* request,
        ::carbon::proto::UpdateGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "update group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    GroupPlan groupPlan;
    EXTRACT_PARAMS(groupPlan, groupplan);
    VersionInfo versionInfo;
    CALL_WITH_RET(updateGroup, versionInfo.version, groupPlan);
    *response->mutable_versioninfo() = autil::legacy::ToJsonString(versionInfo, true);
}

void CarbonMasterServiceImpl::deleteGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::DeleteGroupRequest* request,
        ::carbon::proto::DeleteGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "delete group, request:%s", request->ShortDebugString().c_str());
    
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string groupId = request->groupid();
    CALL_ROUTER(delGroup, groupId);
}

void CarbonMasterServiceImpl::getGroupStatus(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::GetGroupStatusRequest* request,
        ::carbon::proto::GetGroupStatusResponse* response,
        ::google::protobuf::Closure* done)
{
    REPORT_USED_TIME("", METRIC_GET_GROUP_STATUS_LATENCY);
    CARBON_LOG(INFO, "get group status, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

    vector<groupid_t> groupIds;
    for (int32_t i = 0; i < request->groupids_size(); i++) {
        groupIds.push_back(request->groupids(i));
    }
    
    ErrorInfo errorInfo;
    vector<GroupStatus> groupStatusVect;
    _carbonDriverPtr->getRouter()->getGroupStatusList(groupIds, &groupStatusVect, &errorInfo);
    
    response->set_statusinfo(FastToJsonString(groupStatusVect, true));
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
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

void CarbonMasterServiceImpl::createPlanElement(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::CreatePlanElementRequest* request,
        ::carbon::proto::CreatePlanElementResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "create plan element, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string path = request->path();
    string content = request->content();
    CALL(createPlan, path, content);
}

void CarbonMasterServiceImpl::readPlanElement(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::ReadPlanElementRequest* request,
        ::carbon::proto::ReadPlanElementResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "read plan element, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string path = request->path();
    string *value = response->mutable_value();
    CALL(readPlan, path, value);
}

void CarbonMasterServiceImpl::updatePlanElement(::google::protobuf::RpcController* controller,
        const ::carbon::proto::UpdatePlanElementRequest* request,
        ::carbon::proto::UpdatePlanElementResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "update plan element, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string path = request->path();
    string content = request->content();
    CALL(updatePlan, path, content);
}

void CarbonMasterServiceImpl::deletePlanElement(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::DeletePlanElementRequest* request,
        ::carbon::proto::DeletePlanElementResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "delete plan element, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string path = request->path();
    CALL(delPlan, path);
}

void CarbonMasterServiceImpl::readStatus(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::ReadStatusRequest* request,
        ::carbon::proto::ReadStatusResponse* response,
        ::google::protobuf::Closure* done)
{
}
    
void CarbonMasterServiceImpl::adjustRunningArgs(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::AdjustRunningArgsRequest* request,
        ::carbon::proto::AdjustRunningArgsResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "adjust running args, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    
    string field = request->argsfield();
    string value = request->argsvalue();
    ErrorInfo errorInfo;
    GroupManagerPtr groupManagerPtr = _carbonDriverPtr->getGroupManager();
    if (!groupManagerPtr->adjustRunningArgs(field, value)) {
        errorInfo.errorCode = EC_ADJUST_ARGS_FAILED_FOR_DEBUG;
        errorInfo.errorMsg = "adjust args failed for internal error";
    }
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
}

void CarbonMasterServiceImpl::setGlobalConfig(::google::protobuf::RpcController* controller,
        const ::carbon::proto::SetCarbonConfigRequest* request,
        ::carbon::proto::SetCarbonConfigResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "set carbon config, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

    GlobalConfig config;
    EXTRACT_PARAMS(config, config);
    ErrorInfo errorInfo;
    _carbonDriverPtr->setGlobalConfig(config, &errorInfo);
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
}

void CarbonMasterServiceImpl::getGlobalConfig(::google::protobuf::RpcController* controller,
        const ::carbon::proto::GetCarbonConfigRequest* request,
        ::carbon::proto::GetCarbonConfigResponse* response,
        ::google::protobuf::Closure* done)
{
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

    GlobalConfig config = _carbonDriverPtr->getGlobalConfig();
    response->set_config(ToJsonString(config, true)); 
}

void CarbonMasterServiceImpl::getDriverStatus(::google::protobuf::RpcController* controller,
        const ::carbon::proto::GetDriverStatusRequest* request,
        ::carbon::proto::GetDriverStatusResponse* response,
        ::google::protobuf::Closure* done) 
{
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

    ErrorInfo errorInfo;
    DriverStatus status = _carbonDriverPtr->getStatus(&errorInfo);
    response->set_status(ToJsonString(status, true)); 
}

END_CARBON_NAMESPACE(service);

