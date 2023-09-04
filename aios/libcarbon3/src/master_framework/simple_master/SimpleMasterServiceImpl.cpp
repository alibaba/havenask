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
#include "simple_master/SimpleMasterServiceImpl.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, SimpleMasterServiceImpl);


#define EXTRACT_PARAMS(p, field_name) {                                 \
    try {                                                           \
        autil::legacy::FromJsonString(p, request->field_name());    \
    } catch (const autil::legacy::ExceptionBase &e) {               \
        MF_LOG(ERROR, "extract params failed, error:%s", e.what()); \
        response->mutable_errorinfo()->set_errorcode(proto::ErrorInfo::ERROR_SERIALIZE_FAILED); \
        response->mutable_errorinfo()->set_errormsg("parse json str failed"); \
        done->Run(); \
        return;                                                     \
    }                                                               \
}

SimpleMasterServiceImpl::SimpleMasterServiceImpl(
        SimpleMaster *simpleMaster)
    : _simpleMaster(simpleMaster)
{
}

SimpleMasterServiceImpl::~SimpleMasterServiceImpl() {
}

void SimpleMasterServiceImpl::updateAppPlan(
        ::google::protobuf::RpcController *controller,
        const proto::UpdateAppPlanRequest *request,
        proto::UpdateAppPlanResponse *response,
        ::google::protobuf::Closure *done)
{
    MF_LOG(INFO, "update app plan, request: %s",
           request->ShortDebugString().c_str());
    _simpleMaster->updateAppPlan(request, response);
    done->Run();
}

void SimpleMasterServiceImpl::updateRoleProperties(
        ::google::protobuf::RpcController *controller,
        const proto::UpdateRolePropertiesRequest *request,
        proto::UpdateRolePropertiesResponse *response,
        ::google::protobuf::Closure *done)
{
    MF_LOG(INFO, "update role properties, request: %s",
           request->ShortDebugString().c_str());
    _simpleMaster->updateRoleProperties(request, response);
    done->Run();
}

vector<carbon::SlotInfoJsonizeWrapper> SimpleMasterServiceImpl::translateSlotInfosWrapper(const vector<hippo::SlotInfo>& slotInfos) {
    vector<carbon::SlotInfoJsonizeWrapper> slotInfoWrappers;
    for (size_t i = 0; i < slotInfos.size(); i++) {
        carbon::SlotInfoJsonizeWrapper slotInfoWrapper(slotInfos[i]);
        slotInfoWrappers.push_back(slotInfoWrapper);
    }
    return slotInfoWrappers;
}

void SimpleMasterServiceImpl::getRoleSlots(
        ::google::protobuf::RpcController *controller,
        const proto::GetRoleSlotsRequest *request,
        proto::GetRoleSlotsResponse *response,
        ::google::protobuf::Closure *done) 
{
    MF_LOG(INFO, "get role slots, request:[%s].",
           request->ShortDebugString().c_str());
    
    string roleName = request->rolename();
    const std::vector<hippo::SlotInfo> slotInfos = _simpleMaster->getRoleSlots(roleName);
    
    vector<carbon::SlotInfoJsonizeWrapper> slotInfoWrappers = translateSlotInfosWrapper(slotInfos);
    response->set_slotinfos(autil::legacy::ToJsonString(slotInfoWrappers, true));
    done->Run();
}

void SimpleMasterServiceImpl::getAllRoleSlots(
        ::google::protobuf::RpcController *controller,
        const proto::GetAllRoleSlotsRequest *request,
        proto::GetAllRoleSlotsResponse *response,
        ::google::protobuf::Closure *done)
{
    MF_LOG(INFO, "get all role slots, request:[%s].",
           request->ShortDebugString().c_str());
    
    map<string, vector<hippo::SlotInfo> > roleSlots;
    _simpleMaster->getAllRoleSlots(roleSlots);

    map<string, vector<carbon::SlotInfoJsonizeWrapper>> roleSlotWrappers;
    auto iter = roleSlots.begin();
    for ( ; iter != roleSlots.end(); iter++) {
        roleSlotWrappers[iter->first] = translateSlotInfosWrapper(iter->second);
    }
    response->set_roleslotinfos(autil::legacy::ToJsonString(roleSlotWrappers, true));
    done->Run();
}

void SimpleMasterServiceImpl::releaseSlots(
        ::google::protobuf::RpcController *controller,
        const proto::ReleaseSlotsRequest *request,
        proto::ReleaseSlotsResponse *response,
        ::google::protobuf::Closure *done)
{
    MF_LOG(INFO, "release slots, request:[%s].",
           request->ShortDebugString().c_str());
    
    vector<hippo::SlotId> slotIds;
    EXTRACT_PARAMS(slotIds, slotids);

    hippo::ReleasePreference pref;
    EXTRACT_PARAMS(pref, preference);

    _simpleMaster->releaseSlots(slotIds, pref);
    done->Run();
}

END_MASTER_FRAMEWORK_NAMESPACE(master);

