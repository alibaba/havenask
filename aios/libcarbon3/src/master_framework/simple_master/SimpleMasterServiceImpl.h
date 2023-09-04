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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTERSERVICEIMPL_H
#define MASTER_FRAMEWORK_SIMPLEMASTERSERVICEIMPL_H

#include "common/Log.h"
#include "master_framework/common.h"
#include "master_framework/SimpleMaster.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "carbon/Status.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterServiceImpl : public proto::SimpleMasterService
{
public:
    SimpleMasterServiceImpl(SimpleMaster *simpleMaster);
    ~SimpleMasterServiceImpl();
private:
    SimpleMasterServiceImpl(const SimpleMasterServiceImpl &);
    SimpleMasterServiceImpl& operator=(const SimpleMasterServiceImpl &);
public:
    /* override */
    void updateAppPlan(
            ::google::protobuf::RpcController *controller,
            const proto::UpdateAppPlanRequest *request,
            proto::UpdateAppPlanResponse *response,
            ::google::protobuf::Closure *done);

    /* override */
    void updateRoleProperties(
            ::google::protobuf::RpcController *controller,
            const proto::UpdateRolePropertiesRequest *request,
            proto::UpdateRolePropertiesResponse *response,
            ::google::protobuf::Closure *done);

    /* override */
    void getRoleSlots(
            ::google::protobuf::RpcController *controller,
            const proto::GetRoleSlotsRequest *request,
            proto::GetRoleSlotsResponse *response,
            ::google::protobuf::Closure *done);
    
    /* override */
    void getAllRoleSlots(
            ::google::protobuf::RpcController *controller,
            const proto::GetAllRoleSlotsRequest *request,
            proto::GetAllRoleSlotsResponse *response,
            ::google::protobuf::Closure *done);
    
    /* override */
    void releaseSlots(
            ::google::protobuf::RpcController *controller,
            const proto::ReleaseSlotsRequest *request,
            proto::ReleaseSlotsResponse *response,
            ::google::protobuf::Closure *done);

private:
    std::vector<carbon::SlotInfoJsonizeWrapper> translateSlotInfosWrapper(
            const std::vector<hippo::SlotInfo>& slotInfos);


private:
    SimpleMaster *_simpleMaster;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterServiceImpl);

END_MASTER_FRAMEWORK_NAMESPACE(master);

#endif //MASTER_FRAMEWORK_SIMPLEMASTERSERVICEIMPL_H
