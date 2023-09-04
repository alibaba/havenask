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
#ifndef CARBON_CARBONMASTERSERVICEIMPL_H
#define CARBON_CARBONMASTERSERVICEIMPL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/CarbonDriver.h"
#include "carbon/proto/Carbon.pb.h"

BEGIN_CARBON_NAMESPACE(service);

class CarbonMasterServiceImpl : public proto::CarbonMasterService
{
public:
    CarbonMasterServiceImpl(const master::CarbonDriverPtr &carbonDriverPtr);
    ~CarbonMasterServiceImpl();
private:
    CarbonMasterServiceImpl(const CarbonMasterServiceImpl &);
    CarbonMasterServiceImpl& operator=(const CarbonMasterServiceImpl &);
    
public:
    void addGroup(::google::protobuf::RpcController* controller,
                  const ::carbon::proto::AddGroupRequest* request,
                  ::carbon::proto::AddGroupResponse* response,
                  ::google::protobuf::Closure* done);

    void updateGroup(::google::protobuf::RpcController* controller,
                     const ::carbon::proto::UpdateGroupRequest* request,
                     ::carbon::proto::UpdateGroupResponse* response,
                     ::google::protobuf::Closure* done);

    void deleteGroup(::google::protobuf::RpcController* controller,
                     const ::carbon::proto::DeleteGroupRequest* request,
                     ::carbon::proto::DeleteGroupResponse* response,
                     ::google::protobuf::Closure* done);

    void getGroupStatus(::google::protobuf::RpcController* controller,
                        const ::carbon::proto::GetGroupStatusRequest* request,
                        ::carbon::proto::GetGroupStatusResponse* response,
                        ::google::protobuf::Closure* done);

    void setLoggerLevel(::google::protobuf::RpcController* controller,
                        const ::carbon::proto::SetLoggerLevelRequest* request,
                        ::carbon::proto::SetLoggerLevelResponse* response,
                        ::google::protobuf::Closure* done);

    void getCarbonInfo(::google::protobuf::RpcController* controller,
                       const ::carbon::proto::GetCarbonInfoRequest* request,
                       ::carbon::proto::GetCarbonInfoResponse* response,
                       ::google::protobuf::Closure* done);

    void createPlanElement(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::CreatePlanElementRequest* request,
                           ::carbon::proto::CreatePlanElementResponse* response,
                           ::google::protobuf::Closure* done);

    void readPlanElement(::google::protobuf::RpcController* controller,
                         const ::carbon::proto::ReadPlanElementRequest* request,
                         ::carbon::proto::ReadPlanElementResponse* response,
                         ::google::protobuf::Closure* done);

    void updatePlanElement(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::UpdatePlanElementRequest* request,
                           ::carbon::proto::UpdatePlanElementResponse* response,
                           ::google::protobuf::Closure* done);

    void deletePlanElement(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::DeletePlanElementRequest* request,
                           ::carbon::proto::DeletePlanElementResponse* response,
                           ::google::protobuf::Closure* done);
    
    void readStatus(::google::protobuf::RpcController* controller,
                    const ::carbon::proto::ReadStatusRequest* request,
                    ::carbon::proto::ReadStatusResponse* response,
                    ::google::protobuf::Closure* done);
    
    void adjustRunningArgs(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::AdjustRunningArgsRequest* request,
                           ::carbon::proto::AdjustRunningArgsResponse* response,
                           ::google::protobuf::Closure* done);

    void setGlobalConfig(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::SetCarbonConfigRequest* request,
                           ::carbon::proto::SetCarbonConfigResponse* response,
                           ::google::protobuf::Closure* done);

    void getGlobalConfig(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::GetCarbonConfigRequest* request,
                           ::carbon::proto::GetCarbonConfigResponse* response,
                           ::google::protobuf::Closure* done);
    
    void getDriverStatus(::google::protobuf::RpcController* controller,
                           const ::carbon::proto::GetDriverStatusRequest* request,
                           ::carbon::proto::GetDriverStatusResponse* response,
                           ::google::protobuf::Closure* done);

private:
    master::CarbonDriverPtr _carbonDriverPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonMasterServiceImpl);

END_CARBON_NAMESPACE(service);

#endif //CARBON_CARBONMASTERSERVICEIMPL_H
