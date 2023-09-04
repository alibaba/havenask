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
#ifndef CARBON_OPSSERVICEIMPL_H
#define CARBON_OPSSERVICEIMPL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/CarbonDriver.h"
#include "carbon/proto/Ops.pb.h"

BEGIN_CARBON_NAMESPACE(service);

class CarbonOpsServiceImpl : public proto::CarbonOpsService
{
public:
    CarbonOpsServiceImpl(const master::CarbonDriverPtr &carbonDriverPtr);
    ~CarbonOpsServiceImpl();
private:
    CarbonOpsServiceImpl(const CarbonOpsServiceImpl &);
    CarbonOpsServiceImpl& operator=(const CarbonOpsServiceImpl &);
    
public:
    void releaseSlots(::google::protobuf::RpcController* controller,
                      const ::carbon::proto::ReleaseSlotsRequest* request,
                      ::carbon::proto::ReleaseSlotsResponse* response,
                      ::google::protobuf::Closure* done);

    void sweepTagSlots(::google::protobuf::RpcController* controller,
                      const ::carbon::proto::SweepSlotsRequest* request,
                      ::carbon::proto::SweepSlotsResponse* response,
                      ::google::protobuf::Closure* done);

    void syncGroupPlans(::google::protobuf::RpcController* controller,
                      const ::carbon::proto::SyncGroupRequest* request,
                      ::carbon::proto::SyncGroupResponse* response,
                      ::google::protobuf::Closure* done);
    
private:
    master::CarbonDriverPtr _carbonDriverPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonOpsServiceImpl);

END_CARBON_NAMESPACE(service);

#endif //CARBON_OPSERVICEIMPL_H
