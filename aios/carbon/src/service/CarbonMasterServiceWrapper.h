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
#ifndef CARBON_CARBONMASTERSERVICEWRAPPER_H
#define CARBON_CARBONMASTERSERVICEWRAPPER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "service/CarbonMasterServiceImpl.h"

BEGIN_CARBON_NAMESPACE(service);

class CarbonMasterServiceWrapper : public CarbonMasterServiceImpl
{
public:
    CarbonMasterServiceWrapper(
            const master::CarbonDriverPtr &carbonDriverPtr);
    ~CarbonMasterServiceWrapper();
private:
    CarbonMasterServiceWrapper(const CarbonMasterServiceWrapper &);
    CarbonMasterServiceWrapper& operator=(const CarbonMasterServiceWrapper &);
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
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonMasterServiceWrapper);

END_CARBON_NAMESPACE(service);

#endif //CARBON_CARBONMASTERSERVICEWRAPPER_H
