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
#include "service/CarbonMasterServiceWrapper.h"
#include "carbon/Log.h"
#include "carbon/ErrorDefine.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
USE_CARBON_NAMESPACE(master);

BEGIN_CARBON_NAMESPACE(service);

CARBON_LOG_SETUP(service, CarbonMasterServiceWrapper);

CarbonMasterServiceWrapper::CarbonMasterServiceWrapper(
        const CarbonDriverPtr &carbonDriverPtr)
    : CarbonMasterServiceImpl(carbonDriverPtr)
{
    
}

CarbonMasterServiceWrapper::~CarbonMasterServiceWrapper() {
}

#define SHORT_CUT_CALL(method) {                                        \
        ErrorInfo errorInfo;                                            \
        errorInfo.errorCode = EC_NOT_SUPPORT_METHOD;                    \
        errorInfo.errorMsg = string("not support method:") + method;    \
        *(response->mutable_errorinfo()) = autil::legacy::ToJsonString(errorInfo, true); \
        if (done) {                                                     \
            done->Run();                                                \
        }                                                               \
    }

void CarbonMasterServiceWrapper::addGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::AddGroupRequest* request,
        ::carbon::proto::AddGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    SHORT_CUT_CALL("addGroup");
}

void CarbonMasterServiceWrapper::updateGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::UpdateGroupRequest* request,
        ::carbon::proto::UpdateGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    SHORT_CUT_CALL("updateGroup");
}

void CarbonMasterServiceWrapper::deleteGroup(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::DeleteGroupRequest* request,
        ::carbon::proto::DeleteGroupResponse* response,
        ::google::protobuf::Closure* done)
{
    SHORT_CUT_CALL("deleteGroup");
}

END_CARBON_NAMESPACE(service);

