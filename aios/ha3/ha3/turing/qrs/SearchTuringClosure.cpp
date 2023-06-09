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
#include "ha3/turing/qrs/SearchTuringClosure.h"

#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/common/AccessLog.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/proto/QrsService.pb.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "suez/turing/proto/ErrorCode.pb.h"
#include "suez/turing/proto/Search.pb.h"
#include "tensorflow/core/framework/tensor.pb.h"

using namespace autil;
using namespace multi_call;
using namespace suez::turing;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SearchTuringClosure);

SearchTuringClosure::SearchTuringClosure(
        common::AccessLog* pAccessLog,
        suez::turing::GraphRequest* pGraphRequest,
        suez::turing::GraphResponse* pGraphResponse,
        proto::QrsResponse* pQrsResponse,
        multi_call::GigClosure* pDone,
        int64_t startTime)
    : _pAccessLog(pAccessLog),
      _pGraphRequest(pGraphRequest),
      _pGraphResponse(pGraphResponse),
      _pQrsResponse(pQrsResponse),
      _pDone(pDone),
      _startTime(startTime)
{
}

void SearchTuringClosure::Run() {
    _pQrsResponse->set_multicall_ec(_pGraphResponse->multicall_ec());
    _pQrsResponse->set_gigresponseinfo(_pGraphResponse->gigresponseinfo());
    _pQrsResponse->set_formattype(proto::FT_PROTOBUF);
    auto pResult = _pQrsResponse->mutable_assemblyresult();

    if (_pGraphResponse->has_errorinfo() && _pGraphResponse->errorinfo().errorcode() != RS_ERROR_NONE) {
        AUTIL_LOG(WARN, "searchTuring failed: %s", _pGraphResponse->errorinfo().errormsg().c_str());
    }
    else if (_pGraphResponse->outputs_size() <= 0) {
        _pQrsResponse->set_multicall_ec(MULTI_CALL_ERROR_RPC_FAILED);
        AUTIL_LOG(WARN, "empty outputs from searchTuring");
    }
    else {
        const auto& namedTensorProto = _pGraphResponse->outputs(0);
        const auto& tensorProto = namedTensorProto.tensor();
        if (tensorProto.string_val_size() > 0) {
            _pAccessLog->setStatusCode(ERROR_NONE);
            *pResult = tensorProto.string_val(0);
        }
        else {
            _pQrsResponse->set_multicall_ec(MULTI_CALL_ERROR_RPC_FAILED);
            AUTIL_LOG(WARN, "no string data in output tensor");
        }
    }

    _pAccessLog->setProcessTime((TimeUtility::currentTime() - _startTime) / 1000);
    auto pDone = _pDone;
    delete this;
    pDone->Run();
}

} // namespace turing
} // namespace isearch
