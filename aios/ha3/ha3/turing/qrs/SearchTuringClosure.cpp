#include "SearchTuringClosure.h"
#include <autil/TimeUtility.h>
#include <multi_call/common/ErrorCode.h>

using namespace autil;
using namespace multi_call;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SearchTuringClosure);

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
        HA3_LOG(WARN, "searchTuring failed: %s", _pGraphResponse->errorinfo().errormsg().c_str());
    }
    else if (_pGraphResponse->outputs_size() <= 0) {
        _pQrsResponse->set_multicall_ec(MULTI_CALL_ERROR_RPC_FAILED);
        HA3_LOG(WARN, "empty outputs from searchTuring");
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
            HA3_LOG(WARN, "no string data in output tensor");
        }
    }

    _pAccessLog->setProcessTime((TimeUtility::currentTime() - _startTime) / 1000);
    auto pDone = _pDone;
    delete this;
    pDone->Run();
}

END_HA3_NAMESPACE(turing);
