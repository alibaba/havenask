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
#include "aios/network/arpc/arpc/ANetRPCController.h"

#include <cstddef>
#include <google/protobuf/stubs/callback.h>
#include <list>
#include <memory>
#include <stdint.h>
#include <string>

#include "aios/network/anet/ilogger.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

using namespace std;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(ANetRPCController);

ANetRPCController::ANetRPCController() {
    _traceInfo = NULL;
    Reset();
}

ANetRPCController::~ANetRPCController() {}

void ANetRPCController::Reset() {
    _failed = false;
    _reason = "";
    _canceled = false;
    _cancelList.clear();
    _errorCode = ARPC_ERROR_NONE;
    _expireTime = 0;
    _traceInfo = NULL;
}

void ANetRPCController::setProtoArena(const std::shared_ptr<google::protobuf::Arena> &arena) { _arena = arena; }

bool ANetRPCController::Failed() const { return _failed; }

string ANetRPCController::ErrorText() const { return _reason; }

void ANetRPCController::StartCancel() {
    _canceled = true;
    std::list<RPCClosure *>::const_iterator it;
    std::list<RPCClosure *>::const_iterator itEnd = _cancelList.end();

    for (it = _cancelList.begin(); it != itEnd; ++it) {
        (*it)->Run();
    }
}

void ANetRPCController::SetFailed(const string &reason) {
    _failed = true;
    _reason = reason;
}

bool ANetRPCController::IsCanceled() const { return _canceled; }

void ANetRPCController::NotifyOnCancel(RPCClosure *callback) { _cancelList.push_back(callback); }

void ANetRPCController::SetErrorCode(ErrorCode errorCode) { _errorCode = errorCode; }

ErrorCode ANetRPCController::GetErrorCode() { return _errorCode; }

int ANetRPCController::GetExpireTime() { return _expireTime; }

/**
 * Set expire time. 0: use default(5s). < 0 never timeout.
 */
void ANetRPCController::SetExpireTime(int milliseconds) { _expireTime = milliseconds; }

void ANetRPCController::SetClientAddress(const string &clientAddress) { _clientAddress = clientAddress; }

std::string &ANetRPCController::GetClientAddress() { return _clientAddress; }

void ANetRPCController::SetTraceFlag(bool flag) { _tracer.SetTraceFlag(flag); }

bool ANetRPCController::GetTraceFlag() const { return _tracer.GetTraceFlag(); }

Tracer &ANetRPCController::GetTracer() { return _tracer; }

const Tracer &ANetRPCController::GetTracer() const { return _tracer; }

const TraceInfo *ANetRPCController::GetTraceInfo() const { return _traceInfo; }

void ANetRPCController::SetTraceInfo(TraceInfo *traceInfo) { _traceInfo = traceInfo; }

void ANetRPCController::PrintTraceInfo() {
    if (!_tracer.GetTraceFlag() || !_traceInfo) {
        return;
    }

    static volatile int64_t counter = 0;
    static volatile int64_t serverQueueSize = 0;
    static volatile int64_t handleRequestTime = 0;
    static volatile int64_t workItemWaitProcessTime = 0;
    static volatile int64_t workItemProcessTime = 0;
    static volatile int64_t requestOnServerTime = 0;
    static volatile int64_t rtt = 0;

    int64_t cnt = __sync_add_and_fetch(&counter, 1);
    __sync_add_and_fetch(&serverQueueSize, _traceInfo->serverqueuesize());
    __sync_add_and_fetch(&handleRequestTime, _traceInfo->handlerequesttime());
    __sync_add_and_fetch(&workItemWaitProcessTime, _traceInfo->workitemwaitprocesstime());
    __sync_add_and_fetch(&workItemProcessTime, _traceInfo->workitemprocesstime());
    __sync_add_and_fetch(&requestOnServerTime, _traceInfo->requestonservertime());
    __sync_add_and_fetch(&rtt, _traceInfo->rtt());

    if (cnt % 100 == 0) {
        ARPC_LOG(INFO,
                 "serverQueueSize: %ld\nhandleRequestTime: %ld\nworkItemWaitProcessTime: %ld\nworkItemProcessTime: "
                 "%ld\nrequestOnServerTime: %ld\nrtt: %ld",
                 serverQueueSize / cnt,
                 handleRequestTime / cnt,
                 workItemWaitProcessTime / cnt,
                 workItemProcessTime / cnt,
                 requestOnServerTime / cnt,
                 rtt / cnt);
    }
}

ARPC_END_NAMESPACE(arpc);
