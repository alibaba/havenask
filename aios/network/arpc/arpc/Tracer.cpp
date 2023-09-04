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
#include "aios/network/arpc/arpc/Tracer.h"

#include <assert.h>
#include <google/protobuf/arena.h>

#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(Tracer);

Tracer::Tracer(bool enableTrace) {
    _enableTrace = enableTrace;
    _clientTimeout = 0;
    _serverQueueSize = 0;
    _beginHandleRequest = 0;
    _endHandleRequest = 0;
    _beginPostResponse = 0;
    _beginWorkItemProcess = 0;
}

Tracer::~Tracer() {}

TraceInfo *Tracer::ExtractClientTraceInfo(google::protobuf::Arena *arena) const {
    assert(arena);
    TraceInfo *traceInfo = google::protobuf::Arena::CreateMessage<TraceInfo>(arena);
    traceInfo->set_rtt(calcTime(_beginPostRequest, _beginHandleResponse));
    return traceInfo;
}

TraceInfo *Tracer::ExtractServerTraceInfo(google::protobuf::Arena *arena) const {
    assert(arena);
    TraceInfo *traceInfo = google::protobuf::Arena::CreateMessage<TraceInfo>(arena);
    traceInfo->set_serverqueuesize(_serverQueueSize);
    traceInfo->set_handlerequesttime(calcTime(_beginHandleRequest, _endHandleRequest));
    traceInfo->set_workitemwaitprocesstime(calcTime(_endHandleRequest, _beginWorkItemProcess));
    traceInfo->set_workitemprocesstime(calcTime(_beginWorkItemProcess, _beginPostResponse));
    traceInfo->set_requestonservertime(calcTime(_beginHandleRequest, _beginPostResponse));

    traceInfo->set_userpayload(_userPayload);

    return traceInfo;
}

ARPC_END_NAMESPACE(arpc);
