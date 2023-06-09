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
#include "aios/network/gig/multi_call/grpc/server/GrpcCallData.h"
#include "aios/network/gig/multi_call/grpc/server/GrpcServerWorker.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcCallData);

GrpcCallData::GrpcCallData(GrpcServerWorker *worker,
                           ServerCompletionQueueStatus *cqs,
                           const SearchServicePtr &searchService)
    : arena(new google::protobuf::Arena()), status(CREATE), triggered(false),
      beginTime(0), worker(worker), cqs(cqs), stream(&serverContext),
      request(NULL), response(NULL), closure(new GigGrpcClosure(this)),
      searchService(searchService) {
    process();
}

GrpcCallData::GrpcCallData()
    : arena(new google::protobuf::Arena()), status(CREATE), triggered(false),
      beginTime(0), worker(nullptr), cqs(nullptr), stream(nullptr),
      request(NULL), response(NULL), closure(new GigGrpcClosure(this)) {}

GrpcCallData::~GrpcCallData() {
    freeProtoMessage(request);
    freeProtoMessage(response);
    DELETE_AND_SET_NULL(closure);
}

void GrpcCallData::process() {
    switch (status) {
    case CREATE:
        // AUTIL_LOG(INFO, "request create");
        status = READ;
        worker->getAsyncGenericService().RequestCall(
            &serverContext, &stream, cqs->cq.get(), cqs->cq.get(), this);
        break;
    case READ:
        // AUTIL_LOG(INFO, "received");
        status = PROCESS;
        beginQuery();
        stream.Read(&receiveBuffer, this);
        break;
    case PROCESS:
        // AUTIL_LOG(INFO, "request process");
        status = FINISH;
        trigger();
        call();
        break;
    case FINISH:
        // AUTIL_LOG(INFO, "request finish");
        delete this;
        break;
    }
}

void GrpcCallData::trigger() {
    if (!triggered) {
        autil::ScopedReadWriteLock lock(cqs->enqueueLock, 'r');
        if (!cqs->stopped) {
            triggered = true;
            new GrpcCallData(worker, cqs, searchService);
        }
    }
}

void GrpcCallData::beginQuery() {
    beginTime = autil::TimeUtility::currentTime();
    const auto &clientMetaMap = serverContext.client_metadata();
    auto it = clientMetaMap.find(GIG_GRPC_REQUEST_INFO_KEY);
    string requestInfo;
    if (clientMetaMap.end() != it) {
        const auto &stringRef = it->second;
        requestInfo = string(stringRef.data(), stringRef.length());
    }
    if (!requestInfo.empty()) {
        initQueryInfo(requestInfo);
    }
}

void GrpcCallData::initQueryInfo(const std::string &infoStr) {
    closure->setQueryInfo(worker->getAgent()->getQueryInfo(infoStr));
}

void GrpcCallData::initQueryInfoByCompatibleField(
    google::protobuf::Message *request, const GigRpcMethodArgPtr &arg) {
    if (arg != nullptr && arg->isHeartbeatMethod) {
        return;
    }
    if (closure->getQueryInfo()) {
        return;
    }
    string gigMeta;
    if (arg) {
        const std::string &field = arg->compatibleInfo.requestInfoField;
        ProtobufCompatibleUtil::getGigMetaField(request, field, gigMeta);
    }
    initQueryInfo(gigMeta);
}

void GrpcCallData::initQuerySession(
    google::protobuf::Message *request,
    const std::shared_ptr<GigRpcMethodArg> &arg) {
    if (arg != nullptr && arg->isHeartbeatMethod) {
        return;
    }
    const QueryInfoPtr &queryInfo = closure->getQueryInfo();
    QuerySessionPtr session(new QuerySession(searchService, queryInfo));
    if (session->isEmptyInitContext()) {
        std::string traceId, rpcId, userDatas;
        if (ProtobufCompatibleUtil::getEagleeyeField(
                request, arg->compatibleInfo.eagleeyeTraceId,
                arg->compatibleInfo.eagleeyeRpcId,
                arg->compatibleInfo.eagleeyeUserData, traceId, rpcId,
                userDatas)) {
            session->setEagleeyeUserData(traceId, rpcId, userDatas);
        }
    }
    closure->setQuerySession(session);
}

void GrpcCallData::call() {
    MultiCallErrorCode ec = MULTI_CALL_ERROR_NONE;
    auto methodArg = worker->getMethodArg(serverContext.method());
    if (!methodArg) {
        ec = MULTI_CALL_ERROR_NO_METHOD;
    } else {
        request = methodArg->request->New(arena.get());
        if (!ProtobufByteBufferUtil::deserializeFromBuffer(receiveBuffer,
                                                           request)) {
            ec = MULTI_CALL_REPLY_ERROR_REQUEST;
        }
        closure->setCompatibleFieldInfo(&methodArg->compatibleInfo);
    }
    initQueryInfoByCompatibleField(request, methodArg);
    auto &controller = closure->getController();
    controller.setErrorCode(ec);
    if (ec == MULTI_CALL_ERROR_NONE) {
        response = methodArg->response->New(arena.get());
        initQuerySession(request, methodArg);
        methodArg->method(&controller, request, response, closure);
    } else {
        closure->Run();
    }
}

} // namespace multi_call
