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
#include "aios/network/gig/multi_call/grpc/client/GrpcClientStreamHandler.h"
#include "aios/network/gig/multi_call/controller/ControllerFeedBack.h"
#include "aios/network/gig/multi_call/grpc/CompletionQueueStatus.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "aios/network/gig/multi_call/proto/GigStreamHeader.pb.h"
#include "aios/network/gig/multi_call/service/CallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"
#include "aios/network/gig/multi_call/stream/GigStreamClosure.h"
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"
#include "aios/network/gig/multi_call/stream/GigStreamResponse.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcClientStreamHandler);

GrpcClientStreamHandler::GrpcClientStreamHandler(const std::shared_ptr<GigStreamBase> &stream,
                                                 PartIdTy partId, const std::string &bizName,
                                                 const std::string &spec)
    : GigStreamHandlerBase(partId, stream)
    , _bizName(bizName)
    , _spec(spec)
    , _grpcReaderWriter(nullptr)
    , _request(nullptr)
    , _callInfo(nullptr)
    , _cleaned(true)
    , _isRetry(false)
{
    HANDLER_LOG(DEBUG,
                "client handler [%p] constructed, stream [%p] type: %s, biz: "
                "%s, spec: %s",
                this, _stream.get(), typeid(*this).name(), _bizName.c_str(),
                _spec.c_str());
    _streamRpc = true;
    if (stream) {
        _asyncIo = stream->isAsyncMode();
    }
}

GrpcClientStreamHandler::~GrpcClientStreamHandler() {
    if (!_cleaned.load()) {
        HANDLER_LOG(ERROR, "handler not cleaned: %s", typeid(*this).name());
        _streamState.logState(true);
    }
    if (_cancelBuffer.valid) {
        HANDLER_LOG(DEBUG, "cancel message not sent, this type: %s",
                    typeid(*this).name());
        _streamState.logState();
    }
    auto count = closureCount();
    if (count > 0) {
        HANDLER_LOG(ERROR, "closure count: %ld, not zero when finish, %s",
                    count, typeid(*this).name());
        _streamState.logState(true);
    }
    notifyFinish();
    HANDLER_LOG(DEBUG, "begin delete client grpc stream");
    delete _grpcReaderWriter;
    HANDLER_LOG(DEBUG, "end delete client handler [%p]", this);
#ifdef SET_DEBUG
    if (_objectPool) {
        autil::ScopedLock lock(_objectPool->lock);
        _objectPool->set.erase(this);
    }
#endif
}

grpc::ClientContext *GrpcClientStreamHandler::getClientContext() {
    return &_context;
}

void GrpcClientStreamHandler::setGrpcReaderWriter(
    const CompletionQueueStatusPtr &cqs,
    grpc::GenericClientAsyncReaderWriter *readerWriter) {
    _cqs = cqs;
    _grpcReaderWriter = readerWriter;
}

void GrpcClientStreamHandler::setRequestInfo(GigStreamRequest *request,
                                             const CallBackPtr &callBack)
{
    request->fillSpan();
    _request = request;
    _span = request->getSpan();
    _callBack = callBack;
    _isRetry = callBack->isRetry();
    _provider = _callBack->getResource()->getProvider(_isRetry);
}

void GrpcClientStreamHandler::setCallInfo(SingleCallInfo *callInfo) {
    _callInfo = callInfo;
}

GigStreamRpcInfo GrpcClientStreamHandler::snapshotStreamRpcInfo() const {
    GigStreamRpcInfo rpcInfo;
    _streamRpcInfoController.snapshot(rpcInfo);
    rpcInfo.spec = _spec;
    rpcInfo.isRetry = _isRetry;
    return rpcInfo;
}

bool GrpcClientStreamHandler::init() {
    HANDLER_LOG(DEBUG, "begin init, host: %s", _spec.c_str());
    _callBack->rpcBegin();
    _cleaned.store(false, std::memory_order_relaxed);
    autil::ScopedReadWriteLock lock(_cqs->enqueueLock, 'r');
    if (!_cqs->stopped) {
        _grpcReaderWriter->StartCall(getInitClosure());
        return true;
    } else {
        _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_STOPPED);
        _streamState.initFailed();
        runNext(false);
        return false;
    }
}

void GrpcClientStreamHandler::initCallback(bool ok) {
    if (ok) {
        setSendStatus(SS_IDLE);
        _streamState.endInit();
        HANDLER_LOG(DEBUG, "init success");
    } else {
        HANDLER_LOG(DEBUG, "init failed");
        _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_INIT);
        _streamState.initFailed();
    }
}

GigStreamClosureBase *GrpcClientStreamHandler::doSend(
        bool sendEof, grpc::ByteBuffer *message,
        GigStreamClosureBase *closure)
{
    if (_provider) {
        _provider->incStreamQueryCount();
    }
    autil::ScopedReadWriteLock lock(_cqs->enqueueLock, 'r');
    if (!_cqs->stopped) {
        _grpcReaderWriter->Write(*message, closure);
        return nullptr;
    } else {
        return closure;
    }
}

GigStreamClosureBase *GrpcClientStreamHandler::receiveNext() {
    if (_streamState.receiveEof()) {
        return nullptr;
    }
    _receiveBuffer.Clear();
    auto closure = getReceiveClosure();
    autil::ScopedReadWriteLock lock(_cqs->enqueueLock, 'r');
    if (!_cqs->stopped) {
        _grpcReaderWriter->Read(&_receiveBuffer, closure);
        return nullptr;
    } else {
        return closure;
    }
}

bool GrpcClientStreamHandler::doStreamReceive(
    const std::shared_ptr<GigStreamBase> &stream,
    const GigStreamMessage &message)
{
    if (!_request) {
        return false;
    }
    if (_provider) {
        _provider->incStreamResponseCount();
    }
    return _request->receive(_callInfo, stream, message);
}

void GrpcClientStreamHandler::doStreamReceiveCancel(
    const std::shared_ptr<GigStreamBase> &stream,
    const GigStreamMessage &message, MultiCallErrorCode ec)
{
    if (!_request) {
        return;
    }
    _request->receiveCancel(_callInfo, stream, message, ec);
}

bool GrpcClientStreamHandler::ignoreReceiveError() {
    return false;
}

GigStreamClosureBase *GrpcClientStreamHandler::finish() {
    auto cancelled = _streamState.isCancelled();
    auto closure = getFinishClosure();
    autil::ScopedReadWriteLock lock(_cqs->enqueueLock, 'r');
    if (cancelled) {
        HANDLER_LOG(DEBUG, "try client cancel");
        _context.TryCancel();
    }
    HANDLER_LOG(DEBUG, "client finishing");
    if (!_cqs->stopped) {
        _grpcReaderWriter->Finish(&_status, closure);
        HANDLER_LOG(DEBUG, "client finish called");
        return nullptr;
    } else {
        return closure;
    }
}

void GrpcClientStreamHandler::doFinishCallback(bool ok) { clean(); }

void GrpcClientStreamHandler::clean() {
    bool expect = false;
    if (!_cleaned.compare_exchange_weak(expect, true)) {
        return;
    }
    const auto &serverMetaMap = _context.GetServerTrailingMetadata();
    auto it = serverMetaMap.find(GIG_GRPC_RESPONSE_INFO_KEY);
    string responseInfo;
    if (serverMetaMap.end() != it) {
        const auto &stringRef = it->second;
        responseInfo = string(stringRef.data(), stringRef.length());
    }
    updateClockDrift();
    initChildSpan();
    _callBack->run(nullptr, _streamState.getErrorCode(), "", responseInfo);
    _callBack.reset();
    _provider.reset();
    HANDLER_LOG(DEBUG, "client finished");
}

void GrpcClientStreamHandler::initChildSpan() {
    if (!_span || !_callBack) {
        return;
    }
    const auto &resource = _callBack->getResource();
    if (!resource->isNormalRequest()) {
        return;
    }
    const auto &response = resource->getResponse(_isRetry);
    auto streamResponse = dynamic_cast<GigStreamResponse *>(response.get());
    if (streamResponse) {
        auto totalResponseSize = atomic_read(&_streamRpcInfoController._totalReceiveSize);
        streamResponse->setTotalSize(totalResponseSize);
    }

    auto totalRequestSize = atomic_read(&_streamRpcInfoController._totalSendSize);
    _span->setAttribute(opentelemetry::kSpanAttrReqContentLength,
                        autil::StringUtil::toString(totalRequestSize));
    _span->setAttribute(opentelemetry::kSpanAttrNetPeerIp, _spec);
    _callBack->setSpan(_span);
    _span.reset();
}

void GrpcClientStreamHandler::updateClockDrift() {
    if (!_provider || 0 == _netLatency) {
        return;
    }
    // minus means drift from this clock
    auto clockDrift = -(_timeDiff - (_netLatency / 2));
    _provider->updateClockDrift(clockDrift);
}

void GrpcClientStreamHandler::tryCancel() {
    _context.TryCancel();
}

void GrpcClientStreamHandler::abort() {
    stealStream();
    _callBack.reset();
    _provider.reset();
}

} // namespace multi_call
