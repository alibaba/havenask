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

#include "aios/network/gig/multi_call/stream/GigStreamHandlerBase.h"

#include "aios/network/gig/multi_call/proto/GigStreamHeader.pb.h"
#include "aios/network/gig/multi_call/stream/GigStreamBase.h"
#include "aios/network/gig/multi_call/stream/GigStreamClosure.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigStreamHandlerBase);

atomic64_t GigStreamHandlerBase::_handlerId;
atomic64_t GigStreamHandlerBase::_handlerCount;

// #define SET_DEBUG
#ifdef SET_DEBUG
static autil::ThreadMutex HANDLER_SET_LOCK;
static std::set<GigStreamHandlerBase *> HANDLER_SET;
#endif

GigStreamHandlerBase::GigStreamHandlerBase(PartIdTy partId, const GigStreamBasePtr &stream)
    : _partId(partId)
    , _stream(stream)
    , _streamRpc(false)
    , _asyncIo(true)
    , _sendStatus(SS_INIT)
    , _cancelTriggered(false)
    , _terminated(false)
    , _streamRpcInfoController(partId)
    , _callBackThreadPool(nullptr)
    , _peerId(0)
    , _lastClosure(nullptr)
    , _timeDiff(0)
    , _netLatency(0) {
    _thisId = atomic_inc_return(&_handlerId);
    _streamState.setHandlerId(_thisId);
    atomic_inc(&_handlerCount);
#ifdef SET_DEBUG
    {
        autil::ScopedLock lock(HANDLER_SET_LOCK);
        HANDLER_SET.insert(this);
    }
#endif
}

GigStreamHandlerBase::~GigStreamHandlerBase() {
    clearSendQueue();
    atomic_dec(&_handlerCount);
#ifdef SET_DEBUG
    {
        autil::ScopedLock lock(HANDLER_SET_LOCK);
        HANDLER_SET.erase(this);
    }
#endif
}

void GigStreamHandlerBase::setCallBackThreadPool(autil::LockFreeThreadPool *threadPool) {
    _callBackThreadPool = threadPool;
}

autil::LockFreeThreadPool *GigStreamHandlerBase::getCallBackThreadPool() const {
    return _callBackThreadPool;
}

bool GigStreamHandlerBase::send(const SendBufferMessage &message) {
    if (!message.cancel) {
        return sendNormal(message);
    } else {
        return sendCancel(message);
    }
}

bool GigStreamHandlerBase::sendNormal(const SendBufferMessage &message) {
    if (!_streamState.running()) {
        HANDLER_LOG(DEBUG, "send message failed, stream stopped, this type: %s",
                    typeid(*this).name());
        _streamState.logState();
        return false;
    }
    if (_streamState.sendEof()) {
        HANDLER_LOG(ERROR, "send msg overrun, this type: %s", typeid(*this).name());
        _streamState.logState();
        return false;
    }
    {
        autil::ScopedLock lock(_sendLock);
        _sendQueue.push(message);
        if (message.eof) {
            _streamState.setSendEof();
        }
    }
    atomic_inc(&_streamRpcInfoController._totalSendCount);
    runNext(true);
    return true;
}

void GigStreamHandlerBase::runNext(bool send) {
    _streamState.logState();
    auto handlerOp = _streamState.next();
    HANDLER_LOG(DEBUG, "this type: %s, next op: %s, send: %d", typeid(*this).name(),
                StreamState::getOpString(handlerOp), send);
    GigStreamClosureBase *closure = nullptr;
    switch (handlerOp) {
    case HO_RUN:
        if (send) {
            closure = sendNext();
        } else {
            closure = receiveNext();
        }
        break;
    case HO_SEND:
        closure = sendNext();
        break;
    case HO_CANCEL:
        closure = cancelNext();
        break;
    case HO_SEND_CANCEL:
        closure = sendNext();
        break;
    case HO_FINISH:
        closure = finishNext();
        break;
    case HO_IDLE:
        break;
    case HO_FINISHED:
        notifyFinish();
    default:
        break;
    }
    if (closure) {
        closure->run(false);
    }
}

GigStreamClosureBase *GigStreamHandlerBase::sendNext() {
    GigStreamClosureBase *closure = nullptr;
    bool ok = false;
    {
        autil::ScopedLock lock(_sendLock);
        if (SS_IDLE != _sendStatus) {
            HANDLER_LOG(DEBUG, "send busy: %d, type: %s", _sendStatus, typeid(*this).name());
            _streamState.logState();
            return nullptr;
        }
        bool eof = _streamState.sendEof();
        if (_cancelBuffer.valid) {
            if (!_streamState.startCancel()) {
                return nullptr;
            }
            HANDLER_LOG(DEBUG, "sending cancel msg, type: %s", typeid(*this).name());
            grpc::ByteBuffer byteBuffer;
            serializeSendMessage(_cancelBuffer, &byteBuffer);
            _streamRpcInfoController._sendRecord.cancel();
            atomic_add(byteBuffer.Length(), &_streamRpcInfoController._totalSendSize);
            closure = doSend(true, &byteBuffer, getCancelClosure(), ok);
            _cancelBuffer.valid = false;
            _sendStatus = SS_SENDING;
        } else if (!_sendQueue.empty()) {
            auto msg = _sendQueue.front();
            _sendQueue.pop();
            grpc::ByteBuffer byteBuffer;
            serializeSendMessage(msg, &byteBuffer);
            _streamRpcInfoController._sendRecord.begin();
            atomic_add(byteBuffer.Length(), &_streamRpcInfoController._totalSendSize);
            closure = doSend(eof && _sendQueue.empty(), &byteBuffer, getSendClosure(), ok);
            _sendStatus = SS_SENDING;
        } else {
            // no msg
        }
        if (eof) {
            _streamRpcInfoController._sendRecord.eof();
        }
    }
    if (closure && ok) {
        closure->run(ok);
        return nullptr;
    }
    return closure;
}

void GigStreamHandlerBase::sendCallback(bool ok) {
    if (!ok) {
        if (!_streamState.hasError()) {
            HANDLER_LOG(DEBUG, "post stream message failed, peer: %p, this type: %s",
                        (void *)_peerId, typeid(*this).name());
            _streamState.logState();
            _streamState.setErrorCode(MULTI_CALL_REPLY_ERROR_POST);
        }
    }
    {
        autil::ScopedLock lock(_sendLock);
        if (_streamState.sendEof() && _sendQueue.empty()) {
            _streamState.setSendFinished();
        }
    }
    setSendStatus(SS_IDLE);
}

bool GigStreamHandlerBase::sendCancel(const SendBufferMessage &message) {
    stealStream();
    if (!_streamState.running() || _streamState.bidiEof()) {
        _streamState.logState();
        HANDLER_LOG(DEBUG, "stream stopped, cancel msg ignored, stream [%p] this type: %s",
                    getStream().get(), typeid(*this).name());
        return true;
    }
    HANDLER_LOG(DEBUG, "begin send cancel");
    setCancelMessage(message);
    _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_CANCELLED);
    if (message.forceStop) {
        tryCancel();
    }
    HANDLER_LOG(DEBUG, "end send cancel");
    runNext(true);
    return true;
}

GigStreamClosureBase *GigStreamHandlerBase::cancelNext() {
    if (!_cancelTriggered) {
        auto sendMessage = createSendBufferMessage(true, true, nullptr);
        setCancelMessage(sendMessage);
    }
    clearSendQueue();
    return sendNext();
}

void GigStreamHandlerBase::setCancelMessage(const SendBufferMessage &sendMessage) {
    if (_cancelTriggered) {
        return;
    }
    autil::ScopedLock lock(_sendLock);
    if (_cancelTriggered) {
        return;
    }
    _cancelBuffer = sendMessage;
    _cancelTriggered = true;
    HANDLER_LOG(DEBUG, "set cancel msg");
    return;
}

void GigStreamHandlerBase::cancelCallback(bool ok) {
    setSendStatus(SS_IDLE);
    _streamState.endCancel();
}

GigStreamClosureBase *GigStreamHandlerBase::finishNext() {
    clearSendQueue();
    return finish();
}

void GigStreamHandlerBase::serializeSendMessage(const SendBufferMessage &message,
                                                grpc::ByteBuffer *buffer) {
    GigStreamHeader header;
    header.set_eof(message.eof);
    header.set_cancelled(message.cancel);
    header.set_async_io(_asyncIo);
    if (message.cancel) {
        header.set_msg("gig cancel message");
    }
    header.set_has_message(message.hasMsg);
    header.set_begin_time(autil::TimeUtility::currentTime());
    header.set_peer_time_diff(_timeDiff);
    HANDLER_LOG(DEBUG, "receive, begin_time: %ld", header.begin_time());
    const grpc::Slice *msg = nullptr;
    if (message.hasMsg) {
        msg = &message.msg;
    }
    ProtobufByteBufferUtil::serializeToBuffer(&header, msg, buffer);
}

SendBufferMessage
GigStreamHandlerBase::createSendBufferMessage(bool eof, bool cancelled,
                                              const google::protobuf::Message *message) {
    SendBufferMessage sendBuffer;
    sendBuffer.valid = true;
    sendBuffer.cancel = cancelled;
    sendBuffer.eof = eof;
    if (message) {
        sendBuffer.hasMsg = true;
        size_t messageLength = message->ByteSizeLong();
        {
            grpc::Slice msgSlice(messageLength);
            message->SerializeWithCachedSizesToArray(const_cast<uint8_t *>(msgSlice.begin()));
            sendBuffer.msg = msgSlice;
        }
    }
    return sendBuffer;
}

void GigStreamHandlerBase::receiveCallback(bool ok) {
    HANDLER_LOG(DEBUG, "receive ok: %d, this type: %s, receiveEof: %d", (int)ok,
                typeid(*this).name(), _streamState.receiveEof());
    if (!ok) {
        if (!ignoreReceiveError() && !_streamState.hasError()) {
            HANDLER_LOG(DEBUG, "receive failed, ok[%d], this type: %s", ok, typeid(*this).name());
            _streamState.logState();
            _streamState.setErrorCode(MULTI_CALL_ERROR_RPC_FAILED);
        }
        _streamState.disableReceive();
        return;
    }
    auto stream = getStream();
    if (!stream) {
        HANDLER_LOG(DEBUG, "stream is null, type: %s", typeid(*this).name());
        _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_RECEIVE);
        return;
    }
    atomic_add(_receiveBuffer.Length(), &_streamRpcInfoController._totalReceiveSize);
    GigStreamMessage message;
    message.partId = _partId;
    message.handlerId = _thisId;
    bool cancelled = true;
    bool asyncIo = true;
    if (!parseRequest(stream, message, cancelled, asyncIo)) {
        HANDLER_LOG(DEBUG, "parse query failed, this type: %s", typeid(*this).name());
        _streamState.setErrorCode(MULTI_CALL_REPLY_ERROR_REQUEST);
        return;
    }
    // after parseRequest
    message.netLatencyUs = _netLatency;
    _asyncIo = asyncIo;
    stream->setAsyncMode(asyncIo);
    if (_streamState.receiveEof()) {
        if (!cancelled) {
            HANDLER_LOG(ERROR, "receive msg overrun, this type: %s", typeid(*this).name());
            _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_OVERRUN);
            return;
        }
    }
    if (message.message) {
        HANDLER_LOG(TRACE1, "receive msg:[%s], this type: %s",
                    message.message->DebugString().c_str(), typeid(*this).name());
    }
    _streamRpcInfoController._receiveRecord.begin();
    if (cancelled) {
        _streamRpcInfoController._receiveRecord.cancel();
        HANDLER_LOG(DEBUG, "stream cancelled on peer side, this type: %s", typeid(*this).name());
        stream = stealStream();
        if (stream) {
            doStreamReceiveCancel(stream, message, MULTI_CALL_ERROR_STREAM_CANCELLED);
        }
        _streamState.setReceiveCancel();
        _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_CANCELLED);
        return;
    }
    if (message.eof) {
        _streamRpcInfoController._receiveRecord.eof();
        endReceive();
    }
    atomic_inc(&_streamRpcInfoController._totalReceiveCount);
    if (!doStreamReceive(stream, message)) {
        HANDLER_LOG(DEBUG, "receive failed, type: %s", typeid(*this).name());
        _streamState.setErrorCode(MULTI_CALL_ERROR_STREAM_RECEIVE);
        return;
    }
}

bool GigStreamHandlerBase::parseRequest(const std::shared_ptr<GigStreamBase> &stream,
                                        GigStreamMessage &message, bool &cancelled, bool &asyncIo) {
    message.initArena();
    auto arena = message.arena.get();
    auto request = stream->newReceiveMessage(arena);
    if (_streamRpc) {
        auto header = createHeader(arena);
        if (!parseReceiveMessage(header, request)) {
            return false;
        }
        cancelled = header->cancelled();
        message.eof = header->eof() || cancelled;
        asyncIo = header->async_io();
    } else {
        if (!ProtobufByteBufferUtil::deserializeFromBuffer(_receiveBuffer, request)) {
            return false;
        }
        message.eof = true;
        cancelled = false;
    }
    message.message = request;
    return true;
}

bool GigStreamHandlerBase::parseReceiveMessage(GigStreamHeader *&resultHeader,
                                               google::protobuf::Message *&resultMessage) {
    assert(resultHeader);
    assert(resultMessage);
    GrpcByteBufferSource stream;
    if (!stream.Init(_receiveBuffer)) {
        HANDLER_LOG(ERROR, "init receive buffer failed");
        return false;
    }
    size_t headerLength = 0;
    if (!ProtobufByteBufferUtil::getHeaderLength(stream, headerLength)) {
        HANDLER_LOG(ERROR, "getHeaderLength failed");
        return false;
    }
    assert(ProtobufByteBufferUtil::LENGTH_SIZE == stream.ByteCount());
    stream.setLimit(ProtobufByteBufferUtil::LENGTH_SIZE + headerLength);
    if (!resultHeader->ParseFromZeroCopyStream(&stream)) {
        HANDLER_LOG(ERROR, "parse query header failed");
        return false;
    }
    // ugly but necessary
    updateNetLatency(*resultHeader);
    if (resultHeader->has_message()) {
        stream.setLimit(stream.size());
        if (!resultMessage->ParseFromZeroCopyStream(&stream)) {
            HANDLER_LOG(ERROR, "parse query body failed");
            return false;
        }
        return true;
    } else {
        freeProtoMessage(resultMessage);
        resultMessage = nullptr;
        return true;
    }
}

void GigStreamHandlerBase::updateNetLatency(const GigStreamHeader &header) {
    auto beginTime = header.begin_time();
    if (0 != beginTime) {
        auto timeDiff = autil::TimeUtility::currentTime() - beginTime;
        auto peerTimeDiff = header.peer_time_diff();
        if (0 != peerTimeDiff) {
            _netLatency = abs(timeDiff + peerTimeDiff);
        }
        _timeDiff = timeDiff;
    }
}

void GigStreamHandlerBase::clearSendQueue() {
    autil::ScopedLock lock(_sendLock);
    auto queueSize = _sendQueue.size();
    if (queueSize > 0) {
        HANDLER_LOG(DEBUG, "message dropped, count: %ld ", queueSize);
    }
    while (!_sendQueue.empty()) {
        _sendQueue.pop();
    }
}

void GigStreamHandlerBase::endReceive() {
    _streamState.setReceiveEof();
    _streamState.setReceiveFinished();
}

void GigStreamHandlerBase::setSendStatus(SendStatus status) {
    autil::ScopedLock lock(_sendLock);
    _sendStatus = status;
}

GigStreamSendClosure *GigStreamHandlerBase::getSendClosure() {
    atomic_inc(&_streamRpcInfoController._closureCount);
    atomic_inc(&_streamRpcInfoController._totalSendClosureCount);
    return new GigStreamSendClosure(shared_from_this());
}

GigStreamReceiveClosure *GigStreamHandlerBase::getReceiveClosure() {
    atomic_inc(&_streamRpcInfoController._closureCount);
    atomic_inc(&_streamRpcInfoController._totalReceiveClosureCount);
    return new GigStreamReceiveClosure(shared_from_this());
}

GigStreamInitClosure *GigStreamHandlerBase::getInitClosure() {
    atomic_inc(&_streamRpcInfoController._closureCount);
    atomic_inc(&_streamRpcInfoController._totalInitClosureCount);
    return new GigStreamInitClosure(shared_from_this());
}

GigStreamFinishClosure *GigStreamHandlerBase::getFinishClosure() {
    atomic_inc(&_streamRpcInfoController._closureCount);
    atomic_inc(&_streamRpcInfoController._totalFinishClosureCount);
    return new GigStreamFinishClosure(shared_from_this());
}

GigStreamCancelClosure *GigStreamHandlerBase::getCancelClosure() {
    atomic_inc(&_streamRpcInfoController._closureCount);
    atomic_inc(&_streamRpcInfoController._totalCancelClosureCount);
    return new GigStreamCancelClosure(shared_from_this());
}

void GigStreamHandlerBase::finishClosure() {
    atomic_dec(&_streamRpcInfoController._closureCount);
}

int64_t GigStreamHandlerBase::closureCount() const {
    return atomic_read(&_streamRpcInfoController._closureCount);
}

void GigStreamHandlerBase::setLastClosure(GigStreamClosureBase *lastClosure) {
    _lastClosure = lastClosure;
}

GigStreamHeader *GigStreamHandlerBase::createHeader(google::protobuf::Arena *arena) {
    assert(arena);
    return google::protobuf::Arena::CreateMessage<GigStreamHeader>(arena);
}

void GigStreamHandlerBase::notifyFinish() {
    bool expect = false;
    if (!_terminated.compare_exchange_weak(expect, true)) {
        return;
    }
    HANDLER_LOG(DEBUG, "handler finished, type: %s, closureCount: %ld", typeid(*this).name(),
                closureCount());
    _streamState.logState();
    clean();
    auto stream = stealStream();
    if (stream && _streamState.hasError()) {
        HANDLER_LOG(DEBUG, "send cancel to stream[%p], type: %s", stream.get(),
                    typeid(*this).name());
        GigStreamMessage message;
        message.partId = _partId;
        message.eof = true;
        message.handlerId = _thisId;
        message.netLatencyUs = _netLatency;
        doStreamReceiveCancel(stream, message, _streamState.getErrorCode());
    }
}

bool GigStreamHandlerBase::doStreamReceive(const std::shared_ptr<GigStreamBase> &stream,
                                           const GigStreamMessage &message) {
    return stream->innerReceive(message, MULTI_CALL_ERROR_NONE);
}

void GigStreamHandlerBase::doStreamReceiveCancel(const std::shared_ptr<GigStreamBase> &stream,
                                                 const GigStreamMessage &message,
                                                 MultiCallErrorCode ec) {
    stream->innerReceive(message, ec);
}

std::shared_ptr<GigStreamBase> GigStreamHandlerBase::getStream() {
    autil::ScopedLock lock(_streamLock);
    return _stream;
}

std::shared_ptr<GigStreamBase> GigStreamHandlerBase::stealStream() {
    autil::ScopedLock lock(_streamLock);
    return std::move(_stream);
}

void GigStreamHandlerBase::finishCallback(bool ok) {
    HANDLER_LOG(DEBUG, "stream finished, this type: %s", typeid(*this).name());
    doFinishCallback(ok);
    _streamState.endFinish();
}

void GigStreamHandlerBase::doFinishCallback(bool ok) {
}

} // namespace multi_call
