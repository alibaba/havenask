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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMHANDLERBASE_H
#define ISEARCH_MULTI_CALL_GIGSTREAMHANDLERBASE_H

#include <google/protobuf/arena.h>
#include <grpc++/impl/codegen/byte_buffer.h>
#include <queue>

#include "aios/network/anet/atomic.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"
#include "aios/network/gig/multi_call/stream/StreamState.h"
#include "autil/LockFreeThreadPool.h"
#include "autil/ObjectTracer.h"
#include "autil/ThreadPool.h"

namespace multi_call {

class GigStreamClosureBase;
class GigStreamSendClosure;
class GigStreamReceiveClosure;
class GigStreamFinishClosure;
class GigStreamInitClosure;
class GigStreamCancelClosure;
class GigStreamBase;
class GigStreamHeader;
class GigStreamMessage;

struct SendBufferMessage {
public:
    SendBufferMessage()
        : valid(false)
        , cancel(false)
        , eof(false)
        , forceStop(false)
        , hasMsg(false)
    {
    }
public:
    bool valid : 1;
    bool cancel : 1;
    bool eof : 1;
    bool forceStop : 1;
    bool hasMsg : 1;
    grpc::Slice msg;
};

class GigStreamHandlerBase : public autil::ObjectTracer<GigStreamHandlerBase>,
                             public std::enable_shared_from_this<GigStreamHandlerBase>
{
public:
    GigStreamHandlerBase(PartIdTy partId,
                         const std::shared_ptr<GigStreamBase> &stream);
    virtual ~GigStreamHandlerBase();

private:
    GigStreamHandlerBase(const GigStreamHandlerBase &);
    GigStreamHandlerBase &operator=(const GigStreamHandlerBase &);

protected:
    enum SendStatus {
        SS_INIT = 0,
        SS_IDLE = 1,
        SS_SENDING = 2,
    };

public:
    void setCallBackThreadPool(autil::LockFreeThreadPool *threadPool);
    autil::LockFreeThreadPool *getCallBackThreadPool() const;

public:
    bool send(const SendBufferMessage &message);
public:
    std::shared_ptr<GigStreamBase> getStream();
    int64_t getHandlerId() const {
        return _thisId;
    }
public:
    static SendBufferMessage createSendBufferMessage(bool eof, bool cancelled,
                                                     const google::protobuf::Message *message);
protected:
    virtual bool init() = 0;
    virtual void initCallback(bool ok) = 0;
    void runNext(bool send);
    GigStreamClosureBase *sendNext();
    virtual GigStreamClosureBase *doSend(bool sendEof,
                                         grpc::ByteBuffer *message,
                                         GigStreamClosureBase *closure) = 0;
    virtual void tryCancel() {
    }
    void sendCallback(bool ok);
    void cancelCallback(bool ok);
    virtual GigStreamClosureBase *receiveNext() = 0;
    void receiveCallback(bool ok);
    virtual bool ignoreReceiveError() = 0;
    void setCancelMessage(const SendBufferMessage &sendMessage);
    GigStreamClosureBase *cancelNext();
    GigStreamClosureBase *finishNext();
    virtual GigStreamClosureBase *finish() = 0;
    void finishCallback(bool ok);
    virtual void doFinishCallback(bool ok);
    void notifyFinish();
    virtual void clean() = 0;
    // common
    GigStreamSendClosure *getSendClosure();
    GigStreamReceiveClosure *getReceiveClosure();
    GigStreamInitClosure *getInitClosure();
    GigStreamFinishClosure *getFinishClosure();
    GigStreamCancelClosure *getCancelClosure();
    void clearSendQueue();
    bool parseReceiveMessage(GigStreamHeader *&resultHeader,
                             google::protobuf::Message *&resultMessage);
    GigStreamHeader *createHeader(google::protobuf::Arena *arena);
    void setSendStatus(SendStatus status);
    int64_t closureCount() const;
    std::shared_ptr<GigStreamBase> stealStream();
public:
    void setLastClosure(GigStreamClosureBase *lastClosure);

private:
    bool sendNormal(const SendBufferMessage &message);
    bool sendCancel(const SendBufferMessage &message);
    virtual bool doStreamReceive(const std::shared_ptr<GigStreamBase> &stream,
                                 const GigStreamMessage &message);
    virtual void doStreamReceiveCancel(
            const std::shared_ptr<GigStreamBase> &stream,
            const GigStreamMessage &message,
            MultiCallErrorCode ec);
    bool parseRequest(const std::shared_ptr<GigStreamBase> &stream, GigStreamMessage &message,
                      bool &cancelled, bool &asyncIo);
    void finishClosure();
    void serializeSendMessage(const SendBufferMessage &message, grpc::ByteBuffer *buffer);
    void updateNetLatency(const GigStreamHeader &header);
private:
    friend class GigStreamClosureBase;
    friend class GigStreamSendClosure;
    friend class GigStreamReceiveClosure;
    friend class GigStreamInitClosure;
    friend class GigStreamFinishClosure;
    friend class GigStreamCancelClosure;

protected:
    PartIdTy _partId;
    mutable autil::ThreadMutex _streamLock;
    std::shared_ptr<GigStreamBase> _stream;
    bool _streamRpc : 1;
    bool _asyncIo : 1;
    StreamState _streamState;
    // send
    mutable autil::ThreadMutex _sendLock;
    SendStatus _sendStatus;
    std::queue<SendBufferMessage> _sendQueue;
    SendBufferMessage _cancelBuffer;
    bool _cancelTriggered;
    // receive
    grpc::ByteBuffer _receiveBuffer;
    std::atomic<bool> _terminated;

    GigStreamRpcInfoController _streamRpcInfoController;
    autil::LockFreeThreadPool *_callBackThreadPool;
    int64_t _thisId;
    int64_t _peerId;
    GigStreamClosureBase *_lastClosure;
    static atomic64_t _handlerId;
    static atomic64_t _handlerCount;
    int64_t _timeDiff;
    int64_t _netLatency;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigStreamHandlerBase);

#define HANDLER_LOG(level, msg, args...)                                       \
    AUTIL_LOG(level, "handler [%p] partId[%d], " msg, (void *)_thisId,         \
              _partId, ##args);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMHANDLERBASE_H
