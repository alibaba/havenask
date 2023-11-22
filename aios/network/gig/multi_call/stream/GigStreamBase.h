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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMBASE_H
#define ISEARCH_MULTI_CALL_GIGSTREAMBASE_H

#include <list>

#include "aios/network/gig/multi_call/stream/GigStreamMessage.h"

namespace multi_call {

class QueryInfo;

class PartMessageQueue
{
public:
    PartMessageQueue();
    ~PartMessageQueue();

private:
    PartMessageQueue(const PartMessageQueue &);
    PartMessageQueue &operator=(const PartMessageQueue &);

public:
    bool nextMessage(GigStreamMessage &message, MultiCallErrorCode &ec);
    bool peekMessage(GigStreamMessage &message, MultiCallErrorCode &ec);
    bool empty() const {
        return (0 == _count);
    }
    void appendMessage(const GigStreamMessage &message, MultiCallErrorCode ec);
    bool beginReceive();
    bool endReceive();

private:
    autil::ThreadMutex _lock;
    std::atomic<bool> _receiving;
    size_t _count;
    std::list<GigStreamMessage> _messageList;
    MultiCallErrorCode _ec;
};

class GigStreamBase : public std::enable_shared_from_this<GigStreamBase>
{
public:
    GigStreamBase();
    virtual ~GigStreamBase();

private:
    GigStreamBase(const GigStreamBase &);
    GigStreamBase &operator=(const GigStreamBase &);

public:
    virtual google::protobuf::Message *newReceiveMessage(google::protobuf::Arena *arena) const = 0;

public:
    virtual bool send(PartIdTy partId, bool eof, google::protobuf::Message *message) = 0;
    virtual void sendCancel(PartIdTy partId, google::protobuf::Message *message) = 0;

private:
    virtual bool receive(const GigStreamMessage &message) = 0;
    virtual void receiveCancel(const GigStreamMessage &message, MultiCallErrorCode ec) = 0;

public:
    virtual void notifyIdle(PartIdTy partId) = 0;

public:
    virtual bool hasError() const;

public:
    virtual std::shared_ptr<QueryInfo> getQueryInfo() const {
        return nullptr;
    }
    virtual std::string getPeer() const {
        return std::string();
    }

public:
    // async
    void setAsyncMode(bool async) {
        _asyncMode = async;
    }
    bool isAsyncMode() const {
        return _asyncMode;
    }
    // override to implement async receive
    virtual bool notifyReceive(multi_call::PartIdTy partId) {
        return asyncReceive(partId);
    }
    bool asyncReceive(PartIdTy partId);
    bool peekMessage(PartIdTy partId, GigStreamMessage &message,
                     MultiCallErrorCode &ec);
private:
    bool innerReceive(const GigStreamMessage &message, MultiCallErrorCode ec);
    friend class GigStreamHandlerBase;
    friend class GigStreamRequest;

private:
    bool appendMessage(const GigStreamMessage &message, MultiCallErrorCode ec);
    PartMessageQueue &getPartQueue(PartIdTy partId);
    bool tryNotify(PartIdTy partId);
    bool doReceive(PartMessageQueue &partQueue);

private:
    bool _asyncMode = true;
    mutable autil::ReadWriteLock _asyncQueueLock;
    std::unordered_map<PartIdTy, PartMessageQueue> _partAsyncQueues;
};

MULTI_CALL_TYPEDEF_PTR(GigStreamBase);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMBASE_H
