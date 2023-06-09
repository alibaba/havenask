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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMCLOSURE_H
#define ISEARCH_MULTI_CALL_GIGSTREAMCLOSURE_H

#include "autil/WorkItem.h"
#include "autil/LockFreeThreadPool.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/stream/GigStreamHandlerBase.h"

namespace multi_call {

class GigStreamClosureBase;

class GrpcStreamBackWorkItem : public autil::WorkItem {
public:
    GrpcStreamBackWorkItem(GigStreamClosureBase *closure) : _closure(closure) {}
    ~GrpcStreamBackWorkItem() {}

public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    GigStreamClosureBase *_closure;
};

class GigStreamClosureBase {
public:
    GigStreamClosureBase(const GigStreamHandlerBasePtr &handler)
        : _item(this), _ok(false), _handler(handler) {
        _handler->setLastClosure(this);
    }
    virtual ~GigStreamClosureBase() {}

private:
    GigStreamClosureBase(const GigStreamClosureBase &);
    GigStreamClosureBase &operator=(const GigStreamClosureBase &);

public:
    void run(bool ok) {
        _ok = ok;
        auto threadPool = _handler->getCallBackThreadPool();
        if (threadPool) {
            if (autil::ThreadPool::ERROR_NONE ==
                threadPool->pushWorkItem(&_item, false)) {
                return;
            }
        }
        finishClosure();
        delete this;
    }
    void finishClosure() {
        _handler->finishClosure();
        run();
    }
    void drop() {
        _handler->finishClosure();
        delete this;
    }

public:
    virtual void run() = 0;

private:
    GrpcStreamBackWorkItem _item;

protected:
    bool _ok;
    GigStreamHandlerBasePtr _handler;
};

class GigStreamSendClosure : public GigStreamClosureBase {
public:
    GigStreamSendClosure(const GigStreamHandlerBasePtr &handler)
        : GigStreamClosureBase(handler) {
        AUTIL_LOG(DEBUG, "handler: %p, begin send: %p", _handler.get(), this);
    }

public:
    void run() {
        AUTIL_LOG(DEBUG, "handler: %p, send finished: %p, ok: %d",
                  _handler.get(), this, _ok);
        _handler->sendCallback(_ok);
        _handler->runNext(true);
    }

private:
    AUTIL_LOG_DECLARE();
};

class GigStreamReceiveClosure : public GigStreamClosureBase {
public:
    GigStreamReceiveClosure(const GigStreamHandlerBasePtr &handler)
        : GigStreamClosureBase(handler) {
        AUTIL_LOG(DEBUG, "handler: %p, begin receive: %p", _handler.get(),
                  this);
    }

public:
    void run() {
        AUTIL_LOG(DEBUG, "handler: %p, receive finished: %p, ok: %d",
                  _handler.get(), this, _ok);
        _handler->receiveCallback(_ok);
        _handler->runNext(false);
    }

private:
    AUTIL_LOG_DECLARE();
};

class GigStreamInitClosure : public GigStreamClosureBase {
public:
    GigStreamInitClosure(const GigStreamHandlerBasePtr &handler)
        : GigStreamClosureBase(handler) {
        AUTIL_LOG(DEBUG, "handler: %p, begin init: %p", _handler.get(), this);
    }

public:
    void run() {
        AUTIL_LOG(DEBUG, "handler: %p, init finished: %p, ok: %d",
                  _handler.get(), this, _ok);
        _handler->initCallback(_ok);
        _handler->runNext(true);
        _handler->runNext(false);
    }

private:
    AUTIL_LOG_DECLARE();
};

class GigStreamFinishClosure : public GigStreamClosureBase {
public:
    GigStreamFinishClosure(const GigStreamHandlerBasePtr &handler)
        : GigStreamClosureBase(handler) {
        AUTIL_LOG(DEBUG, "handler: %p, begin finish wait: %p", _handler.get(),
                  this);
    }

public:
    void run() {
        AUTIL_LOG(DEBUG, "handler: %p, finish finshed: %p, ok: %d",
                  _handler.get(), this, _ok);
        _handler->finishCallback(_ok);
        _handler->runNext(false);
    }

private:
    AUTIL_LOG_DECLARE();
};

class GigStreamCancelClosure : public GigStreamClosureBase {
public:
    GigStreamCancelClosure(const GigStreamHandlerBasePtr &handler)
        : GigStreamClosureBase(handler) {
        AUTIL_LOG(DEBUG, "handler: %p, begin cancel: %p", _handler.get(), this);
    }
    ~GigStreamCancelClosure() {}

public:
    void run() {
        AUTIL_LOG(DEBUG, "handler: %p, cancel finshed: %p, ok: %d",
                  _handler.get(), this, _ok);
        _handler->cancelCallback(_ok);
        _handler->runNext(true);
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMCLOSURE_H
