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
#ifndef ISEARCH_MULTI_CALL_PROTOCOLCALLBACK_H
#define ISEARCH_MULTI_CALL_PROTOCOLCALLBACK_H

#include "autil/WorkItem.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "autil/LockFreeThreadPool.h"
#include "aios/network/gig/multi_call/service/CallBack.h"

namespace autil {
class ThreadPool;
}

namespace multi_call {

class TcpConnection;
class ArpcConnection;
class HttpConnection;

class TcpCallBack : public anet::IPacketHandler {
public:
    TcpCallBack(const CallBackPtr &callBack,
                autil::LockFreeThreadPool *callBackThreadPool)
        : _callBack(callBack), _callBackThreadPool(callBackThreadPool) {}
    ~TcpCallBack() {}

private:
    TcpCallBack(const TcpCallBack &);
    TcpCallBack &operator=(const TcpCallBack &);

public:
    anet::IPacketHandler::HPRetCode handlePacket(anet::Packet *packet,
                                                 void *args) override;

private:
    CallBackPtr _callBack;
    autil::LockFreeThreadPool *_callBackThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

class HttpCallBack : public anet::IPacketHandler {
public:
    HttpCallBack(const CallBackPtr &callBack,
                 const std::shared_ptr<HttpConnection> &httpConnection,
                 anet::Connection *anetConection,
                 autil::LockFreeThreadPool *callBackThreadPool)
        : _callBack(callBack), _httpConnection(httpConnection),
          _anetConnection(anetConection),
          _callBackThreadPool(callBackThreadPool) {}
    ~HttpCallBack() {}

private:
    HttpCallBack(const HttpCallBack &);
    HttpCallBack &operator=(const HttpCallBack &);

public:
    anet::IPacketHandler::HPRetCode handlePacket(anet::Packet *packet,
                                                 void *args) override;

private:
    CallBackPtr _callBack;
    std::shared_ptr<HttpConnection> _httpConnection;
    anet::Connection *_anetConnection;
    autil::LockFreeThreadPool *_callBackThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

class CallBackWorkItem : public autil::WorkItem {
public:
    CallBackWorkItem(anet::Packet *packet, CallBackPtr &callback)
        : _packet(packet), _callBack(callback) {}
    ~CallBackWorkItem() {}

public:
    void process() override;
    void destroy() override;
    void drop() override;

protected:
    anet::Packet *_packet;
    CallBackPtr _callBack;
};

class HttpCallBackWorkItem : public CallBackWorkItem {
public:
    HttpCallBackWorkItem(anet::Packet *packet, CallBackPtr &callback)
        : CallBackWorkItem(packet, callback) {}
    ~HttpCallBackWorkItem() {}

public:
    void process() override;
};

class ArpcCallBack;

class ArpcCallBackWorkItem : public autil::WorkItem {
public:
    ArpcCallBackWorkItem(ArpcCallBack *callBack) : _callBack(callBack) {}
    ~ArpcCallBackWorkItem() {}

public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    ArpcCallBack *_callBack;

private:
    AUTIL_LOG_DECLARE();
};

class ArpcCallBack : public RPCClosure {
public:
    ArpcCallBack(google::protobuf::Message *request,
                 google::protobuf::Message *response,
                 const CallBackPtr &callBack,
                 autil::LockFreeThreadPool *callBackThreadPool)
        : _request(request), _response(response), _callBack(callBack),
          _callBackThreadPool(callBackThreadPool), _item(this) {}
    virtual ~ArpcCallBack() {
        freeProtoMessage(_request);
        _request = NULL;
    }

public:
    arpc::ANetRPCController *getController() { return &_controller; }

public:
    void Run() override;
    virtual void callBack();

protected:
    MultiCallErrorCode getCompatibleInfo(MultiCallErrorCode currentEc,
                                         std::string &responseInfo);

protected:
    google::protobuf::Message *_request;
    google::protobuf::Message *_response;
    CallBackPtr _callBack;
    arpc::ANetRPCController _controller;
    autil::LockFreeThreadPool *_callBackThreadPool;
    ArpcCallBackWorkItem _item;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_PROTOCOLCALLBACK_H
