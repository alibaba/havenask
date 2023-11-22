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
#ifndef HTTP_ARPC_HTTPRPCSERVERWORKITEM_H
#define HTTP_ARPC_HTTPRPCSERVERWORKITEM_H

#include <memory>

#include "HTTPRPCServerClosure.h"
#include "ProtoJsonizer.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "autil/WorkItem.h"

namespace anet {
class Connection;
}

namespace http_arpc {

class HTTPRPCServerWorkItem : public autil::WorkItem {
public:
    HTTPRPCServerWorkItem(RPCService *pService,
                          RPCMethodDescriptor *pMethodDes,
                          anet::Connection *pConnection,
                          bool keepAlive,
                          const std::string &encoding,
                          const ProtoJsonizerPtr &protoJsonizer,
                          const EagleInfo &eagleInfo,
                          std::string requestStr);
    ~HTTPRPCServerWorkItem();

public:
    virtual void process();
    virtual void destroy();
    virtual void drop();

public:
    void SetBeginTime(int64_t beginTime);
    void SetAddr(const std::string &addr);
    void SetAiosDebugType(const char *aiosDebugType);
    void SetRPCServicePtr(const std::shared_ptr<RPCService> &servicePtr) {
        _servicePtr = servicePtr;
    }
private:
    std::shared_ptr<RPCService> _servicePtr;
    RPCService *_service;
    RPCMethodDescriptor *_method;
    anet::Connection *_connection;
    bool _keepAlive;
    std::string _encoding;
    ProtoJsonizerPtr _protoJsonizer;
    EagleInfo _eagleInfo;
    std::string _requestStr;
    int64_t _beginTime;
    std::string _addr;
    std::string _aiosDebugType;
};

} // namespace http_arpc
#endif // HTTP_ARPC_HTTPRPCSERVERWORKITEM_H
