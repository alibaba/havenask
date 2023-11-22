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
#ifndef HTTP_ARPC_HTTPRPCSERVERCLOSURE_H
#define HTTP_ARPC_HTTPRPCSERVERCLOSURE_H

#include <map>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/http_arpc/HTTPRPCController.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"

namespace anet {
class Connection;
class HTTPPacket;
} // namespace anet

namespace http_arpc {

struct EagleInfo {
    std::string traceid;
    std::string rpcid;
    std::string udata;
    std::string gigdata;

public:
    bool isEmpty() const { return traceid.empty() && rpcid.empty() && udata.empty(); }
};

class HTTPRPCServerClosure : public RPCClosure {
public:
    HTTPRPCServerClosure(anet::Connection *connection,
                         RPCMessage *requestMessage,
                         RPCMessage *responseMessage,
                         bool keepAlive,
                         const std::string &encoding,
                         const ProtoJsonizerPtr &protoJsonizer,
                         const EagleInfo &eagleInfo,
                         const std::string &aiosDebugType);
    ~HTTPRPCServerClosure();

public:
    /* override */ void Run();
    HTTPRPCController *getController() { return &_httpController; }
    const std::string &getAiosDebugType() const {
        return _aiosDebugType;
    }
    // for case
    void setProtoJsonizer(ProtoJsonizerPtr protoJsonizer);
    ProtoJsonizerPtr getProtoJsonizer() const;
    const EagleInfo &getEagleInfo() const { return _eagleInfo; }
    std::string getResponseHeader(const std::string &key) {
        auto iter = _responseHeader.find(key);
        if (iter != _responseHeader.end()) {
            return iter->second;
        }
        return "";
    }
    void addResponseHeader(const std::string &key, const std::string &value) {
        if (key.empty()) {
            return;
        }
        _responseHeader[key] = value;
    }

private:
    anet::HTTPPacket *buildPacket();

private:
    anet::Connection *_connection;
    RPCMessage *_requestMessage;
    RPCMessage *_responseMessage;
    HTTPRPCController _httpController;
    bool _keepAlive;
    std::string _encoding;
    ProtoJsonizerPtr _protoJsonizer;
    EagleInfo _eagleInfo;
    std::map<std::string, std::string> _responseHeader;
    std::string _aiosDebugType;
};

} // namespace http_arpc

#endif // HTTP_ARPC_HTTPRPCSERVERCLOSURE_H
