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
#ifndef HTTP_ARPC_HTTPRPCSERVERADAPTER_H
#define HTTP_ARPC_HTTPRPCSERVERADAPTER_H

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "ProtoJsonizer.h"
#include "HTTPRPCServerClosure.h"
#include "autil/Lock.h"

namespace http_arpc {

class HTTPRPCServer;

class HTTPRPCServerAdapter : public anet::IServerAdapter
{
public:
    HTTPRPCServerAdapter(HTTPRPCServer *server);
    ~HTTPRPCServerAdapter();
public:
    /* override */ anet::IPacketHandler::HPRetCode
    handlePacket(anet::Connection *connection, anet::Packet *packet);
    void setProtoJsonizer(const ProtoJsonizerPtr& protoJsonizer);
    ProtoJsonizerPtr& getProtoJsonizer();
private:
    anet::IPacketHandler::HPRetCode handleCmdPacket(
            anet::Connection *connection,
            anet::Packet *packet);
    anet::IPacketHandler::HPRetCode handleRegularPacket(
            anet::Connection *connection,
            anet::Packet *packet);
    void fillInfoFromUri(const std::string& uri,
                         std::string &serviceAndMethod,
                         std::string &request, bool haCompatible = false);
    anet::HTTPPacket *buildErrorPacket(bool isKeepAlive,
            int statusCode,
            std::string reason);
    void handleError(anet::Connection *connection,
                     anet::HTTPPacket *httpPacket,
                     int statusCode,
                     std::string reason);
    void sendError(anet::Connection *connection,
                   anet::HTTPPacket *packet);
    bool processHaCompatible(const std::string& uri,
                             std::string &serviceAndMethod,
                             std::string &request);
    static EagleInfo getEagleInfo(anet::HTTPPacket *httpPacket);

private:
    HTTPRPCServer *_server;
    ProtoJsonizerPtr _protoJsonizer;
    autil::ReadWriteLock _mutex;
};

}
#endif //HTTP_ARPC_HTTPRPCSERVERADAPTER_H
