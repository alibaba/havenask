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
#include "HTTPRPCServerAdapter.h"

#include <cstring>
#include <sstream>

#include "HTTPRPCServer.h"
#include "HTTPRPCServerWorkItem.h"
#include "Log.h"
#include "ProtoJsonizer.h"
#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "autil/StringTokenizer.h"
#include "autil/URLUtil.h"

using namespace std;
using namespace arpc;
using namespace anet;
using namespace autil;
using namespace std;

namespace http_arpc {

HTTP_ARPC_DECLARE_AND_SETUP_LOGGER(HTTPRPCServerAdapter);

class PacketFreeGuard {
public:
    PacketFreeGuard(Packet *packet) : _packet(packet) {}
    ~PacketFreeGuard() { _packet->free(); }

private:
    Packet *_packet;
};

HTTPRPCServerAdapter::HTTPRPCServerAdapter(HTTPRPCServer *server) : _server(server) {
    _protoJsonizer.reset(new ProtoJsonizer());
}

HTTPRPCServerAdapter::~HTTPRPCServerAdapter() { _protoJsonizer.reset(); }

IPacketHandler::HPRetCode HTTPRPCServerAdapter::handlePacket(Connection *connection, Packet *packet) {
    PacketFreeGuard freePacket(packet);
    if (packet->isRegularPacket()) {
        return handleRegularPacket(connection, packet);
    } else {
        return handleCmdPacket(connection, packet);
    }
}

IPacketHandler::HPRetCode HTTPRPCServerAdapter::handleCmdPacket(Connection *connection, Packet *packet) {
    ControlPacket *cmd = dynamic_cast<ControlPacket *>(packet);
    assert(cmd);
    HTTP_ARPC_LOG(DEBUG, "Control Packet (%s) received!", cmd->what());
    return IPacketHandler::FREE_CHANNEL;
}

void HTTPRPCServerAdapter::fillInfoFromUri(const string &uri,
                                           string &serviceAndMethod,
                                           string &request,
                                           bool haCompatible) {
    size_t pos = uri.find("?");
    if (pos != string::npos) {
        serviceAndMethod = uri.substr(0, pos);
        request = uri.substr(pos + 1);
        return;
    }
    if (haCompatible && processHaCompatible(uri, serviceAndMethod, request)) {
        return;
    }
    size_t serviceBegin = uri.find("/");
    if (serviceBegin != string::npos) {
        size_t methodBegin = uri.find("/", serviceBegin + 1);
        if (methodBegin != string::npos) {
            size_t requestBegin = uri.find("/", methodBegin + 1);
            if (requestBegin != string::npos) {
                request = uri.substr(requestBegin + 1);
                serviceAndMethod = uri.substr(0, requestBegin);
            } else {
                serviceAndMethod = uri;
                request = "";
            }
        } else {
            serviceAndMethod = uri;
            request = "";
        }
    } else {
        serviceAndMethod = uri;
        request = "";
    }
}

bool HTTPRPCServerAdapter::processHaCompatible(const string &uri, string &serviceAndMethod, string &request) {
    static const string STATUS_CHECK_PREFIX = "status";
    static const string HTTP_SUPPORT_PREFIX = "httpsupport:file";
    static const string HA_QUERY_PREFIX = "query=";
    static const string HA_CONFIG_PREFIX = "config=";
    if (uri.empty()) {
        return false;
    }
    if (uri.find(HA_CONFIG_PREFIX) != string::npos && uri.find(HA_QUERY_PREFIX) != string::npos) {
        serviceAndMethod = "/";
        if (uri[0] == '/') {
            request = uri.substr(1);
        } else {
            request = uri;
        }
        return true;
    }
    size_t pos = uri.find(STATUS_CHECK_PREFIX);
    if (pos == 0 || (pos == 1 && uri[0] == '/')) {
        serviceAndMethod = "/status_check";
        request = uri;
        return true;
    }
    pos = uri.find(HTTP_SUPPORT_PREFIX);
    if (pos == 0 || (pos == 1 && uri[0] == '/')) {
        serviceAndMethod = "/status_check";
        request = uri;
        return true;
    }
    return false;
}

IPacketHandler::HPRetCode HTTPRPCServerAdapter::handleRegularPacket(Connection *connection, Packet *packet) {
    int64_t beginTime = TimeUtility::currentTime();
    char addr[256];
    if (!connection->getIpAndPortAddr(addr, 256)) {
        HTTP_ARPC_LOG(WARN, "get ip addr failed!");
        addr[0] = 0;
    }
    HTTPPacket *httpPacket = dynamic_cast<HTTPPacket *>(packet);
    assert(httpPacket);
    const char *uri = httpPacket->getURI();
    HTTPPacket::HTTPMethod httpMethod = httpPacket->getMethod();

    string serviceAndMethod;
    string request;

    if (uri && unlikely(strcmp(uri, "/__method__") == 0)) {
        stringstream ss;
        vector<string> names = _server->getRPCNames();
        for (size_t i = 0; i < names.size(); i++) {
            ss << names[i] << '\n';
        }
        handleError(connection, httpPacket, 200, ss.str());
        return IPacketHandler::KEEP_CHANNEL;
    }
    if (httpMethod == HTTPPacket::HM_GET) {
        bool haCompatible = _server->haCompatible();
        if (_server->needDecodeUri()) {
            string decodedUri = URLUtil::decode(uri);
            fillInfoFromUri(decodedUri, serviceAndMethod, request, haCompatible);
        } else {
            fillInfoFromUri(uri, serviceAndMethod, request, haCompatible);
        }
    } else if (httpMethod == HTTPPacket::HM_POST) {
        size_t len = 0;
        char *buf = httpPacket->getBody(len);
        serviceAndMethod = uri;
        request = string(buf, len);
    } else {
        stringstream ss;
        ss << "invalid http method " << int(httpMethod);
        handleError(connection, httpPacket, 400, ss.str());
        return IPacketHandler::KEEP_CHANNEL;
    }
    if (request.empty()) {
        request = "{}";
    }
    ServiceMethodPair serviceMethodPair = _server->getMethod(serviceAndMethod);
    RPCService *service = serviceMethodPair.first.second;
    RPCMethodDescriptor *method = serviceMethodPair.second;

    if (!service || !method) {
        stringstream ss;
        ss << "service or method not found for " << serviceAndMethod << ", host: " << addr;
        handleError(connection, httpPacket, 404, ss.str());
        return IPacketHandler::KEEP_CHANNEL;
    }

    const char *encoding = httpPacket->getHeader("Accept-Encoding");
    string encodingStr;
    if (encoding) {
        encodingStr = encoding;
    }
    auto protoJsonizer = getProtoJsonizer(service, method);
    HTTPRPCServerWorkItem *workItem = new HTTPRPCServerWorkItem(service,
                                                                method,
                                                                connection,
                                                                httpPacket->isKeepAlive(),
                                                                encodingStr,
                                                                protoJsonizer,
                                                                getEagleInfo(httpPacket),
                                                                std::move(request));
    workItem->SetBeginTime(beginTime);
    workItem->SetAddr(string(addr));
    if (!_server->PushItem(service, workItem)) {
        handleError(connection,
                    httpPacket,
                    503,
                    method->service()->name() + string(" push to threadpool fail, maybe queue full"));
        return IPacketHandler::KEEP_CHANNEL;
    }
    return IPacketHandler::KEEP_CHANNEL;
}

HTTPPacket *HTTPRPCServerAdapter::buildErrorPacket(bool isKeepAlive, int statusCode, string reason)

{
    HTTPPacket *packet = new HTTPPacket();
    packet->setPacketType(HTTPPacket::PT_RESPONSE);
    packet->setStatusCode(statusCode);
    packet->setBody(reason.c_str(), reason.size());
    packet->setKeepAlive(isKeepAlive);
    return packet;
}

void HTTPRPCServerAdapter::handleError(Connection *connection, HTTPPacket *httpPacket, int statusCode, string reason) {
    HTTP_ARPC_LOG(ERROR, "%s", reason.c_str());
    HTTPPacket *packet = buildErrorPacket(httpPacket->isKeepAlive(), statusCode, reason);
    sendError(connection, packet);
}

void HTTPRPCServerAdapter::sendError(Connection *connection, HTTPPacket *packet) {
    if (!packet->isKeepAlive()) {
        connection->setWriteFinishClose(true);
    }
    if (!connection->postPacket(packet)) {
        packet->free();
    }
}

void HTTPRPCServerAdapter::setProtoJsonizer(const ProtoJsonizerPtr &protoJsonizer) {
    ScopedWriteLock lock(_mutex);
    _protoJsonizer = protoJsonizer;
}

ProtoJsonizerPtr &HTTPRPCServerAdapter::getProtoJsonizer() {
    ScopedReadLock lock(_mutex);
    return _protoJsonizer;
}

bool HTTPRPCServerAdapter::setProtoJsonizer(RPCService *service,
                                            const std::string &method,
                                            const ProtoJsonizerPtr &protoJsonizer) {
    auto serviceDesc = service->GetDescriptor();
    const auto &serviceName = serviceDesc->full_name();
    auto methodDesc = serviceDesc->FindMethodByName(method);
    if (!methodDesc) {
        HTTP_ARPC_LOG(ERROR,
                      "set protoJsonizer failed, method [%s] not found in service [%s]",
                      method.c_str(),
                      serviceName.c_str());
        return false;
    }
    ScopedWriteLock lock(_mutex);
    auto &methodMap = _serviceProtoJsonizerMap[serviceName];
    methodMap[method] = protoJsonizer;
    return true;
}

ProtoJsonizerPtr HTTPRPCServerAdapter::getProtoJsonizer(RPCService *service, RPCMethodDescriptor *method) {
    const auto &name = service->GetDescriptor()->full_name();
    const auto &methodName = method->name();
    ScopedReadLock lock(_mutex);
    auto it = _serviceProtoJsonizerMap.find(name);
    if (it == _serviceProtoJsonizerMap.end()) {
        return _protoJsonizer;
    } else {
        const auto &methodMap = it->second;
        auto methodIt = methodMap.find(methodName);
        if (methodMap.end() == methodIt) {
            return _protoJsonizer;
        } else {
            return methodIt->second;
        }
    }
}

EagleInfo HTTPRPCServerAdapter::getEagleInfo(anet::HTTPPacket *httpPacket) {
    EagleInfo eagleInfo;
    const char *traceId = httpPacket->getHeader("EagleEye-TraceId");
    if (traceId) {
        eagleInfo.traceid = traceId;
    }
    const char *rpcId = httpPacket->getHeader("EagleEye-RpcId");
    if (rpcId) {
        eagleInfo.rpcid = rpcId;
    }
    const char *udata = httpPacket->getHeader("EagleEye-UserData");
    if (udata) {
        eagleInfo.udata = udata;
    }
    const char *gdata = httpPacket->getHeader("GigData");
    if (!gdata) {
        gdata = httpPacket->getHeader("gig_data");
    }
    if (gdata) {
        eagleInfo.gigdata = gdata;
    }
    return eagleInfo;
}

} // namespace http_arpc
