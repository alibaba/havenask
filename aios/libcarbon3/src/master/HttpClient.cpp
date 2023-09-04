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
#include "master/HttpClient.h"
#include "master/ANetTransportSingleton.h"
#include "master/Flag.h"
#include "carbon/Log.h"
#include "monitor/CarbonMonitor.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace anet;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HttpClient);

#define HTTP_QUERY_TIMEOUT (1000 * 30)  // 30 sec

HttpClient::HttpClient() : _streamer(&_factory) {
}

HttpClient::~HttpClient() {
}

HTTPPacket* HttpClient::createBaseHttpRequest(const string &spec,
        const string &uri,
        anet::HTTPPacket::HTTPMethod method)
{
    HTTPPacket *request = new HTTPPacket;
    request->setMethod(method);
    request->setURI(uri.c_str());
    request->addHeader("Accept", "*/*");
    request->addHeader("Connection", "Keep-Alive");
    string host = getHostFromSpec(spec);
    //host should be set, or http://jmenv.tbsite.net:8080/vipserver/serverlist will return 400
    request->addHeader("Host", host.c_str());
    for (auto kv : _opts.headers) {
        request->addHeader(kv.first.c_str(), kv.second.c_str());
    }
    int timeout = _opts.timeout > 0 ? _opts.timeout : HTTP_QUERY_TIMEOUT;
    request->setExpireTime(timeout);
    return request;
}

HTTPPacket* HttpClient::createPayloadRequest(HTTPPacket::HTTPMethod method, const string &spec, const string &uri,
        const string &data)
{
    HTTPPacket *request = createBaseHttpRequest(spec, uri, method);
    const char* ct = _opts.contentType.empty() ? "application/x-www-form-urlencoded" : _opts.contentType.c_str();
    request->addHeader("Content-Type", ct);
    request->setBody(data.c_str(), data.size());
    return request;
}

string HttpClient::getHostFromSpec(const string &spec) {
    size_t pos = spec.find(":");
    if (pos == string::npos) {
        return spec;
    }

    string protocolPrefix = spec.substr(0, pos);
    if (protocolPrefix != "tcp") {
        return spec;
    }

    return spec.substr(pos + 1);
}

IPacketHandler::HPRetCode HttpClient::handlePacket(
        Packet *packet, void *args)
{
    QueryResult *queryResult = (QueryResult*) args;
    processPacket(packet, queryResult);
    _counter.dec();

    return IPacketHandler::FREE_CHANNEL;
}

bool HttpClient::processPacket(Packet *packet, QueryResult *queryResult) {
    assert(packet);
    if (!packet->isRegularPacket()) {
        ControlPacket *cmd = dynamic_cast<ControlPacket*>(packet);
        CARBON_LOG(ERROR, "query [%s] failed, error:[%s].",
                   queryResult->debugInfo.c_str(), cmd->what());
        queryResult->statusCode = 404;
        return false;
    } else {
        HTTPPacket* httpPacket = dynamic_cast<HTTPPacket*>(packet);
        if (httpPacket == NULL) {
            CARBON_LOG(ERROR, "cast response packet to HTTPPacket failed.");
            queryResult->statusCode = 404;
            return false;
        } else {
            queryResult->statusCode = httpPacket->getStatusCode();
            auto location = httpPacket->getHeader("Location");
            if (location) {
                queryResult->relocationUrl = string(location);
            }
            CARBON_LOG(DEBUG, "query response, statusCode:%d, len:[%zd], body:[%p].",
                       queryResult->statusCode,
                       httpPacket->getBodyLen(),
                       httpPacket->getBody());
            if (httpPacket->getBodyLen() > 0 && httpPacket->getBody() != NULL) {
                queryResult->content = string(httpPacket->getBody(),
                        httpPacket->getBodyLen());
                CARBON_LOG(DEBUG, "response content:%s",
                        queryResult->content.c_str());
            }
        }
    }
    packet->free();
    return true;
}

void HttpClient::request(anet::HTTPPacket::HTTPMethod method, const std::string &spec,
                    const std::string &uri,
                    const std::string &body,
                    QueryResult &queryResult)
{
    queryResult.debugInfo = spec + ":" + uri;
    HTTPPacket *request = createPayloadRequest(method, spec, uri, body);
    doSendQuery(spec, request, queryResult);
}

void HttpClient::doSendQuery(const string &spec,
                             HTTPPacket* request,
                             QueryResult &queryResult)
{
    //Connection *conn = ANetTransportSingleton::getInstance()->connect(
    //        spec.c_str(), &_streamer, false);
    HttpConnectionPtr conn = _pool->getConnection(spec);
    if (conn == NULL) {
        CARBON_LOG(ERROR, "connect to %s failed.", spec.c_str());
        queryResult.statusCode = 404;
        request->free();
        return ;
    }

    Packet *packet = conn->sendPacket(request, false, Flag::longConnection());
    if (packet == NULL) {
        CARBON_LOG(ERROR, "query to host [%s] failed.", spec.c_str());
        queryResult.statusCode = 404;
        request->free();
    } else {
        if (!processPacket(packet, &queryResult)) {
            CARBON_LOG(ERROR, "send query to host [%s] failed.",
                       spec.c_str());
        }
    }
}

END_CARBON_NAMESPACE(master);
