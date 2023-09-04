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
#ifndef CARBON_HTTPCLIENT_H
#define CARBON_HTTPCLIENT_H

#include "common/common.h"
#include "carbon/Log.h"
#include "monitor/CarbonMonitor.h"
#include "master/ConnectionPool.h"
#include "aios/network/anet/anet.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

struct QueryResult {
    int32_t statusCode;
    std::string content;
    std::string debugInfo;
    std::string relocationUrl;
};

struct HttpClientOpts {
    HttpClientOpts() : timeout(0) {}
    std::string contentType;
    std::map<std::string, std::string> headers;
    int timeout;
};

class HttpClient : public anet::IPacketHandler
{
public:
    HttpClient();
    ~HttpClient();
private:
    HttpClient(const HttpClient &);
    HttpClient& operator=(const HttpClient &);
    
public:
    virtual void request(anet::HTTPPacket::HTTPMethod method, const std::string &spec,
                      const std::string &uri,
                      const std::string &body,
                      QueryResult &queryResult);

    void setOpts(const HttpClientOpts& opts) { _opts = opts; }

protected:
    anet::IPacketHandler::HPRetCode handlePacket(
            anet::Packet *packet, void *args);

private:
    /* virtual for test */
    virtual void doSendQuery(const std::string &spec,
                             anet::HTTPPacket* request,
                             QueryResult &queryResult);

    anet::HTTPPacket* createBaseHttpRequest(const std::string &spec,
            const std::string &uri,
            anet::HTTPPacket::HTTPMethod method);

    anet::HTTPPacket* createPayloadRequest(anet::HTTPPacket::HTTPMethod method, const string &spec, const string &uri,
            const string &data);
    
    std::string getHostFromSpec(const std::string &spec);

    bool processPacket(anet::Packet *packet, QueryResult *queryResult);
    
    static std::tuple<std::string, std::string> splitUrl(const std::string &url);
private:
    anet::HTTPPacketFactory _factory;
    anet::HTTPStreamer _streamer;
    autil::TerminateNotifier _counter;
    ConnectionPool *_pool = new ConnectionPool();
    HttpClientOpts _opts;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HttpClient);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HTTPCLIENT_H
