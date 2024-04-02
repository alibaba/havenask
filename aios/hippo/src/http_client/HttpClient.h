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
#ifndef HIPPO_HTTPCLIENT_H
#define HIPPO_HTTPCLIENT_H

#include "util/Log.h"
#include "common/common.h"
#include "aios/network/anet/anet.h"
#include "autil/Lock.h"

BEGIN_HIPPO_NAMESPACE(http_client);

struct QueryResult {
    int32_t statusCode;
    std::string content;
    std::string debugInfo;
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
    /* virtual for mock */
    virtual bool init();

    // spec := <tcp:ip:port>
    /* virtual for mock */
    virtual void queryStatus(const std::vector<std::string> &specVec,
                             const std::string &uri,
                             std::vector<int32_t> &statusVec);

    /* virtual for mock */
    virtual void get(const std::string &spec,
                     const std::string &uri,
                     QueryResult &queryResult);

    /* virtual for mock */
    virtual void post(const std::string &spec,
                      const std::string &uri,
                      const std::string &paramData,
                      QueryResult &queryResult);

    /* virtual for mock */
    virtual void query(const std::vector<std::string> &specVec,
                       const std::string &uri,
                       const std::vector<std::string> &datas,
                       std::vector<QueryResult> &resultVec);

    /* virtual for mock */
    virtual void query(const std::vector<std::string> &urls,
                       const std::vector<std::string> &datas,
                       std::vector<QueryResult> *resultVec);

    /* virtual for mock */
    virtual void doSendQuery(const std::string &spec,
                     anet::HTTPPacket* request,
                     QueryResult &queryResult);

    void setOpts(const HttpClientOpts& opts) { _opts = opts; }
protected:
    anet::IPacketHandler::HPRetCode handlePacket(
            anet::Packet *packet, void *args);

private:
    bool doQuery(anet::Connection *conn, const std::string &spec,
                 const std::string &uri, QueryResult *queryResult);

    bool doQuery(anet::Connection *conn, const std::string &spec,
                 const std::string &uri, const std::string &data,
                 QueryResult *queryResult);

    anet::HTTPPacket* createBaseHttpRequest(const std::string &spec,
            const std::string &uri,
            anet::HTTPPacket::HTTPMethod method);

    std::string getHostFromSpec(const std::string &spec);

    anet::HTTPPacket* createGetRequest(const std::string &spec,
            const std::string &uri);

    anet::HTTPPacket* createPostRequest(const std::string &spec,
            const std::string &uri,
            const std::string &data);

    void releaseConns(const std::vector<anet::Connection*> &conns);

    bool processPacket(anet::Packet *packet, QueryResult *queryResult);
private:
    anet::HTTPPacketFactory _factory;
    anet::HTTPStreamer _streamer;
    anet::Transport _transport;
    autil::TerminateNotifier _counter;
    HttpClientOpts _opts;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(HttpClient);

END_HIPPO_NAMESPACE(http_client);

#endif //HIPPO_HTTPCLIENT_H
