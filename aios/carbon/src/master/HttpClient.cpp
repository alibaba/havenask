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
#include "carbon/Log.h"
#include "monitor/MonitorUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include <climits>

using namespace std;
using namespace autil;
using namespace anet;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HttpClient);

#define HTTP_QUERY_TIMEOUT (1000 * 30)  // 30 sec

HttpClient::HttpClient() : _streamer(&_factory) {
    _streamer.setPkgLimit(ULONG_MAX);
}

HttpClient::~HttpClient() {
}

void HttpClient::queryStatus(const vector<string> &specVec,
                             const string &path,
                             std::vector<int32_t> &statusVec)
{
    vector<string> datas;
    vector<QueryResult> queryResultVec;
    query(specVec, path, datas, queryResultVec);

    assert(specVec.size() == queryResultVec.size());

    statusVec.resize(queryResultVec.size());
    for (size_t i = 0; i < queryResultVec.size(); i++) {
        statusVec[i] = queryResultVec[i].statusCode;
    }
}

void HttpClient::get(const std::string &spec_, const std::string &uri_,
                     QueryResult &queryResult)
{
    int64_t startTime = TimeUtility::currentTime();

    auto spec = spec_;
    auto uri = uri_;
    const int redirectLimit = 16;
    int redirectLevel = 0;
    while (redirectLevel < redirectLimit) {
        HTTPPacket *request = createGetRequest(spec, uri);
        CARBON_LOG(DEBUG, "query server spec:%s, uri:%s.", spec.c_str(), uri.c_str());
        queryResult.debugInfo = spec + ":" + uri;
        doSendQuery(spec, request, queryResult);
        if (302 != queryResult.statusCode) {
            break;
        }
        tie(spec, uri) = splitUrl(queryResult.relocationUrl);
        if (spec.empty()) {
            CARBON_LOG(WARN, "can not get spec from redirect url:%s",
                       queryResult.relocationUrl.c_str());
        }
        ++redirectLevel;
    }
    if (redirectLevel >= redirectLimit) {
        CARBON_LOG(WARN, "max redirect level %d reached", redirectLimit);
    }

    int64_t endTime = TimeUtility::currentTime();
    int64_t queryTime = (endTime - startTime) / 1000;
    if (queryTime > 500) {
        CARBON_LOG(WARN, "query to [%s/%s], usedTime [%ld ms], result[%s].",
                   spec.c_str(), uri.c_str(), queryTime,
                   queryResult.content.c_str());
    }
}

void HttpClient::get(const string &url, QueryResult &queryResult) {
    auto splited_url = splitUrl(url);
    get(std::get<0>(splited_url), std::get<1>(splited_url), queryResult);
}

tuple<string, string> HttpClient::splitUrl(const string &url) {
    if (url.find("http://") != 0) {
        CARBON_LOG(WARN, "not valid url: %s", url.c_str());
        return make_tuple("", "");
    }
    auto headSize = sizeof("http://") - 1;
    auto body = url.substr(headSize);
    auto splitIdx = body.find('/');

    string spec;
    string uri;
    
    if (splitIdx == string::npos) {
        spec = body;
        uri = "/";
    } else {
        spec = body.substr(0, splitIdx);
        uri = body.substr(splitIdx);
    }
    
    if (spec.find(":") == string::npos) {
        spec += ":80";
    }
    return make_tuple("tcp:" + spec, uri);
}

void HttpClient::query(const vector<string> &specVec,
                       const string &uri,
                       const vector<string> &dataVec,
                       vector<QueryResult> &queryResultVec)
{
    size_t hostCount = specVec.size();
    vector<Connection*> conns;
    conns.reserve(hostCount);
    queryResultVec.resize(hostCount);

    for (size_t i = 0; i < hostCount; i++) {
        const string &hostSpec = specVec[i];
        Connection *conn = ANetTransportSingleton::getInstance()->connect(hostSpec.c_str(),
                &_streamer, false);
        if (conn == NULL) {
            CARBON_LOG(ERROR, "connect to %s failed.", hostSpec.c_str());
            queryResultVec[i].statusCode = 404;
            continue;
        }
        _counter.inc();
        bool ret = false;
        if (dataVec.size() == 0) {
            queryResultVec[i].debugInfo = hostSpec + ":" + uri;
            ret = doQuery(conn, hostSpec, uri, &queryResultVec[i]);
        } else {
            string data;
            if (i < dataVec.size()) {
                data = dataVec[i];
            }
            queryResultVec[i].debugInfo = hostSpec + ":" + uri;
            ret = doQuery(conn, hostSpec, uri, data, &queryResultVec[i]);
        }

        if (!ret) {
            CARBON_LOG(ERROR, "query to [%s] failed.",
                       queryResultVec[i].debugInfo.c_str());
            _counter.dec();
        }
        conns.push_back(conn);
    }

    _counter.wait();

    releaseConns(conns);
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

HTTPPacket* HttpClient::createGetRequest(const string &spec, const string &uri) {
    CARBON_LOG(DEBUG, "get uri:[%s] from spec:[%s].", uri.c_str(), spec.c_str());
    HTTPPacket *request = createBaseHttpRequest(spec, uri, HTTPPacket::HM_GET);
    CARBON_LOG(DEBUG, "request len: %zd.", request->getBodyLen());
    return request;
}

HTTPPacket* HttpClient::createPostRequest(const string &spec, const string &uri,
        const string &data)
{
    return createPayloadRequest(HTTPPacket::HM_POST, spec, uri, data);
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

void HttpClient::releaseConns(const vector<Connection*> &conns) {
    for (size_t i = 0; i < conns.size(); i++) {
        assert(conns[i]);
        conns[i]->close();
        conns[i]->subRef();
    }
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
            auto contentType = httpPacket->getHeader("Content-Type");
            if (contentType) {
                queryResult->contentType = string(contentType);
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

void HttpClient::delete_(const std::string &spec,
                      const std::string &uri,
                      QueryResult &queryResult)
{
    request(HTTPPacket::HM_DELETE, spec, uri, std::string(), queryResult);
}

void HttpClient::put(const std::string &spec,
                      const std::string &uri,
                      const std::string &body,
                      QueryResult &queryResult)
{
    request(HTTPPacket::HM_PUT, spec, uri, std::string(), queryResult);
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

void HttpClient::request(anet::HTTPPacket::HTTPMethod method, const std::string &spec,
                    const std::string &uri,
                    const std::string &accept,
                    const std::string &body,
                    QueryResult &queryResult)
{
    queryResult.debugInfo = spec + ":" + uri;
    HTTPPacket *request = createPayloadRequest(method, spec, uri, body);
    request->addHeader("accept", (char*)accept.c_str());
    doSendQuery(spec, request, queryResult);
}

void HttpClient::post(const std::string &spec,
                      const std::string &uri,
                      const std::string &paramData,
                      QueryResult &queryResult)
{
    int64_t startTime = TimeUtility::currentTime();
    queryResult.debugInfo = spec + ":" + uri;
    HTTPPacket *request = createPostRequest(spec, uri, paramData);
    doSendQuery(spec, request, queryResult);
    int64_t endTime = TimeUtility::currentTime();
    int64_t postTime = (endTime - startTime) / 1000;
    if (postTime > 2000) {
        CARBON_LOG(WARN, "post to [%s/%s], usedTime [%ld ms], result [%s], paramData [%s].",
               spec.c_str(), uri.c_str(), postTime, queryResult.content.c_str(), paramData.c_str());
    }
}

void HttpClient::post(const std::string &url,
                      const std::string &paramData,
                      QueryResult &queryResult)
{
    tuple<string, string> splitedUrl = splitUrl(url);
    string spec = std::get<0>(splitedUrl);
    if (spec == "") {
        CARBON_LOG(ERROR, "invalid url:%s", url.c_str());
        queryResult.statusCode = 404;
        queryResult.content = "invalid url";
        return;
    }

    string uri = std::get<1>(splitedUrl);
    if (uri == "") {
        uri = "/";
    }
    post(spec, uri, paramData, queryResult);
}

void HttpClient::doSendQuery(const string &spec,
                             HTTPPacket* request,
                             QueryResult &queryResult)
{
    Connection *conn = ANetTransportSingleton::getInstance()->connect(
            spec.c_str(), &_streamer, false);

    if (conn == NULL) {
        CARBON_LOG(ERROR, "connect to %s failed.", spec.c_str());
        queryResult.statusCode = 404;
        request->free();
        return ;
    }

    Packet *packet = conn->sendPacket(request, false);
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

    conn->close();
    conn->subRef();
}

bool HttpClient::doQuery(Connection *conn,
                         const string &spec,
                         const string &uri,
                         QueryResult *queryResult)
{
    assert(conn);
    HTTPPacket *request = createGetRequest(spec, uri);
    if (!conn->postPacket(request, this, queryResult, false)) {
        request->free();
        return false;
    }
    return true;
}

bool HttpClient::doQuery(Connection *conn,
                         const string &spec,
                         const string &uri,
                         const string &data,
                         QueryResult *queryResult)
{
    CARBON_LOG(DEBUG, "post packet data:%s", data.c_str());
    HTTPPacket *request = createPostRequest(spec, uri, data);
    if (!conn->postPacket(request, this, queryResult, false)) {
        request->free();
        return false;
    }
    return true;
}

void HttpClient::query(const std::vector<std::string> &urls,
                       const std::vector<std::string> &datas,
                       std::vector<QueryResult> *resultVec)
{
    vector<string> specs;
    string uri;
    for (size_t i = 0; i < urls.size(); i++) {
        string url = urls[i].substr(string("http://").size());
        size_t pos = url.find("/");
        string spec = "tcp:" + url.substr(0, pos);
        if (i == 0) {
            uri = url.substr(pos);
        }
        specs.push_back(spec);
    }

    return query(specs, uri, datas, *resultVec);
}

END_CARBON_NAMESPACE(master);
