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
#include "sdk/C2ProxyClient.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/EnvUtil.h"

using namespace autil;
using namespace anet;
using namespace autil::legacy;

BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, C2ProxyClient);

inline std::string URI(const std::vector<std::string>& sec) {
    return StringUtil::toString(sec, "/");
}

typedef std::vector<std::string> VSTR;

C2ProxyClient::C2ProxyClient(const std::string& host) : _host(host) {
    _accept = CONTENT_TYPE_PB;
    _defaultTimeOut = 150*1000;
    hippo::http_client::HttpClientOpts opts;
    opts.timeout = _defaultTimeOut;
    _http.reset(new http_client::HttpClient());
    _http->setOpts(opts);
    _http->init();
}

bool C2ProxyClient::allocate(const proto::AllocateRequest &request, proto::AllocateResponse &response) {
    auto uri = URI(VSTR{"/apis/v1/slots/apps", _app, "allocate"});
    http_client::QueryResult httpResult;
    doRequest(HTTPPacket::HM_POST, uri, request, httpResult);
    bool ok = parseAllocateResult(uri, httpResult, response);
    HIPPO_LOG(DEBUG, "query c2-proxy: %s, %s, %d, request: %s", _host.c_str(), uri.c_str(), ok, http_arpc::ProtoJsonizer::toJsonString(response).c_str());
    if (response.errorinfo().errorcode() != proto::ERROR_NONE) {
        HIPPO_LOG(ERROR, "allocate failed. errorCode:[%d], errorMsg:[%s]",
                  response.errorinfo().errorcode(),
                  response.errorinfo().errormsg().c_str());
        return false;
    }
    return ok;
}

bool C2ProxyClient::parseAllocateResult(const std::string& api, http_client::QueryResult &httpResult, proto::AllocateResponse& result) {
    if (httpResult.statusCode != 200) {
        HIPPO_LOG(WARN, "request c2-proxy failed: %s, %d, %s", api.c_str(), httpResult.statusCode, httpResult.content.c_str());
        return false;
    }
    if (!result.ParseFromString(httpResult.content)) {
        HIPPO_LOG(WARN, "parse c2-proxy response failed: %s, %lu, %s", api.c_str(), httpResult.content.size(), httpResult.content.c_str());
        return false;
    }
    HIPPO_LOG(DEBUG, "parse c2-proxy response: %s, %lu, %s", api.c_str(), httpResult.content.size(), httpResult.content.c_str());
    return  true;
}

bool C2ProxyClient::launch(const proto::ProcessLaunchRequest &request, proto::ProcessLaunchResponse &response) {
    auto uri = URI(VSTR{"/apis/v1/slots/apps", _app, "launch"});
    http_client::QueryResult httpResult;
    doRequest(HTTPPacket::HM_POST, uri, request, httpResult);
    HIPPO_LOG(DEBUG, "query c2-proxy: %s, %s, request: %s", _host.c_str(), uri.c_str(), http_arpc::ProtoJsonizer::toJsonString(request).c_str());
    return parseLaunchResult(uri, httpResult, response);
}

bool C2ProxyClient::parseLaunchResult(const std::string& api, http_client::QueryResult &httpResult, proto::ProcessLaunchResponse& result) {
    if (httpResult.statusCode != 200) {
        HIPPO_LOG(WARN, "request c2-proxy failed: %s, %d, %s", api.c_str(), httpResult.statusCode, httpResult.content.c_str());
        return false;
    }
    if (!result.ParseFromString(httpResult.content)){
        HIPPO_LOG(WARN, "parse c2-proxy response failed: %s, %lu, %s", api.c_str(), httpResult.content.size(), httpResult.content.c_str());
        return false;
    }
    HIPPO_LOG(DEBUG, "parse c2-proxy response: %s, %lu", api.c_str(), httpResult.content.size());
    return  true;
}

bool C2ProxyClient::buildHttpRequest(const ::google::protobuf::Message &message,
                                    const std::string &uri,
                                    const anet::HTTPPacket::HTTPMethod method,
                                    HTTPPacket *httpRequest) const
{
    httpRequest->setMethod(method);
    httpRequest->setURI(uri.c_str());
    httpRequest->addHeader("Accept", _accept.c_str());
    httpRequest->addHeader("Content-Type", _accept.c_str());
    httpRequest->addHeader("Connection", "Keep-Alive");
    std::string host = getHostFromSpec(_host);
    httpRequest->addHeader("Host", host.c_str());
    std::string data;
    message.SerializeToString(&data);
    httpRequest->setBody(data.c_str(), data.size());
    return true;
}

std::string C2ProxyClient::getHostFromSpec(const std::string& spec) const {
    size_t pos = spec.find(":");
    if (pos == std::string::npos) {
        return spec;
    }
    std::string protocolPrefix = spec.substr(0, pos);
    if (protocolPrefix != "tcp") {
        return spec;
    }
    return spec.substr(pos + 1);
}

bool C2ProxyClient::doRequest(anet::HTTPPacket::HTTPMethod method, const std::string& api,
                             const ::google::protobuf::Message& req, http_client::QueryResult &httpResult) {
    HTTPPacket *httpRequest = new HTTPPacket;
    buildHttpRequest(req, api, method, httpRequest);
    _http->doSendQuery(_host, httpRequest, httpResult);
    // httpRequest->free();
    return true;
}

HIPPO_LOG_SETUP(sdk, C2ProxyClientCreator);

C2ProxyClient* C2ProxyClientCreator::createC2ProxyClient() {
    std::string host = autil::EnvUtil::getEnv(K_C2_DRIVER_PROXY);
    HIPPO_LOG(INFO, "c2 driver proxy host: %s", host.c_str());
    if (!host.empty()) {
        return new C2ProxyClient(host);
    }
    return NULL;
}

END_HIPPO_NAMESPACE(sdk);
