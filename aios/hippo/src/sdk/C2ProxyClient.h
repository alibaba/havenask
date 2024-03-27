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
// c2-proxy client
#ifndef HIPPO_C2ProxyClient_H
#define HIPPO_C2ProxyClient_H

#include "aios/network/anet/anet.h"
#include "http_client/HttpClient.h"
#include "http_client/PBJson.h"
#include "hippo/DriverCommon.h"
#include "common/common.h"
#include "hippo/proto/ApplicationMaster.pb.h"
#include "hippo/proto/Slave.pb.h"
#include <google/protobuf/message.h>

BEGIN_HIPPO_NAMESPACE(sdk);

#define CONTENT_TYPE_JSON "application/json"
#define CONTENT_TYPE_PB "application/x-protobuf"

class C2ProxyClient
{
private:

public:
    // host: ip:port
    C2ProxyClient(const std::string& host);
    virtual ~C2ProxyClient() {}

    bool allocate(const proto::AllocateRequest &request, proto::AllocateResponse &response);
    bool launch(const proto::ProcessLaunchRequest &request, proto::ProcessLaunchResponse &response);

    void setApplicationId(const std::string &applicationId) {
        _app = applicationId;
    }
    void setHttpClient(const http_client::HttpClientPtr &httpClient) {
        _http = httpClient;
    }

protected:
    //virtual for test
    virtual bool doRequest(anet::HTTPPacket::HTTPMethod method, const std::string& api,
                           const ::google::protobuf::Message& request, http_client::QueryResult &httpResult);

    bool buildHttpRequest(const ::google::protobuf::Message &message,
                          const std::string &uri,
                          const anet::HTTPPacket::HTTPMethod method,
                          anet::HTTPPacket *httpRequest) const;
private:
    bool parseAllocateResult(const std::string& api, http_client::QueryResult &httpResult, proto::AllocateResponse& result);
    bool parseLaunchResult(const std::string& api, http_client::QueryResult &httpResult, proto::ProcessLaunchResponse& result);
    std::string getHostFromSpec(const std::string& spec) const;

 private:
    http_client::HttpClientPtr _http;
    std::string _host;
    std::string _app;
    std::string _accept;
    int32_t _defaultTimeOut;
    HIPPO_LOG_DECLARE();
};

HIPPO_TYPEDEF_PTR(C2ProxyClient);

#define K_C2_DRIVER_PROXY "C2_DRIVER_PROXY"

class C2ProxyClientCreator
{
private:
    HIPPO_LOG_DECLARE();
public:
    static C2ProxyClient* createC2ProxyClient();
};

END_HIPPO_NAMESPACE(sdk);

#endif
