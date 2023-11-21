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
#include "client/C2GatewayClientImpl.h"
#include "autil/StringUtil.h"
#include "carbon/Log.h"
#include "hippo/proto/Client.pb.h"
#include "master/HttpClient.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "master/ANetTransportSingleton.h"

BEGIN_C2_NAMESPACE(gateway);

using namespace autil;
using namespace anet;
using namespace autil::legacy;
using namespace carbon;
using namespace carbon::master;
using namespace hippo;

const std::string OPERATION_API = "/api/v1/hippos/operation";

CARBON_LOG_SETUP(c2, C2GatewayClientImpl);

C2GatewayClientImpl::C2GatewayClientImpl() {}

C2GatewayClientImpl::~C2GatewayClientImpl() {
    carbon::master::ANetTransportSingleton::stop();
}

bool C2GatewayClientImpl::init(const std::string& gatewayConf) {
    // gatwayConf host={host};clusterId={clusterId};platform={platform}
    vector<string> gatewayConfigs = StringUtil::split(gatewayConf, ";");
    if (gatewayConfigs.size() < 3) {
        CARBON_LOG(ERROR, "init parse gatewayConf error %s", gatewayConf.c_str());
        return false;
    }
    for (size_t i = 0; i < gatewayConfigs.size(); i++) {
        const string configStr = gatewayConfigs[i];
        vector<string> kv = StringUtil::split(configStr, "=");
        if (kv.size() != 2) {
            continue;
        }
        StringUtil::toLowerCase(kv[0]);
        if ("host" == kv[0]) {
            _host = "tcp:"+kv[1]; //tcp:xxxx
        }
        else if ("clusterid" == kv[0]) {
            _clusterId = kv[1];
        }
        else if ("platform" == kv[0]) {
            _platform = kv[1];
        }
    }
    if (_host.empty() || _clusterId.empty() || _platform.empty()) {
        CARBON_LOG(ERROR, "init parse gatewayConf error %s", gatewayConf.c_str());
        return false;
    }

    if (!carbon::master::ANetTransportSingleton::init()) {
        CARBON_LOG(ERROR, "init anet transport failed");
        return false;
    }

    const int TIMEOUT = 60 * 1000;
    _http.reset(new carbon::master::HttpClient());
    carbon::master::HttpClientOpts opts;
    opts.contentType = "application/json";
    opts.timeout = TIMEOUT;
    _http->setOpts(opts);
    CARBON_LOG(INFO, "create gateway client with host: %s, timeout: %dms", _host.c_str(), TIMEOUT);
    return true;
}

bool C2GatewayClientImpl::submitApplication(const hippo::proto::SubmitApplicationRequest& request,
    hippo::proto::SubmitApplicationResponse* response) {
    autil::legacy::json::JsonMap jsonReq;
    http_arpc::ProtoJsonizer::toJsonMap(request, jsonReq);
    auto arpcOperation = newArpcOperation("/ClientService/submitApplication", jsonReq);
    return doRequest(__func__, HTTPPacket::HM_POST, OPERATION_API, arpcOperation, response);
}

bool C2GatewayClientImpl::stopApplication(const hippo::proto::StopApplicationRequest& request,
    hippo::proto::StopApplicationResponse* response) {
    autil::legacy::json::JsonMap jsonReq;
    http_arpc::ProtoJsonizer::toJsonMap(request, jsonReq);
    auto arpcOperation = newArpcOperation("/ClientService/stopApplication", jsonReq);
    return doRequest(__func__, HTTPPacket::HM_POST, OPERATION_API, arpcOperation, response);
}

bool C2GatewayClientImpl::getAppStatus(const hippo::proto::AppStatusRequest& request, hippo::proto::AppStatusResponse* response) {
    autil::legacy::json::JsonMap jsonReq;
    http_arpc::ProtoJsonizer::toJsonMap(request, jsonReq);
    auto arpcOperation = newArpcOperation("/ClientService/getAppStatus", jsonReq);
    return doRequest(__func__, HTTPPacket::HM_POST, OPERATION_API, arpcOperation, response);
}

bool C2GatewayClientImpl::updateAppMaster(const hippo::proto::UpdateAppMasterRequest& request,
    hippo::proto::UpdateAppMasterResponse* response) {
    autil::legacy::json::JsonMap jsonReq;
    http_arpc::ProtoJsonizer::toJsonMap(request, jsonReq);
    auto arpcOperation = newArpcOperation("/ClientService/updateAppMaster", jsonReq);
    return doRequest(__func__, HTTPPacket::HM_POST, OPERATION_API, arpcOperation, response);
}

template <typename T>
bool C2GatewayClientImpl::doRequest(const std::string& apiHint, anet::HTTPPacket::HTTPMethod method, const std::string& api,
    const ArpcOperation<T> req, ::google::protobuf::Message* result, bool verbose) {
    carbon::master::QueryResult httpResult;
    std::string body;
    int64_t beginTime = autil::TimeUtility::currentTime();

    body = ToJsonString(req, true);

    if (verbose) {
        CARBON_LOG(INFO, "request gateway, api: %s, uri: %s, host: %s, body: %s", apiHint.c_str(), api.c_str(),
            _host.c_str(), body.c_str());
    }
    _http->request(method, _host, api, body, httpResult);
    if (httpResult.statusCode != 200) {
        CARBON_LOG(WARN, "request gateway failed: %s, %d, %s (%s)", api.c_str(), httpResult.statusCode,
            httpResult.content.c_str(), body.c_str());
        return false;
    }
    try {
        GatewayResponse resp;
        FromJsonString(resp, httpResult.content);
        if (resp.status == 0) {
            http_arpc::ProtoJsonizer::fromJsonMap(resp.data, result);
        } else {
            CARBON_LOG(WARN, "request gateway failed: %s, %s", api.c_str(), httpResult.content.c_str());
            return false;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        CARBON_LOG(WARN, "parse gateway response failed: %s, %s, err %s", api.c_str(), httpResult.content.c_str(),
            e.what());
        return false;
    }

    if (verbose) {
        int64_t endTime = autil::TimeUtility::currentTime();
        int64_t usedTimeMS = (endTime - beginTime) / 1000;
        CARBON_LOG(INFO, "request gateway success, api: %s, uri: %s, host: %s, length: %zu, used: %ld ms",
            apiHint.c_str(), api.c_str(), _host.c_str(), httpResult.content.length(), usedTimeMS);
    }
    return true;
}


template <typename T>
C2GatewayClientImpl::ArpcOperation<T> C2GatewayClientImpl::newArpcOperation(const std::string path, T value) {
    std::map<std::string, std::string> metaInfo;
    metaInfo["platform"] = _platform;
    metaInfo["clusterId"] = _clusterId;
    ArpcOperation<T> arpcOperation;
    arpcOperation.op = "POST";
    arpcOperation.path = path;
    arpcOperation.metaInfo = metaInfo;
    arpcOperation.value = value;
    return arpcOperation;
}

END_C2_NAMESPACE(gateway);

namespace c2 {
    GatewayClient* GatewayClient::create() { return new c2::gateway::C2GatewayClientImpl; }
};
