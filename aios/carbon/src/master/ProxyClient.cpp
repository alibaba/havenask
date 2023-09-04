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
#include "autil/StringUtil.h"
#include "master/ProxyClient.h"
#include "master/HttpClient.h"
#include "master/GlobalVariable.h"
#include "carbon/proto/Status.pb.h"
#include "carbon/ProtoWrapper.h"

BEGIN_CARBON_NAMESPACE(master);
using namespace autil;
using namespace anet;
using namespace autil::legacy;

CARBON_LOG_SETUP(master, ProxyClient);

inline std::string URI(const std::vector<std::string>& sec) {
    return StringUtil::toString(sec, "/");
}
typedef std::vector<std::string> VSTR;

#define SET_ERRINFO(errInfo, resp) { errInfo->errorCode = (ErrorCode) resp.code; errInfo->errorMsg = resp.msg; }

static const char* SCH_TYPES[] = {"group", "role", "node"};

std::string ProxyClient::formatSchOptions(bool single) {
    std::vector<std::vector<std::string>> vecs;
    const CarbonInfo &tmpCarbonInfo = GlobalVariable::getCarbonInfo();
    vecs.push_back(std::vector<std::string>{"zkAddress", tmpCarbonInfo.carbonZk});
    vecs.push_back(std::vector<std::string>{"schType", single ? SCH_TYPES[1] : SCH_TYPES[0]});
    return StringUtil::toString(vecs, "=", ";");
}

ProxyClient::ProxyClient(const std::string& host, const std::string& app, bool single, bool useJsonEncode) 
        : _host(host), _app(app), _single(single), _useJsonEncode(useJsonEncode) {
    _http.reset(new HttpClient());
    HttpClientOpts opts;
    opts.contentType = CONTENT_TYPE_JSON;
    opts.headers["schOptions"] = formatSchOptions(_single);
    _accept = _useJsonEncode ? CONTENT_TYPE_JSON : CONTENT_TYPE_PB;
    opts.timeout = 80*1000;
    _http->setOpts(opts);
}

bool ProxyClient::addGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group"});
    CommonResponse resp;
    if (!doRequest(HTTPPacket::HM_POST, uri, plan, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group", plan.groupId});
    CommonResponse resp;
    if (!doRequest(HTTPPacket::HM_PUT, uri, plan, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::delGroup(const groupid_t &groupId, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group", groupId});
    CommonResponse resp;
    if (!doRequest(HTTPPacket::HM_DELETE, uri, 0, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::setGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "groups"});
    CommonResponse resp;
    if (!doRequest(HTTPPacket::HM_POST, uri, groupPlanMap, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}


bool ProxyClient::getGroupStatusList(const std::vector<groupid_t>& ids, std::vector<GroupStatus>& statusList, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group", "status"});
    QueryResult httpResult; 
    if (!doQuery(HTTPPacket::HM_POST, uri, ids, httpResult, _accept, false)) {
        return false;
    }
    if (httpResult.contentType == CONTENT_TYPE_PB) {
        proto::Response response;
        if (!parseResult(uri, httpResult, &response)) {
            return false;
        }
        ProtoWrapper::convert(response.data(), &statusList);
    } else {
        StatusResponse result;
        if (!parseResult(uri, httpResult, &result)) {
            return false;
        }
        statusList.swap(result.data);
    }
    return true;
}

bool ProxyClient::reclaimWorkerNodes(const std::vector<ReclaimWorkerNode>& workerNodes, 
    ProxyWorkerNodePreference *pref, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "reclaimWorkerNodes"});
    ReclaimWorkerNodesBody body(workerNodes, pref);
    CommonResponse resp;
    if (!doRequest(HTTPPacket::HM_POST, uri, body, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

template <typename T>
bool ProxyClient::doRequest(anet::HTTPPacket::HTTPMethod method, const std::string& api, const T& req, BaseResponse* result) {
    QueryResult httpResult;
    if (!doQuery(method, api, req, httpResult)){
        return false;
    }
    return parseResult(api, httpResult, result);
}

bool ProxyClient::parseResult(const std::string& api, QueryResult &httpResult, BaseResponse* result) {
    try {
        FromJsonString(*result, httpResult.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(WARN, "parse carbon-proxy response failed: %s, %s, err %s", api.c_str(), httpResult.content.c_str(), e.what());
        return false;
    }
    if (!result->isSuccess()) {
        CARBON_LOG(WARN, "carbon-proxy response failed: %s, %s", api.c_str(), httpResult.content.c_str());
        return false;
    }
    return  true;
}

bool ProxyClient::parseResult(const std::string& api, QueryResult &httpResult, proto::Response* result) {
    if (!result->ParseFromString(httpResult.content)){
        CARBON_LOG(WARN, "parse carbon-proxy response failed: %s, %s, %lu", api.c_str(), httpResult.content.c_str(), httpResult.content.size());
        return false;
    }
    if (result->code() != 0 || result->subcode() != 0) {
        CARBON_LOG(WARN, "carbon-proxy response failed: %s, %s", api.c_str(), httpResult.content.c_str());
        return false;
    }
    return  true;
}

template <typename T>
bool ProxyClient::doQuery(anet::HTTPPacket::HTTPMethod method, const std::string& api, 
                      const T& req, 
                      QueryResult &httpResult, 
                      const std::string& accept,
                      bool verbose) {
    std::string body;
    if (method != HTTPPacket::HM_DELETE) {
        body = ToJsonString(req, true);
    }
    if (verbose) {
        CARBON_LOG(INFO, "request carbon-proxy, method: %d, uri: %s, host: %s, body: %s", method, api.c_str(), _host.c_str(), body.c_str());
    }
    _http->request(method, _host, api, accept, body, httpResult);
    if (httpResult.statusCode != 200) {
        CARBON_LOG(WARN, "request carbon-proxy failed: %s, %d, %s (%s)", api.c_str(), httpResult.statusCode, httpResult.content.c_str(), body.c_str());
        return false;
    }
    return true;
}

END_CARBON_NAMESPACE(master);
