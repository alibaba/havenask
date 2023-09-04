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
#include "autil/EnvUtil.h"
#include "master/ProxyClient.h"
#include "master/HttpClient.h"
#include "monitor/MonitorUtil.h"

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

ProxyClient::ProxyClient(const std::string& host, const std::string& app, int schType) : _host(host), _app(app), _schType(schType) {
    const int TIMEOUT = 80 * 1000;
    _http.reset(new HttpClient());
    HttpClientOpts opts;
    opts.contentType = "application/json";
    opts.headers["schOptions"] = formatSchOptions(_schType);
    opts.timeout = TIMEOUT;
    _http->setOpts(opts);
    CARBON_LOG(INFO, "create proxy client with host: %s, timeout: %dms, schOpts: [%s]", host.c_str(), TIMEOUT, formatSchOptions(_schType).c_str());
}

std::string ProxyClient::formatSchOptions(int schType) {
    std::vector<std::vector<std::string>> vecs;
    vecs.push_back(std::vector<std::string>{"schType", SCH_TYPES[schType]});
    vecs.push_back(std::vector<std::string>{"appChecksum", getAppChecksum()});
    return StringUtil::toString(vecs, "=", ";");
}

std::string ProxyClient::getAppChecksum() {
    string checksum = autil::EnvUtil::getEnv("HIPPO_APP_CHECKSUM");
    return checksum.empty() ? "-1" : checksum;
}

bool ProxyClient::addGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group"});
    CommonResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_POST, uri, plan, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group", plan.groupId});
    CommonResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_PUT, uri, plan, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::delGroup(const groupid_t &groupId, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "group", groupId});
    CommonResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_DELETE, uri, 0, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::setGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "groups"});
    CommonResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_POST, uri, groupPlanMap, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

bool ProxyClient::getGroupStatusList(const std::vector<groupid_t>& ids, std::vector<GroupStatus>& statusList) {
    auto uri = URI(VSTR{"/app", _app, "group", "status"});
    StatusResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_POST, uri, ids, &resp, false)) {
        return false;
    }
    statusList.swap(resp.data);
    return true;
}

bool ProxyClient::reclaimWorkerNodes(const std::vector<ReclaimWorkerNode>& workerNodes, 
    ProxyWorkerNodePreference *pref, ErrorInfo *errorInfo) {
    auto uri = URI(VSTR{"/app", _app, "reclaimWorkerNodes"});
    ReclaimWorkerNodesBody body(workerNodes, pref);
    CommonResponse resp;
    if (!doRequest(__func__, HTTPPacket::HM_POST, uri, body, &resp)) {
        SET_ERRINFO(errorInfo, resp);
        return false;
    }
    return true;
}

template <typename T>
bool ProxyClient::doRequest(const std::string& apiHint, anet::HTTPPacket::HTTPMethod method, const std::string& api, const T& req, BaseResponse* result, bool verbose) {
    if (_host.empty()) { // disable
        return true;
    }
    QueryResult httpResult;
    std::string body;
    int64_t beginTime = autil::TimeUtility::currentTime();
    if (method != HTTPPacket::HM_DELETE) {
        body = ToJsonString(req, true);
    }
    if (verbose) {
        CARBON_LOG(INFO, "request carbon-proxy, api: %s, uri: %s, host: %s, body: %s", apiHint.c_str(), api.c_str(), _host.c_str(), body.c_str());
    }
    {
        carbon::monitor::TagMap tags {{"method", apiHint}};
        REPORT_USED_TIME(METRIC_PROXY_LATENCY, tags);
        _http->request(method, _host, api, body, httpResult);
    }
    if (httpResult.statusCode != 200) {
        CARBON_LOG(WARN, "request carbon-proxy failed: %s, %d, %s (%s)", api.c_str(), httpResult.statusCode, httpResult.content.c_str(), body.c_str());
        return false;
    }
    try {
        carbon::monitor::TagMap tags {{"method", apiHint}};
        REPORT_USED_TIME(METRIC_DECODE_LATENCY, tags);
        FastFromJsonString(*result, httpResult.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(WARN, "parse carbon-proxy response failed: %s, %s, err %s", api.c_str(), httpResult.content.c_str(), e.what());
        return false;
    }
    if (!result->isSuccess()) {
        CARBON_LOG(WARN, "carbon-proxy response failed: %s, %s, (%s)", api.c_str(), httpResult.content.c_str(), body.c_str());
        return false;
    }
    if (verbose) {
        int64_t endTime = autil::TimeUtility::currentTime();
        int64_t usedTimeMS = (endTime - beginTime) / 1000;
        CARBON_LOG(INFO, "request carbon-proxy success, api: %s, uri: %s, host: %s, length: %zu, used: %ld ms",
            apiHint.c_str(), api.c_str(), _host.c_str(), httpResult.content.length(), usedTimeMS);
    }
    return  true;
}

END_CARBON_NAMESPACE(master);
