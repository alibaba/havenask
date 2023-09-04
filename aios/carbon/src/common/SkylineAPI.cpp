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
#include "common/SkylineAPI.h"
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/legacy/md5.h>
#include <autil/legacy/string_tools.h>

BEGIN_CARBON_NAMESPACE(common);

using namespace autil;
using namespace autil::legacy;

CARBON_LOG_SETUP(common, SkylineAPI);

// skyline api doc: https://yuque.antfin-inc.com/at7ocb/qbn0oy/kzr3tz
// better to append carbon application name ? to track the api usage
const static std::string APPNAME = "carbon";
const static Operator DEFAULT_OPER("SYSTEM", APPNAME, APPNAME);

SkylineAPI::SkylineAPI(HttpClientPtr http, const SkylineConf& conf) : _http(http), _conf(conf) {
    HttpClientOpts opts;
    opts.contentType = "application/json";
    opts.headers["account"] = conf.user;
    _http->setOpts(opts);
    _conf.host = HttpUtil::fixHostSpec(_conf.host);
}

bool SkylineAPI::moveServerGroup(const std::vector<std::string>& ips, const std::string& group, bool offline, const std::string& appUseType) {
    std::vector<std::string> sns;
    if (!querySNList(ips, sns)) {
        return false;
    }
    if (!moveServerGroup(sns, group)) {
        CARBON_LOG(WARN, "move host group failed: %s: %s", group.c_str(), ToJsonString(ips, true).c_str());
        return false;
    }
    // simply set all servers' state even it's already offline/online
    if (!setServerState(sns, offline ? SERVER_STATE_OFFLINE : SERVER_STATE_ONLINE)) {
        CARBON_LOG(WARN, "set server state failed: %s", ToJsonString(ips, true).c_str());
        return false;
    }
    if (!appUseType.empty()) {
        // simply set all servers' appUseType
        setServerUseType(sns, appUseType); 
    }
    return true;
}

bool SkylineAPI::moveServerGroup(const std::vector<std::string>& sns, const std::string& group) {
    BatchUpdateGroup req(group);
    return batchUpdate(sns, req);
}

bool SkylineAPI::setServerState(const std::vector<std::string>& sns, const std::string& state) {
    BatchUpdateState req(state);
    return batchUpdate(sns, req);
}

bool SkylineAPI::setServerUseType(const std::vector<std::string>& sns, const std::string& type) {
    for (auto sn : sns) {
        if (!setServerUseType(sn, type)) {
            CARBON_LOG(WARN, "set server app_use_type failed: %s %s", sn.c_str(), type.c_str());
        }
    }
    return true;
}

bool SkylineAPI::setServerUseType(const std::string& sn, const std::string& type) {
    Result<void> result;
    return doRequest("/openapi/device/server/update", setAppUseTypeReq(sn, type), &result);
}

template <typename T>
bool SkylineAPI::batchUpdate(const std::vector<std::string>& sns, T req) {
    for (size_t i = 0; i < sns.size(); i += BATCH_UPDATE_MAX) {
        std::vector<std::string> tmpSns(sns.begin() + i, 
                i + BATCH_UPDATE_MAX < sns.size() ? sns.begin() + i + BATCH_UPDATE_MAX : sns.end());
        req.snList.swap(tmpSns);
        if (!batchUpdateStep(req)) {
            CARBON_LOG(WARN, "batch update server failed: %s", ToJsonString(req.snList, true).c_str());
            return false;
        }
    }
    return true;
}

template <typename T>
bool SkylineAPI::batchUpdateStep(const T& req) {
    Result<void> result;
    return doRequest("/openapi/device/server/batch_update", req, &result);
}

bool SkylineAPI::queryGroupServers(const std::string& group, std::vector<Server>* servers) {
    Query query("server", "sn,ip,app_server_state,app_use_type", "app_group.name='" + group + "'");
    if (!queryPage("/item/query", query, servers)) {
        return false;
    }
    return true;
}

bool SkylineAPI::querySNList(const std::vector<std::string>& ips, std::vector<std::string>& sns) {
    // `IN' exp limit size to 1000
    for (size_t i = 0; i < ips.size(); i += NUMBER_IN_PAGE) {
        std::vector<std::string> ipQuoteList;
        auto end = i + NUMBER_IN_PAGE > ips.size() ? ips.end() : ips.begin() + i + NUMBER_IN_PAGE;
        std::transform(ips.begin() + i, end, std::back_inserter(ipQuoteList),
                [] (const std::string& ip) { return "'" + ip + "'"; });
        std::string ipstr = StringUtil::toString(ipQuoteList, ",");
        Query query("server", "sn", "ip IN " + ipstr);
        std::vector<Server> items;
        if (!queryPage("/item/query", query, &items)) {
            CARBON_LOG(WARN, "query server sn list failed: %s", ipstr.c_str());
            return false;
        }

        std::transform(items.begin(), items.end(), std::back_inserter(sns),
                [] (const Server& server) { return server.sn; });
    }
    if (sns.size() < ips.size()) {
        CARBON_LOG(WARN, "not found all ips sn (%lu, %lu)", sns.size(), ips.size());
    }
    return true;
}

template <typename T>
bool SkylineAPI::queryPage(const std::string& api, Query& query, std::vector<T>* items) {
    bool hasMore = true;
    do {
        Result<QueryResultT<T>> result;
        if (!doRequest(api, query, &result)) {
            CARBON_LOG(WARN, "request skyline failed at page %d", query.page);
            return false;
        }
        auto &pageItems = result.value.itemList;
        if (!pageItems.empty()) {
            items->insert(items->end(), pageItems.begin(), pageItems.end());
        }
        hasMore = result.value.hasMore;
        query.page ++;
    } while (hasMore);
    return true;
}

template <typename T, typename RT>
bool SkylineAPI::doRequest(const std::string& api, const T& item, Result<RT>* result) {
    auto tm = TimeUtility::currentTimeInSeconds();
    auto sig = createMd5Key(_conf.user, _conf.userKey, tm);

    QueryResult httpResult;
    Request<T> request(Auth(APPNAME, _conf.user, sig, tm), item, DEFAULT_OPER);
    std::string body = ToJsonString(request, true);
    _http->post(_conf.host, api, body, httpResult);

    if (httpResult.statusCode != 200) {
        CARBON_LOG(WARN, "request skyline failed: %s, %d, %s (%s)", api.c_str(), httpResult.statusCode, httpResult.content.c_str(), body.c_str());
        return false;
    }
    try {
        FromJsonString(*result, httpResult.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(WARN, "parse skyline response failed: %s, err (%s), %s", api.c_str(), e.what(), httpResult.content.c_str());
        return false;
    }
    if (!result->success) {
        CARBON_LOG(WARN, "skyline response failed: %s, %s, (%s)", api.c_str(), httpResult.content.c_str(), body.c_str());
        return false;
    }
    return  true;
}

std::string SkylineAPI::createMd5Key(const std::string& user, const std::string& ukey, int64_t tm) {
    std::string str = user + ukey + autil::StringUtil::toString(tm);
    uint8_t out[16];
    DoMd5((uint8_t*)str.c_str(), str.size(), out);
    std::string outs((char*) out, sizeof(out));   
    return ToLowerCaseString(StringToHex(outs));
}

END_CARBON_NAMESPACE(common);
