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
#ifndef CARBON_SKYLINE_API_H
#define CARBON_SKYLINE_API_H

#include "common/common.h"
#include "common/HttpUtil.h"
#include "common/SkylineTypes.h"
#include "carbon/Log.h"
#include "master/HttpClient.h"
#include <autil/legacy/jsonizable.h>

BEGIN_CARBON_NAMESPACE(common);

using namespace master;
USE_CARBON_NAMESPACE(skyline);

struct SkylineConf
{
    std::string host;
    std::string user;
    std::string userKey;
};

class SkylineAPI
{
public:
    SkylineAPI(HttpClientPtr http, const SkylineConf& conf);
    MOCKABLE ~SkylineAPI() {}
    MOCKABLE bool moveServerGroup(const std::vector<std::string>& ips, const std::string& group, bool offline,
            const std::string& appUseType);
    MOCKABLE bool queryGroupServers(const std::string& group, std::vector<Server>* servers);

private:
    bool setServerUseType(const std::vector<std::string>& sns, const std::string& type);
    bool setServerUseType(const std::string& sn, const std::string& type);
    bool moveServerGroup(const std::vector<std::string>& sns, const std::string& group);
    bool setServerState(const std::vector<std::string>& sns, const std::string& state);
    template <typename T>
    bool batchUpdate(const std::vector<std::string>& sns, T req);
    template <typename T>
    bool batchUpdateStep(const T& req);

    bool querySNList(const std::vector<std::string>& ips, std::vector<std::string>& sns);
    template <typename T>
    bool queryPage(const std::string& api, Query& query, std::vector<T>* items);
    template <typename T, typename RT>
    bool doRequest(const std::string& api, const T& item, Result<RT>* result);
    std::string createMd5Key(const std::string& user, const std::string& ukey, int64_t tm);

private:
    HttpClientPtr _http;
    SkylineConf _conf;

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SkylineAPI);

END_CARBON_NAMESPACE(common);

#endif
