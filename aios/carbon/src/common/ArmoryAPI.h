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
#ifndef CARBON_ARMORY_API_H
#define CARBON_ARMORY_API_H

#include "common/common.h"
#include "common/HttpUtil.h"
#include "carbon/Log.h"
#include "master/HttpClient.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(common);

using namespace master;

struct ArmoryConf
{
    std::string host;
    std::string user;
    std::string userKey;
    std::string owner;
};

JSONIZABLE_CLASS(IPItem)
{
public:
    IPItem() : id(0) {}
    IPItem(const std::string& ip, uint64_t _id) : dns_ip(ip), id(_id) {}
    JSONIZE() {
        JSON_FIELD(dns_ip);
        json.Jsonize(".id", id, (uint64_t)0);
    }

    std::string dns_ip;
    uint64_t id = 0;
};

template <typename T>
struct ArmoryResult : public autil::legacy::Jsonizable
{
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(result);
        JSON_FIELD(num);
    }

    std::vector<T> result; 
    int num = 0;
};

template <typename T>
struct ArmoryResult2 : public autil::legacy::Jsonizable
{
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(success);
        JSON_FIELD(data);
    }

    T data = T();
    bool success = false;
};

template <> struct ArmoryResult2<void> : public autil::legacy::Jsonizable
{
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(success);
    }

    bool success = false;
};

class ArmoryAPI
{
public:
    ArmoryAPI(HttpClientPtr http, const ArmoryConf& conf) : _http(http), _conf(conf) {
        _conf.host = HttpUtil::fixHostSpec(_conf.host);
    }

    MOCKABLE ~ArmoryAPI() {}
    MOCKABLE bool getGroupIPList(const std::string& group, ArmoryResult<IPItem>& result);
    MOCKABLE bool getIPIDs(const std::vector<std::string>& ips, ArmoryResult<IPItem>& result);
    MOCKABLE bool moveHostGroup(const std::vector<uint64_t>& ids, const std::string& group, bool offline);
    MOCKABLE bool moveHostGroup(const std::vector<std::string>& ips, const std::string& group, bool offline);
    // appUseType: PUBLISH, PRE_PUBLISH
    MOCKABLE bool updateHostInfo(uint64_t id, const std::string& appUseType);

TESTABLE_BEGIN(private)
    ArmoryAPI() {}
    void setHttpClient(HttpClientPtr http) { _http = http; }
TESTABLE_END()

private:
    bool getGroupIPListStep(const std::string& group, int start, ArmoryResult<IPItem>& result);
    bool doGetIPIDs(const std::vector<std::string>& ips, ArmoryResult<IPItem>& result);
    bool doMoveHostGroup(const std::vector<uint64_t>& ids, const std::string& group, bool offline);
    std::string createMd5Key(const std::string& user, const std::string& ukey);
    QueryResult callArmory(const std::string& api, const StringKVS& args);

private:
    HttpClientPtr _http;
    ArmoryConf _conf;

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ArmoryAPI);

END_CARBON_NAMESPACE(common);

#endif // CARBON_ARMORY_API_H
