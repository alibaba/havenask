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
#include "ArmoryAPI.h"
#include "autil/legacy/md5.h"
#include "autil/legacy/string_tools.h"
#include "autil/TimeUtility.h"
#include "autil/StringUtil.h"

BEGIN_CARBON_NAMESPACE(common);

#define RESULT_FAILED(result) (result.statusCode != 200 || result.content.empty())
#define IP_NUM_IN_GROUP 20000
#define _S0(n) #n
#define _S(n)_S0(n)
#define IP_NUM_IN_GROUP_STR _S(IP_NUM_IN_GROUP)
#define ARMORY_UNIVERSAL_SEARCH_API "/page/api/free/opsfreeInterface/search.htm"
#define ARMORY_GET_GROUPINFO_API "/page/api/armorySearch/getAppGroupInfoByAppGroupName.htm"
#define ARMORY_CREATE_GROUP_API "/page/api/appGroupInterface/createAppGroup.htm"
#define ARMORY_BATCH_UPDATE_API "/page/api/free/node/batchUpdate.htm"
#define ARMORY_UPDATE_DEVICE_INFO_API "/page/api/armoryUpdate/updateNode.htm"

#define RET_VOID
#define PARSE_JSON_RESP(dst, content, ret) \
    try { \
        FromJsonString(dst, content); \
    } catch (const autil::legacy::ExceptionBase &e) { \
        CARBON_LOG(ERROR, "parse armory json failed: api [%s], resp [%s], err [%s]", api, content.c_str(), e.what()); \
        return ret; \
    }
#define CHECK_CALL_ARMORY(args, ret) \
    ret = callArmory(api, args); \
    if (RESULT_FAILED(ret)) { \
        CARBON_LOG(WARN, "call armory failed, api [%s], resp [%d:%s], debug [%s]", \
                api, ret.statusCode, ret.content.c_str(), ret.debugInfo.c_str()); \
        return false; \
    }
#define VERIFY_RESULT(result, content) \
    if (!result.success) { \
        CARBON_LOG(WARN, "armory response failed: [%s]", content.c_str()); \
    }

using namespace std;
using namespace autil;
using namespace autil::legacy;

CARBON_LOG_SETUP(common, ArmoryAPI);

namespace {

JSONIZABLE_CLASS(UpdateItem)
{
public:
    UpdateItem(uint64_t id, const std::string& group, bool offline) {
        this->id = StringUtil::toString(id);
        setGroup(group, offline);
    }

    JSONIZE() {
        JSON_FIELD(id);
        JSON_FIELD(value);
    }
    
    void setGroup(const std::string& group, bool offline) {
        value["nodegroup"] = group;
        value["state"] = offline ? "working_offline" : "working_online";
    }

    std::string id;
    StringKVS value;    
};

JSONIZABLE_CLASS(UpdateInfoNode)
{
public:
    JSONIZE() {
        JSON_FIELD(id);
        JSON_FIELD(nodetype);
        JSON_FIELD(valueMap);
    }

    uint64_t id;
    std::string nodetype = "device";
    StringKVS valueMap;
};

}

bool ArmoryAPI::getGroupIPList(const std::string& group, ArmoryResult<IPItem>& result) {
    ArmoryResult<IPItem> stepRet;
    int start = 0;
    do {
        stepRet.num = 0; stepRet.result.clear();
        if (start > 0) {
            CARBON_LOG(DEBUG, "get group ip list [%s] from %d", group.c_str(), start);
        }
        if (!getGroupIPListStep(group, start, stepRet)) {
            CARBON_LOG(ERROR, "get armory ip list failed for group: [%s] at %d", group.c_str(), start);
            return false;
        }
        result.result.insert(result.result.end(), stepRet.result.begin(), stepRet.result.end());
        result.num += stepRet.num;
        start += stepRet.num;
    } while (stepRet.num == IP_NUM_IN_GROUP);
    return true;
}

bool ArmoryAPI::getGroupIPListStep(const std::string& group, int start, ArmoryResult<IPItem>& result) {
    const char* api = ARMORY_UNIVERSAL_SEARCH_API;
    StringKVS args = {
        {"from", "device"},
        {"q", "nodegroup==" + group},
        {"num", IP_NUM_IN_GROUP_STR},
        {"start", autil::StringUtil::toString(start)},
    };
    QueryResult qr;
    CHECK_CALL_ARMORY(args, qr);
    PARSE_JSON_RESP(result, qr.content, false);
    return true;
}

bool ArmoryAPI::getIPIDs(const std::vector<std::string>& ips, ArmoryResult<IPItem>& result) {
    const size_t MAX_COUNT = 100;
    for (size_t i = 0; i < ips.size(); i += MAX_COUNT) {
        std::vector<std::string> tmpIps(ips.begin() + i, 
                i + MAX_COUNT < ips.size() ? ips.begin() + i + MAX_COUNT : ips.end());
        if (!doGetIPIDs(tmpIps, result)) return false;
    }
    return true;
}

bool ArmoryAPI::moveHostGroup(const std::vector<uint64_t>& ids, const std::string& group, bool offline) {
    const size_t MAX_COUNT = 500;
    for (size_t i = 0; i < ids.size(); i += MAX_COUNT) {
        std::vector<uint64_t> tmpIds(ids.begin() + i, 
                i + MAX_COUNT < ids.size() ? ids.begin() + i + MAX_COUNT : ids.end());
        if (!doMoveHostGroup(tmpIds, group, offline)) return false;
    }
    return true;
}

bool ArmoryAPI::moveHostGroup(const std::vector<std::string>& ips, const std::string& group, bool offline) {
    ArmoryResult<IPItem> result;
    if (!getIPIDs(ips, result)) return false;
    std::vector<uint64_t> ids;
    std::transform(result.result.begin(), result.result.end(), std::back_inserter(ids),
            [] (const IPItem& i) { return i.id; });
    return moveHostGroup(ids, group, offline);
}

bool ArmoryAPI::updateHostInfo(uint64_t id, const std::string& appUseType) {
    const char* api = ARMORY_UPDATE_DEVICE_INFO_API;
    UpdateInfoNode node;
    node.id = id;
    node.valueMap["appUseType"] = appUseType;
    StringKVS args = {
        {"node", ToJsonString(node)}
    };
    QueryResult qr;
    CHECK_CALL_ARMORY(args, qr);
    ArmoryResult2<void> result;
    PARSE_JSON_RESP(result, qr.content, false);
    VERIFY_RESULT(result, qr.content);
    return result.success;
}

bool ArmoryAPI::doMoveHostGroup(const std::vector<uint64_t>& ids, const std::string& group, bool offline) {
    const char* api = ARMORY_BATCH_UPDATE_API;
    std::vector<UpdateItem> nodes;
    for (const auto& id : ids) {
        nodes.push_back(UpdateItem(id, group, offline));
    }
    std::string nodeStr;
    try {
        nodeStr = ToJsonString(nodes);
    } catch (const autil::legacy::ExceptionBase &e) {
        return false;
    }
    StringKVS args = {
        {"node", nodeStr}
    };
    QueryResult qr;
    CHECK_CALL_ARMORY(args, qr);
    ArmoryResult2<void> result;
    PARSE_JSON_RESP(result, qr.content, false);
    VERIFY_RESULT(result, qr.content);
    return result.success;
}

bool ArmoryAPI::doGetIPIDs(const std::vector<std::string>& ips, ArmoryResult<IPItem>& result) {
    const char* api = ARMORY_UNIVERSAL_SEARCH_API;
    std::string iplist = StringUtil::toString(ips, ",");
    StringKVS args = {
        {"q", "dns_ip in (" + iplist + ")"},
        {"select", "id,dns_ip"},
        {"num", StringUtil::toString(ips.size())},
    };
    QueryResult qr;
    CHECK_CALL_ARMORY(args, qr);
    PARSE_JSON_RESP(result, qr.content, false);
    return true;
}

QueryResult ArmoryAPI::callArmory(const std::string& api, const StringKVS& args) {
    QueryResult queryResult;
    StringKVS kvs = args;
    kvs["_username"] = _conf.user;
    kvs["key"] = createMd5Key(_conf.user, _conf.userKey);
    _http->post(_conf.host, api, HttpUtil::encodeParams(kvs), queryResult);
    return queryResult;
}

std::string ArmoryAPI::createMd5Key(const std::string& user, const std::string& ukey) {
    std::string tm = TimeUtility::currentTimeString("%Y%m%d");
    std::string str = user + tm + ukey;
    uint8_t out[16];
    DoMd5((uint8_t*)str.c_str(), str.size(), out);
    std::string outs((char*) out, sizeof(out));   
    return ToLowerCaseString(StringToHex(outs));
}

END_CARBON_NAMESPACE(common);
