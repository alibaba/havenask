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
#include "master/VPCSLBServiceAdapter.h"
#include <common/HttpUtil.h>
#include <common/Crypto.h>
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/base64.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
USE_CARBON_NAMESPACE(common);

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, VPCSLBServiceAdapter);
CARBON_LOG_SETUP(master, Caller);
CARBON_LOG_SETUP(master, VPCCaller);
CARBON_LOG_SETUP(master, SLBCaller);
CARBON_LOG_SETUP(master, ServerIdCache);

#define SLB_ACTION_ADD_SERVERS "AddBackendServers"
#define SLB_ACTION_ADD_VGROUP_SERVERS "AddVServerGroupBackendServers"
#define SLB_ACTION_DEL_SERVERS "RemoveBackendServers"
#define SLB_ACTION_DEL_VGROUP_SERVERS "RemoveVServerGroupBackendServers"
#define SLB_ACTION_GET_SERVERS "DescribeHealthStatus"
#define SLB_API_VERSION "2014-05-15"
#define SLB_SIG_VERSION "1.0"
#define SLB_SIG_METHOD "HMAC-SHA1"

#define ECS_ACTION_GET_ENI "DescribeNetworkInterfaces"
#define ECS_API_VERSION "2014-05-26"

#define MAX_OP_SERVER_COUNT 20

#define LOG_PREFIX _id.c_str()

string Caller::HTTP_METHOD_GET("GET");
string Caller::HTTP_METHOD_POST("POST");

Caller::Caller(const HttpClientPtr &httpClientPtr, const std::string &httpMethod)
    : _httpClientPtr(httpClientPtr)
    , _httpMethod(httpMethod)
{}

bool Caller::call(const ParamMap &params, VPCResponse *response) {
    ParamMap tmpParams = params;
    if (!extendParams(tmpParams)) {
        return false;
    }
    string paramData = common::HttpUtil::encodeParams(tmpParams);
    string url = "http://" + getAddress() + "/?" + paramData;

    QueryResult queryResult;
    _httpClientPtr->get(url, queryResult);
    if (queryResult.statusCode != 200 && queryResult.content.empty()) {
        CARBON_LOG(ERROR, "call slb or ecs service failed. errorCode: %d, query: %s",
                   queryResult.statusCode, url.c_str());
        return false;
    }
    
    try {
        FromJsonString(*response, queryResult.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "parse query result failed. error:%s", e.what());
        return false;
    }
    return !hasError(response);
}

bool VPCCaller::hasError(const VPCResponse *response) const {
    if (response && !response->code.empty()) {
        CARBON_LOG(ERROR, "%s", ToJsonString(response).c_str());
        return true;
    }
    return false;
}

bool VPCCaller::extendParams(ParamMap &params) const {
    params["Timestamp"] = formatTimestamp();
    params["AccessKeyId"] = _config.accessKeyId;
    params["SignatureMethod"] = SLB_SIG_METHOD;
    params["SignatureNonce"] = autil::StringUtil::toString(TimeUtility::currentTime());
    params["Version"] = getApiVersion();
    params["Format"] = "json";
    params["SignatureVersion"] = SLB_SIG_VERSION;
    params["Signature"] = signature("GET", params);
    return true;
}

string VPCCaller::formatTimestamp() const {
    time_t rawtime;
    struct tm *timeinfo;
    char buffer [256];
    time(&rawtime);
    timeinfo = gmtime(&rawtime);
    strftime(buffer, 256, "%Y-%m-%dT%H:%M:%SZ", timeinfo);
    
    return string(buffer);
}

string VPCCaller::signature(const string &httpMethod, const ParamMap &params) const {
    string signIn = HttpUtil::encodeParams(params);
    signIn = HttpUtil::urlEncode(signIn);
    if (!httpMethod.empty()) {
        signIn = httpMethod + "&%2F&" + signIn;
    }
    string secretKey = _config.accessKeySecret + "&";
    char signOut[256];
    size_t signOutLenLen = 256;
    Crypto::hmacsha1((const uint8_t*)secretKey.c_str(),
                     secretKey.length(),
                     (const uint8_t*)signIn.c_str(),
                     signIn.length(),
                     (uint8_t*)signOut, &signOutLenLen);
    return Base64EncodeFast(string(signOut, signOutLenLen));
}

string SLBCaller::getAddress() const {
    return _config.slbApiEndpoint;
}

string SLBCaller::getApiVersion() const {
    return SLB_API_VERSION;
}

bool SLBCaller::hasError(const VPCResponse *response) const {
    const SLBResponse *slbResponse = dynamic_cast<const SLBResponse*>(response);
    if (slbResponse && !slbResponse->code.empty()) {
        CARBON_LOG(ERROR, "%s", ToJsonString(response).c_str());
        return true;
    }
    return false;
}

string ECSCaller::getAddress() const {
    return _config.ecsApiEndpoint;
}

string ECSCaller::getApiVersion() const {
    return ECS_API_VERSION;
}

VPCSLBServiceAdapter::VPCSLBServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : ServiceAdapter(id)
    , _httpClientPtr(new HttpClient)
    , _serviceNodeManagerPtr(serviceNodeManagerPtr)
{
}

VPCSLBServiceAdapter::~VPCSLBServiceAdapter() {
}

bool VPCSLBServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_vpcSlbServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "vpc slb service config from json fail, exception[%s]",
               e.what());
        return  false;
    }
    if (!_vpcSlbServiceConfig.vServerGroupId.empty() && _vpcSlbServiceConfig.listenerPort.empty()) {
        CARBON_PREFIX_LOG(ERROR, "listenerPort must be set when use vServerGroup mode");
        return false;
    }
    if (_vpcSlbServiceConfig.servicePort.empty()) {
        _vpcSlbServiceConfig.servicePort = _vpcSlbServiceConfig.listenerPort;
    }
    _serverIdCachePtr.reset(new ServerIdCache(_httpClientPtr, _vpcSlbServiceConfig));
    _slbCallerPtr.reset(new SLBCaller(_httpClientPtr, _vpcSlbServiceConfig));
    return true;
}

bool VPCSLBServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    ServiceNodeMap tmpNodes = truncateNodes(nodes);
    map<string, string> args;

    if (!_vpcSlbServiceConfig.vServerGroupId.empty()) {
        args["Action"] = SLB_ACTION_ADD_VGROUP_SERVERS;
        args["VServerGroupId"] = _vpcSlbServiceConfig.vServerGroupId;
    } else {
        args["Action"] = SLB_ACTION_ADD_SERVERS;
        args["LoadBalancerId"] = _vpcSlbServiceConfig.lbId;
    }
    args["BackendServers"] = formatBackendServers(tmpNodes);
    args["RegionId"] = _vpcSlbServiceConfig.regionId;

    AddBackendServerResponse response;
    if (!_slbCallerPtr->call(args, &response)) {
        return false;
    }

    if (!_serviceNodeManagerPtr->addNodes(tmpNodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to SerivceNodeManager failed.");
        return false;
    }
    
    return true;
}

string VPCSLBServiceAdapter::formatBackendServers(const ServiceNodeMap &nodes) {
    vector<string> serverIds = getServerIds(nodes);
    vector<SLBBackendServer> servers;
    for (size_t i = 0; i < serverIds.size(); i++) {
        SLBBackendServer server;
        server.serverId = serverIds[i];
        server.type = _vpcSlbServiceConfig.serverType;
        server.port = _vpcSlbServiceConfig.servicePort;
        servers.push_back(server);
    }
    return ToJsonString(servers);
}    

vector<string> VPCSLBServiceAdapter::getServerIds(const ServiceNodeMap &nodes) {
    vector<string> servers;
    for (ServiceNodeMap::const_iterator it = nodes.begin();
         it != nodes.end(); it++)
    {
        string serverId = _serverIdCachePtr->getServerId(it->second.ip);
        if (serverId.empty()) {
            continue;
        }
        servers.push_back(serverId);
    }
    return servers;
}

bool VPCSLBServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    ServiceNodeMap tmpNodes = truncateNodes(nodes);
    
    map<string, string> args;
    if (!_vpcSlbServiceConfig.vServerGroupId.empty()) {
        args["Action"] = SLB_ACTION_DEL_VGROUP_SERVERS;
        args["VServerGroupId"] = _vpcSlbServiceConfig.vServerGroupId;
    } else {
        args["Action"] = SLB_ACTION_DEL_SERVERS;
        args["LoadBalancerId"] = _vpcSlbServiceConfig.lbId;
    }
    args["BackendServers"] = formatBackendServers(tmpNodes);
    args["RegionId"] = _vpcSlbServiceConfig.regionId;
    
    DelBackendServerResponse response;
    bool ret = _slbCallerPtr->call(args, &response);
    CARBON_LOG(INFO, "del slb servers request: %s, requestId: %s",
               ToJsonString(args).c_str(), response.requestId.c_str());
    if (!ret) {
        return false;
    }

    if (!_serviceNodeManagerPtr->delNodes(tmpNodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes to SerivceNodeManager failed.");
        return false;
    }

    return true;
}

bool VPCSLBServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    ParamMap args;
    args["Action"] = SLB_ACTION_GET_SERVERS;
    args["LoadBalancerId"] = _vpcSlbServiceConfig.lbId;
    args["RegionId"] = _vpcSlbServiceConfig.regionId;
    if (!_vpcSlbServiceConfig.listenerPort.empty()) {
        args["ListenerPort"] = _vpcSlbServiceConfig.listenerPort;
    }

    GetBackendServerResponse response;
    if (!_slbCallerPtr->call(args, &response)) {
        CARBON_LOG(ERROR, "get lb's backend servers failed. lb:[%s]",
                   _vpcSlbServiceConfig.lbId.c_str());
        return false;
    }

    const ServiceNodeMap &localNodes = _serviceNodeManagerPtr->getNodes();
    const vector<SLBBackendServerStatus> &servers = response.backendServers.servers;
    for (size_t i = 0; i < servers.size(); i++) {
        const auto &server = servers[i];
        const auto &serverId = server.serverId;
        string ip = _serverIdCachePtr->getIp(serverId);
        if (ip.empty()) {
            continue;
        }
        
        ServiceNode node;
        node.ip = ip;
        node.status = SN_UNAVAILABLE;
        if (server.healthStatus == "normal") {
            node.status = SN_AVAILABLE;
        }

        if (localNodes.find(node.ip) == localNodes.end()) {
            continue;
        }
        
        (*nodes)[node.ip] = node;
    }
    
    return true;
}

bool VPCSLBServiceAdapter::recover() {
    if (_serviceNodeManagerPtr == NULL) {
        CARBON_LOG(ERROR, "recover service adapter failed, "
                   "cause ServiceNodeManager is NULL, adapterId: %s",
                   _id.c_str());
        return false;
    }

    return _serviceNodeManagerPtr->recover();
}

ServiceNodeMap VPCSLBServiceAdapter::truncateNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() > 20) {
        ServiceNodeMap tmpNodes;
        size_t i = 0;
        for (ServiceNodeMap::const_iterator it = nodes.begin();
             it != nodes.end() && i < 20; it++, i++)
        {
            tmpNodes.insert({it->first, it->second});
        }

        CARBON_PREFIX_LOG(INFO, "node count [%zd] to be added to slb "
                          "is exceed the limit 20, truncate the nodes.",
                          nodes.size());
        return tmpNodes;
    } 

    return nodes;
}

ServerIdCache::ServerIdCache(const HttpClientPtr &httpClientPtr,
                             const VPCSLBServiceConfig &config)
    : _ecsCaller(httpClientPtr, config)
    , _config(config)
{
}

#define DO_GET(cache, key)                      \
    string v = doGet(cache, key);               \
    if (v.empty()) {                            \
        updateCache();                          \
    }                                           \
    return doGet(cache, key);

string ServerIdCache::getServerId(const string &ip) {
    DO_GET(_ip2ServerIdMap, ip);
}

string ServerIdCache::getIp(const string &serverId) {
    DO_GET(_serverId2IpMap, serverId);
}

#undef DO_GET

string ServerIdCache::doGet(const map<string, string> &cache, const string &key) {
    map<string, string>::const_iterator it = cache.find(key);
    if (it == cache.end()) {
        return "";
    }
    return it->second;
}

void ServerIdCache::updateCache() {
    map<string, string> ip2enis = getAllEnis();
    setCache(ip2enis);
}

void ServerIdCache::setCache(const map<string, string> &ip2enis) {
    _ip2ServerIdMap = ip2enis;
    _serverId2IpMap.clear();
    for (map<string, string>::iterator it = _ip2ServerIdMap.begin();
         it != _ip2ServerIdMap.end(); it++)
    {
        _serverId2IpMap[it->second] = it->first;
    }
}

map<string, string> ServerIdCache::getAllEnis() {
    ParamMap params;
    params["Action"] = ECS_ACTION_GET_ENI;
    params["RegionId"] = _config.regionId;
    params["VpcId"] = _config.vpcId;
    if (!_config.vswitchId.empty()) {
        params["VSwitchId"] = _config.vswitchId;
    }
    if (!_config.resourceGroupId.empty()) {
        params["ResourceGroupId"] = _config.resourceGroupId;
    }

    map<string, string> ip2enis;
    int count = 0;
    for (int pageNum = 1;; pageNum++) {
        params["PageNumber"] = autil::StringUtil::toString(pageNum);
        params["PageSize"] = "100";
        GetEniResponse response;
        if (!_ecsCaller.call(params, &response)) {
            CARBON_LOG(ERROR, "get eni infos failed.");
            break;
        }
        const auto &nifs = response.nifSets.nifs;
        for (size_t i = 0; i < nifs.size(); i++) {
            ip2enis[nifs[i].ip] = nifs[i].nifId;
        }
        count += nifs.size();
        if (count >= response.totalCount) {
            break;
        }
    }
    return ip2enis;
}

END_CARBON_NAMESPACE(master);

