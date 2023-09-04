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
#include "master/VIPServiceAdapter.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"
#include "master/Flag.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, VIPServiceAdapter);

#define LOG_PREFIX _id.c_str()

VIPServiceAdapter::VIPServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : ServiceAdapter(id)
{
    _serviceNodeManagerPtr = serviceNodeManagerPtr;
    _httpClientPtr.reset(new HttpClient);
}

VIPServiceAdapter::~VIPServiceAdapter() {
}

bool VIPServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_vipServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "vip service config from json fail, exception[%s]",
               e.what());
        return  false;
    }
    CARBON_PREFIX_LOG(INFO, "parse vip service config success.");
    return true;
}
bool VIPServiceAdapter::splitUrl(const string &url, string *hostSpec,
                                 string *uri)
{
    *hostSpec = "";
    *uri = "";
    
    string s = url;
    if (StringUtil::startsWith(url, "http://")) {
        s = url.substr(strlen("http://"));
    }

    string dom = s;
    size_t pos = s.find("/");
    if (pos != string::npos) {
        dom = s.substr(0, pos);
        *uri = s.substr(pos);
    }

    pos = dom.find(":");
    if (pos == string::npos) {
        dom = dom + ":80";
    }
    *hostSpec = "tcp:" + dom;
    return true;
}

bool VIPServiceAdapter::getServerList(vector<string> *servers) {
    string queryStr = "/vipserver/serverlist";
    string jmenvDomSpec = string("tcp:") + _vipServiceConfig.jmenvDom;

    if (_vipServiceConfig.jmenvDom == "") {
        if (!splitUrl(_vipServiceConfig.jmenvUrl,
                      &jmenvDomSpec, &queryStr))
        {
            CARBON_LOG(ERROR, "split url failed. url:[%s].",
                       _vipServiceConfig.jmenvUrl.c_str());
            return false;
        }
    }

    QueryResult queryResult;
    _httpClientPtr->get(jmenvDomSpec,
                        queryStr, queryResult);
    if (queryResult.statusCode != 200) {
        CARBON_LOG(ERROR, "query server list from %s failed.",
               _vipServiceConfig.jmenvDom.c_str());
        _vipServerSpec = "";        
        return false;
    }

    *servers = StringUtil::split(queryResult.content, "\n");
    CARBON_LOG(INFO, "get vip server list, host:[%s], uri:[%s], result:[%s]",
               jmenvDomSpec.c_str(), queryStr.c_str(),
               queryResult.content.c_str());
    return true;
}

bool VIPServiceAdapter::getAvailableVIPServer() {
    vector<string> servers;
    if (!getServerList(&servers)) {
        return false;
    }

    if (servers.size() == 0) {
        CARBON_LOG(ERROR, "vipserver ip list is empty.");
        return false;
    }
    
    QueryResult queryResult;

    srand(time(NULL));
    size_t start = (size_t)(rand()) % servers.size();
    string queryStr = "/vipserver/api/ip4Dom?dom=" + _vipServiceConfig.domain;
    for (size_t i = 0; i < servers.size(); i++) {
        size_t idx = (start + i) % servers.size();
        const string &server = servers[idx];
        string spec = server;
        if (server.find(":") == string::npos) {
            spec = server + ":80";
        }
        spec = string("tcp:") + spec;
        _httpClientPtr->get(spec, queryStr, queryResult);
        if (queryResult.statusCode == 200) {
            _vipServerSpec = spec;
            CARBON_PREFIX_LOG(INFO, "find availabel server [%s].", server.c_str());
            return true;
        } else {
            string errorMsg = "query server [" + server + "] is valid failed.";
            if (queryResult.statusCode == 400) {
                errorMsg = errorMsg + queryResult.content;
            }
            CARBON_PREFIX_LOG(WARN, "%s ret code:%d.", errorMsg.c_str(),
                   queryResult.statusCode);
        }
    }
    return false;
}

bool VIPServiceAdapter::doQuery(const std::string &uri, const std::string& qs, bool get, std::string &content) {
    if (_vipServerSpec == "") {
        if (!getAvailableVIPServer()) {
            CARBON_PREFIX_LOG(ERROR, "can not get any available vip server from %s.",
                   _vipServiceConfig.jmenvDom.c_str());
            return false;
        }
    }
    QueryResult queryResult;
    //TODO: URL encode
    if (get) {
        std::string queryStr = uri + "?" + qs;
        _httpClientPtr->get(_vipServerSpec, queryStr, queryResult);
    } else {
        _httpClientPtr->post(_vipServerSpec, uri, qs, queryResult);
    }
    if (queryResult.statusCode != 200) {
        CARBON_PREFIX_LOG(ERROR, "query result from %s failed.", _vipServerSpec.c_str());
        _vipServerSpec = "";        
        return false;
    }
    content = queryResult.content;
    return true;
}

nodespec_t VIPServiceAdapter::getServiceNodeSpec(
        const ServiceNode& node) const
{
    nodespec_t nodeSpec = node.ip + ":" + StringUtil::toString(
            _vipServiceConfig.port);
    return nodeSpec;
}

bool VIPServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        CARBON_PREFIX_LOG(DEBUG, "do not need add nodes.");
        return true;
    }
    
    string ipListStr;
    ServiceNodeMap::const_iterator it = nodes.begin();
    for (; it != nodes.end(); it++) {
        const ServiceNode &node = it->second;
        string ipStr = node.ip + ":" + StringUtil::toString(_vipServiceConfig.port);
        ipStr += "_" + StringUtil::toString(_vipServiceConfig.weight);        
        if (!master::Flag::isTestPublish()) {
            ipStr += "_false"; // make node in vipserver unavailable at first
        } else {
            ipStr += "_true";
        }
        if (ipListStr == "") {
            ipListStr = ipStr;
        } else {
            ipListStr += "," +  ipStr;
        }
    }
    string uri = "/vipserver/api/addIP4Dom";
    string queryStr = "dom=" + _vipServiceConfig.domain
                      + "&token=" + _vipServiceConfig.token
                      + "&ipList=" + ipListStr;
    string content;
    if (!doQuery(uri, queryStr, false, content)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes failed, query:%s.",
               queryStr.c_str());
        return false;
    }
    CARBON_PREFIX_LOG(INFO, "vipserver add nodes, query:%s, response:%s.",
            queryStr.c_str(), content.c_str());
    if (content != "ok") {
        return false;
    }
    if (!_serviceNodeManagerPtr->addNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to ServiceNodeManager failed.");
        return false;
    }
    return true;
}

bool VIPServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        CARBON_PREFIX_LOG(DEBUG, "do not need del nodes.");
        return true;
    }
    string ipListStr;
    ServiceNodeMap::const_iterator it = nodes.begin();
    for (; it != nodes.end(); it++) {
        const ServiceNode &node = it->second;
        string ipStr = node.ip + ":" + StringUtil::toString(
                _vipServiceConfig.port);
        if (ipListStr == "") {
            ipListStr = ipStr;
        } else {
            ipListStr += "," +  ipStr;
        }
    }
    string uri = "/vipserver/api/remvIP4Dom";
    string queryStr = "dom=" + _vipServiceConfig.domain
                      + "&token=" + _vipServiceConfig.token
                      + "&ipList=" + ipListStr;
    string content;
    if (!doQuery(uri, queryStr, false, content)) {
        return false;
    }
    if (content != "ok") {
        CARBON_PREFIX_LOG(ERROR, "del nodes failed, query:%s, response:%s.",
               queryStr.c_str(), content.c_str());
        return false;
    }
    if (!_serviceNodeManagerPtr->delNodes(nodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to ServiceNodeManager failed.");
        return false;
    }
    return true;
}

bool VIPServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    string uri = "/vipserver/api/ip4Dom";
    string queryStr = "dom=" + _vipServiceConfig.domain;
    string content;
    if (!doQuery(uri, queryStr, true, content)) {
        return false;
    }
    if (StringUtil::startsWith(content, "caused:")) {
        CARBON_PREFIX_LOG(ERROR, "get nodes from vip server failed.%s", content.c_str());
        return false;
    }
    map<string, vector<IPInfo> > ipInfos;
    try {
        FromJsonString(ipInfos, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "parse ip infos from content failed, content[%s], "
               "exception[%s]", content.c_str(), e.what());
        return  false;
    }
    if (ipInfos.find("ips") == ipInfos.end()) {
        CARBON_PREFIX_LOG(ERROR, "content do not has the key ips.");
        return  false;
    }

    if (ipInfos["ips"].size() == 0) {
        CARBON_PREFIX_LOG(WARN, "ip list on domain[%s] is empty.",
               _vipServiceConfig.domain.c_str());
    }
    
    for (size_t i = 0; i < ipInfos["ips"].size(); i++) {
        IPInfo &ipInfo = ipInfos["ips"][i];
        ServiceNode node;
        node.ip = ipInfo.ip;
        if (ipInfo.valid) {
            node.status = SN_AVAILABLE;
        } else {
            node.status = SN_UNAVAILABLE;            
        }
        string spec = ipInfo.ip + ":" + StringUtil::toString(ipInfo.port);
        (*nodes)[spec] = node;
    }
    filterNodes(nodes);
    CARBON_PREFIX_LOG(DEBUG, "get vip nodes:[%s].", ToJsonString(*nodes).c_str());
    return true;
}

void VIPServiceAdapter::filterNodes(ServiceNodeMap *nodes) {
    ServiceNodeMap remote = *nodes;
    const ServiceNodeMap &local = _serviceNodeManagerPtr->getNodes();
    nodes->clear();
    intersection(local, remote, nodes);
}

bool VIPServiceAdapter::recover() {
    if (_serviceNodeManagerPtr == NULL) {
        CARBON_LOG(ERROR, "recover service adapter failed, "
                   "cause ServiceNodeManager is NULL, adapterId: %s",
                   _id.c_str());
        return false;
    }

    return _serviceNodeManagerPtr->recover();
}

#undef LOG_PREFIX

END_CARBON_NAMESPACE(master);
