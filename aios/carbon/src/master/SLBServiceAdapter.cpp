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
#include "master/SLBServiceAdapter.h"
#include "autil/legacy/md5.h"
#include "autil/legacy/base64.h"
#include <common/HttpUtil.h>
#include <time.h>

using namespace std;
using namespace autil::legacy;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, SLBServiceAdapter);

#define GET_RS_LIST "query_loadbalancer_info"
#define GET_RS_HEALTH "query_lb_healthcheck"
#define ADD_RS "add_lb_rs"
#define DEL_RS "delete_lb_rs"

#define LOG_PREFIX _id.c_str()

SLBServiceAdapter::SLBServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                                     const string &id)
    : ServiceAdapter(id)
{
    _serviceNodeManagerPtr = serviceNodeManagerPtr;
    _httpClientPtr.reset(new HttpClient);
}

SLBServiceAdapter::~SLBServiceAdapter() {
}

bool SLBServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_slbServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "slb service config from json fail, exception[%s]",
               e.what());
        return  false;
    }
    return true;
}

bool SLBServiceAdapter::fillRsServer(const ServiceNode &node, RealServer &rs) {
    rs.rsIp = node.ip;
    rs.port = _slbServiceConfig.rsPort;
    rs.rsType = _slbServiceConfig.rsType;
    return true;
}

bool SLBServiceAdapter::doOperateNodes(const string &action, const ServiceNodeMap &nodes) {
    map<string, string> args;
    args["region_no"] = _slbServiceConfig.region;
    args["lb_id"] = _slbServiceConfig.lbid;
    args["aliyun_idkp"] = _slbServiceConfig.idkp;


    vector<RealServer> rsList;
    for (auto node : nodes) {
        RealServer rs;
        if (!fillRsServer(node.second, rs)) {
            continue;
        }
        rsList.push_back(rs);
    }

    if (!rsList.empty()) {
        args["rs_list"] = ToJsonString(rsList, true);
        Response response;
        if (!call(action, args, &response)) {
            CARBON_PREFIX_LOG(ERROR, "add rs node failed.");
            return false;
        }
    }
    
    return true;
}

bool SLBServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    ServiceNodeMap tmpNodes = truncateNodes(nodes);

    if (!doOperateNodes(ADD_RS, tmpNodes)) {
        return false;
    }

    if (!_serviceNodeManagerPtr->addNodes(tmpNodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to SerivceNodeManager failed.");
        return false;
    }

    return true;
}
   
bool SLBServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    ServiceNodeMap tmpNodes = truncateNodes(nodes);

    if (!doOperateNodes(DEL_RS, tmpNodes)) {
        return false;
    }

    if (!_serviceNodeManagerPtr->delNodes(tmpNodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes to SerivceNodeManager failed.");
        return false;
    }
    return true;
}

bool SLBServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    map<string, string> args;
    args["region_no"] = _slbServiceConfig.region;
    args["lb_id"] = _slbServiceConfig.lbid;
    args["aliyun_idkp"] = _slbServiceConfig.idkp;
    args["bid"] = _slbServiceConfig.bid;
    
    GetRsListResponse response;
    if (!call(GET_RS_LIST, args, &response)) {
        CARBON_PREFIX_LOG(ERROR, "get rs list failed.");
        return false;
    }

    const vector<RealServer> &rsList = response.data.rsList;

    GetRsHealthResponse rsHealthResponse;
    if (!call(GET_RS_HEALTH, args, &rsHealthResponse)) {
        CARBON_PREFIX_LOG(ERROR, "get rs health failed.");
        return false;
    }

    map<string, string> rsStatusMap;
    for (auto vipStatus : rsHealthResponse.data.vipStatusList) {
        if (vipStatus.frontendPort == _slbServiceConfig.frontendPort) {
            for (auto rsStatus : vipStatus.rsStatusList) {
                rsStatusMap[rsStatus.rsIp] = rsStatus.status;
            }
        }
    }

    const ServiceNodeMap &localNodes = _serviceNodeManagerPtr->getNodes();
    for (auto rs : rsList) {
        ServiceNode node;
        node.ip = rs.rsIp;
        node.status = SN_UNAVAILABLE;

        if (localNodes.find(rs.rsIp) == localNodes.end()) {
            continue;
        }

        map<string, string>::iterator it = rsStatusMap.find(rs.rsIp);
        if (it == rsStatusMap.end()) {
            (*nodes)[node.ip] = node;
            continue;
        }

        const string &rsStatus = it->second;
        if (rsStatus == "normal" || rsStatus == "notavailable") {
            node.status = SN_AVAILABLE;
        }
        (*nodes)[node.ip] = node;
    }
    
    return true;
}

bool SLBServiceAdapter::call(const string &action,
                             const map<string, string> &args,
                             Response *response)
{
    map<string, string> params = args;
    params["action"] = action;
    params["session"] = _slbServiceConfig.accessId;
    params["timestamp"] = formatTimestamp();
    params["aliyun_idkp"] = _slbServiceConfig.idkp;
    params["sign"] = signature(params);

    string paramData = common::HttpUtil::encodeParams(params);
    string url = _slbServiceConfig.apiUrl + "?" + paramData;
                 
    QueryResult queryResult;
    _httpClientPtr->get(url, queryResult);
    if (queryResult.statusCode != 200 && queryResult.content.empty()) {
        CARBON_PREFIX_LOG(ERROR, "call slb server failed.");
        return false;
    }

    try {
        FromJsonString(*response, queryResult.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "parse query result failed.");
        return false;
    }

    if (response->code != 200) {
        CARBON_PREFIX_LOG(ERROR, "call slb server failed, error:%s",
                          response->msg.c_str());
        return false;
    }
    
    return true;
}

string SLBServiceAdapter::formatTimestamp() {
    time_t rawtime;
    struct tm *timeinfo;
    char buffer [256];
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(buffer, 256, "%Y-%m-%d %H:%M:%S", timeinfo);
    
    return string(buffer);
}

string SLBServiceAdapter::signature(const map<string, string> &params) {
    string paramData;
    for (map<string, string>::const_iterator it = params.begin();
         it != params.end(); it++)
    {
        const string &k = it->first;
        const string &v = it->second;
        if (paramData.empty()) {
            paramData = (k + "=" + v);
        } else {
            paramData += ("&" + k + "=" + v);
        }
    }

    paramData += _slbServiceConfig.accessKey;
    
    char buf[16];
    DoMd5((uint8_t*)paramData.c_str(), paramData.size(), (uint8_t*)buf);
    istringstream is(string(buf, 16));
    ostringstream os;
    Base64Encoding(is, os);
    return os.str();
}

ServiceNodeMap SLBServiceAdapter::truncateNodes(const ServiceNodeMap &nodes) {
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

bool SLBServiceAdapter::recover() {
    if (_serviceNodeManagerPtr == NULL) {
        CARBON_LOG(ERROR, "recover service adapter failed, "
                   "cause ServiceNodeManager is NULL, adapterId: %s",
                   _id.c_str());
        return false;
    }

    return _serviceNodeManagerPtr->recover();
}


END_CARBON_NAMESPACE(master);

