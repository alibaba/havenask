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
#include "master/LVSServiceAdapter.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include <time.h>
#include <stdlib.h>
#include <KeyCenterClient.h>
#include "common/HttpUtil.h"
#include "autil/legacy/base64.h"

#define UPDATE_VIP "updateVipNoArmory"
#define GET_RS_OF_VS "getRsOfVs"
#define GET_RS_STATUS "getRsStatus"

#define VIP_OP_ADD "ADD"
#define VIP_OP_DEL "DELETE"
#define VIP_OP_UPDATE "UPDATE"

#define VIP_STATUS_ENABLE "enable"
#define VIP_STATUS_DISABLE "disable"

// 5 sec
#define LVS_SYNC_INTERVAL (5 * 1000 * 1000) 

using namespace std;
using namespace autil;

BEGIN_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(master, LVSServiceAdapter);

#define LOG_PREFIX _id.c_str()

class KeyCenterInitializer {
public:
    KeyCenterInitializer() {
        cerr << "Initialize key center client" << endl;
        KeyCenter::Initialize();
    }
};

KeyCenterInitializer gKeyCenter;

LVSServiceAdapter::LVSServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : ServiceAdapter(id)
{
    _serviceNodeManagerPtr = serviceNodeManagerPtr;
    _httpClientPtr.reset(new HttpClient);
    _signContext = NULL;
    _lastSyncTime = 0;
}


LVSServiceAdapter::~LVSServiceAdapter() {
    if (_signContext) {
        free(_signContext);
    }
}

bool LVSServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_lvsServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "lvs service config from json fail, exception[%s]",
               e.what());
        return  false;
    }

    if (_lvsServiceConfig.token == "") {
        int ret = KeyCenter::Initialize(_lvsServiceConfig.publishNum.c_str(),
                _lvsServiceConfig.appName.c_str(),
                _lvsServiceConfig.keyCenterUrl.c_str(),
                "/tmp/", false, &_signContext);
        if (ret != 0) {
            CARBON_PREFIX_LOG(ERROR, "init keycenter client failed.");
            return false;
        }
    }
        
    CARBON_PREFIX_LOG(INFO, "parse lvs service config success.");
    return true;
}

nodespec_t LVSServiceAdapter::getServiceNodeSpec(const ServiceNode &node) const {
    return node.ip;
}

bool LVSServiceAdapter::doAddNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        return true;
    }
    ServiceNodeMap tmpNodes = truncateNodes(nodes);
    auto filterNodes = filterAddNodes(tmpNodes);
  
    if (!updateVip(filterNodes, VIP_OP_ADD, VIP_STATUS_ENABLE)) {
        return false;
    }

    if (!_serviceNodeManagerPtr->addNodes(tmpNodes)) {
        CARBON_PREFIX_LOG(ERROR, "add nodes to SerivceNodeManager failed.");
        return false;
    }
    
    return true;
}

bool LVSServiceAdapter::doDelNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() == 0) {
        return true;
    }

    ServiceNodeMap toDisableNodes, toDelNodes;
    filterNodes(nodes, &toDisableNodes, &toDelNodes);
    toDisableNodes = truncateNodes(toDisableNodes);
    toDelNodes = truncateNodes(toDelNodes);

    if (!toDisableNodes.empty() &&
        !updateVip(toDisableNodes, VIP_OP_UPDATE, VIP_STATUS_DISABLE))
    {
        return false;
    }

    if (!toDelNodes.empty() &&
        !updateVip(toDelNodes, VIP_OP_DEL, VIP_STATUS_DISABLE))
    {
        return false;
    }

    if (!toDelNodes.empty() && !_serviceNodeManagerPtr->delNodes(toDelNodes)) {
        CARBON_PREFIX_LOG(ERROR, "del nodes from ServiceNodeManager failed.");
        return false;
    }

    return true;
}
    
bool LVSServiceAdapter::doGetNodes(ServiceNodeMap *nodes) {
    GetRsRequest request;
    request.sysName = _lvsServiceConfig.sysName;
    request.ip = _lvsServiceConfig.vipIp;
    request.port = _lvsServiceConfig.vipPort;
    
    GetRsResponse response;
    if (!call(GET_RS_OF_VS, &request, &response)) {
        return false;
    }

    GetRsStatusResponse rsStatusResponse;
    if (!call(GET_RS_STATUS, &request, &rsStatusResponse)) {
        return false;
    }

    map<string, RsStatus> rsStatusMap;
    for (size_t i = 0; i < rsStatusResponse.data.rs.size(); i++) {
        const RsStatus &rsStatus = rsStatusResponse.data.rs[i];
        rsStatusMap[rsStatus.rsIp] = rsStatus;
    }

    const ServiceNodeMap &localNodes = _serviceNodeManagerPtr->getNodes();
    for (auto rsInfo : response.data) {
        const string &rsIp = rsInfo.rsIp;
        if (localNodes.find(rsIp) == localNodes.end()) {
            continue;
        }
        
        ServiceNode node;
        node.ip = rsIp;
        node.status = SN_UNAVAILABLE;
        
        auto it = rsStatusMap.find(rsIp);
        if (it != rsStatusMap.end()) {
            if (rsInfo.status == "enable" && it->second.hc == "UP") {
                node.status = SN_AVAILABLE;
            } else if (rsInfo.status == "disable") {
                node.status = SN_DISABLED;
            }
        }
        (*nodes)[node.ip] = node;
    }
    return true;
}

bool LVSServiceAdapter::updateVip(const ServiceNodeMap &nodes,
                                  const string &op,
                                  const string &status)
{
    if (nodes.empty()){
        return true;
    }
    UpdateVipRequest request;
    request.sysName = _lvsServiceConfig.sysName;
    request.ip = _lvsServiceConfig.vipIp;
    request.port = _lvsServiceConfig.vipPort;
    request.applyUser = _lvsServiceConfig.applyUser;
    request.changeOrderId = genChangeOrderId(op);

    for (ServiceNodeMap::const_iterator it = nodes.begin();
         it != nodes.end(); it ++)
    {
        const ServiceNode &node = it->second;
        UpdateRsInfo rs;
        rs.ip = node.ip;
        rs.port = _lvsServiceConfig.appPort;
        rs.site = _lvsServiceConfig.site;
        rs.status = status;
        rs.op = op;

        request.rs.push_back(rs);
    }

    CARBON_LOG(INFO, "update vip call:[%s], rs_list:[%s].",
               request.ip.c_str(),
               autil::legacy::ToJsonString(request.rs).c_str());
    UpdateVipResponse response;
    if (!call(UPDATE_VIP, &request, &response)) {
        return false;
    }

    return true;
}

string LVSServiceAdapter::getApiUrl(const string &api) {
    string url = _lvsServiceConfig.xvipManagerUrl;
    if (url.length() > 0 && *url.rbegin() != '/') {
        url += "/";
    }
    return url + api;
}

bool LVSServiceAdapter::call(const string &api, const RequestBase *request,
                             ResponseBase *response)
{
    QueryResult result;
    string url = getApiUrl(api);
    string paramData;
    if (!getParamData(url, request, paramData)) {
        return false;
    }

    _httpClientPtr->post(url, paramData, result);
    CARBON_LOG(INFO, "call lvs api [%s], "
               "statusCode:%d, response:%s",
               url.c_str(), result.statusCode,
               result.content.c_str());
    try {
        FromJsonString(*response, result.content);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "deserialize response for api [%s] failed, "
                   "error:[%s], content:[%s]",
                   api.c_str(), e.what(), result.content.c_str());
        return false;
    }

    if (result.statusCode != 200 || response->errCode != "OK") {
        CARBON_LOG(ERROR, "call lvs api [%s] failed, "
                   "statusCode:%d, response:%s",
                   url.c_str(), result.statusCode,
                   result.content.c_str());
        return false;
    }

    return true;
}

bool LVSServiceAdapter::getParamData(const string &url,
                                     const RequestBase *request,
                                     string &paramData)
{
    map<string, string> params;
    request->toKVMap(params);
    if (_lvsServiceConfig.token != "") {
        params["token"] = _lvsServiceConfig.token;
        paramData = HttpUtil::encodeParams(params);
        return true;
    } else {
        srand(time(NULL));
        params["nonce"] = StringUtil::toString(rand());
        params["timestamp"] = StringUtil::toString(TimeUtility::currentTime() / 1000);
        string uri = extractUri(url);
        string signature;
        if (!genSignature(uri, params, signature)) {
            CARBON_LOG(ERROR, "gen signature for uri [%s] failed.", uri.c_str());
            return false;
        }

        paramData = HttpUtil::encodeParams(params);
        paramData += "&signature=" + HttpUtil::urlEncode(signature);
        return true;
    }

    return true;
}

bool LVSServiceAdapter::genSignature(const string &uri, const map<string, string> &params,
                                     string &signature)
{
    string paramData;
    for (const auto& kv : params) {
        string s = kv.first + "=" + kv.second;
        if (paramData.empty()) {
            paramData = s;
        } else {
            paramData += "&" + s;
        }
    }

    return genSignature(uri, paramData, signature);
}

bool LVSServiceAdapter::genSignature(const string &uri, const string &paramData,
                                     string &signature)
{
    string in = uri + "?" + paramData;
    size_t outLen = 0;
    unsigned char* sig = KeyCenter::sign(_lvsServiceConfig.key.c_str(),
            0, (const unsigned char*)in.c_str(), in.length(), &outLen, true, _signContext);
    if (sig == NULL) {
        CARBON_LOG(ERROR, "sign for key [%s] failed.",
                   _lvsServiceConfig.key.c_str());
        return false;
    }

    signature = string((const char*)sig, outLen);
    free(sig);

    istringstream is;
    ostringstream os;
    is.str(signature);
    legacy::Base64Encoding(is, os);
    signature = os.str();
    return true;
}

string LVSServiceAdapter::extractUri(const string &url) {
    size_t pos = 0;
    if (StringUtil::startsWith(url, "http://")) {
        pos = url.find('/', strlen("http://"));
    } else {
        pos = url.find('/');
    }
    if (pos == string::npos) {
        return "/";
    }
    
    return url.substr(pos);
}

ServiceNodeMap LVSServiceAdapter::filterAddNodes(const ServiceNodeMap &nodes) {

    GetRsRequest request;
    request.sysName = _lvsServiceConfig.sysName;
    request.ip = _lvsServiceConfig.vipIp;
    request.port = _lvsServiceConfig.vipPort;
    
    GetRsResponse response;
    if (!call(GET_RS_OF_VS, &request, &response)) {
        return nodes;
    }

    set<string> remoteNodeSet;
    for (auto rsInfo : response.data) {
        const string &rsIp = rsInfo.rsIp;
        remoteNodeSet.insert(rsIp);
    }
    ServiceNodeMap filterNodes;
    for (ServiceNodeMap::const_iterator it = nodes.begin();
         it != nodes.end(); it ++) {
        const ServiceNode &node = it->second;
        if (remoteNodeSet.find(node.ip) == remoteNodeSet.end()){
            filterNodes[node.ip] = node;
        }
    }
    return filterNodes;
}


ServiceNodeMap LVSServiceAdapter::truncateNodes(const ServiceNodeMap &nodes) {
    if (nodes.size() > 500) {
        ServiceNodeMap tmpNodes;
        size_t i = 0;
        for (ServiceNodeMap::const_iterator it = nodes.begin();
             it != nodes.end() && i < 500; it++, i++)
        {
            tmpNodes.insert({it->first, it->second});
        }

        CARBON_PREFIX_LOG(INFO, "node count [%zd] to be added to lvs "
                          "is exceed the limit 500, truncate the nodes.",
                          nodes.size());
        return tmpNodes;
    } 

    return nodes;
}

void LVSServiceAdapter::filterNodes(const ServiceNodeMap &nodes,
                                    ServiceNodeMap *toDisableNodes,
                                    ServiceNodeMap *toDelNodes)
{
    for (const auto &nodeKV : nodes) {
        if (SN_DISABLED == nodeKV.second.status) {
            toDelNodes->insert(nodeKV);
        } else {
            toDisableNodes->insert(nodeKV);
        }
    }
}
    

bool LVSServiceAdapter::recover() {
    if (_serviceNodeManagerPtr == NULL) {
        CARBON_LOG(ERROR, "recover service adapter failed, "
                   "cause ServiceNodeManager is NULL, adapterId: %s",
                   _id.c_str());
        return false;
    }

    return _serviceNodeManagerPtr->recover();
}

string LVSServiceAdapter::genChangeOrderId(const string &op) {
    return op + "_" + StringUtil::toString(TimeUtility::currentTime());
}

bool LVSServiceAdapter::canSync() {
    int64_t cur = TimeUtility::currentTime();
    int64_t interval = cur - _lastSyncTime;
    if (interval < 0 || interval > LVS_SYNC_INTERVAL) {
        _lastSyncTime = cur;
        return true;
    }
    return false;
}

RequestBase::RequestBase() {}       
RequestBase::~RequestBase() {}

string RequestBase::urlEncode() const {
    map<string, string> kvs;
    toKVMap(kvs);
    return HttpUtil::encodeParams(kvs);
}

#undef LOG_PREFIX

END_CARBON_NAMESPACE(master);

