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
#ifndef CARBON_LVSSERVICEADAPTER_H
#define CARBON_LVSSERVICEADAPTER_H

#include "carbon/Log.h"
#include "common/common.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"

BEGIN_CARBON_NAMESPACE(master);

/*
  xvip interface doc: 
       xxxx://invalid/xnet2/xvip-doc/wikis/xvip_open_API_document
 */

struct LVSServiceConfig : public autil::legacy::Jsonizable {
    LVSServiceConfig() {
        sysName = "hippo";
        token = "";
        vipPort = 0;
        appPort = 0;
        xvipManagerUrl = "http://xvip.alibaba-inc.com/xvip/api";
        keyCenterUrl = "http://keycenter-service.alibaba-inc.com/keycenter";
    }
    
    std::string vipIp;
    int32_t vipPort;
    int32_t appPort;
    std::string applyUser;
    std::string sysName;
    std::string token;
    std::string xvipManagerUrl;
    std::string publishNum;
    std::string appName;
    std::string key;
    std::string keyCenterUrl;
    std::string site;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("vipIp", vipIp, vipIp);
        json.Jsonize("vipPort", vipPort, 0);
        json.Jsonize("appPort", appPort, 0);
        json.Jsonize("applyUser", applyUser, applyUser);
        json.Jsonize("sysName", sysName, sysName);
        json.Jsonize("token", token, token);
        json.Jsonize("xvipManagerUrl", xvipManagerUrl, xvipManagerUrl);
        json.Jsonize("publishNum", publishNum, publishNum);
        json.Jsonize("appName", appName, appName);
        json.Jsonize("key", key, key);
        json.Jsonize("keyCenterUrl", keyCenterUrl, keyCenterUrl);
        json.Jsonize("site", site, site);
    }
};

class RequestBase {
public:
    RequestBase();
    virtual ~RequestBase();
    
    void toKVMap(std::map<std::string, std::string> &kvs) const {
        kvs["sysname"] = sysName;
        _toKVMap(kvs);
    }
    
    std::string urlEncode() const;

public:
    std::string sysName;
    
protected:
    virtual void _toKVMap(std::map<std::string, std::string> &kvs) const = 0;
};

struct UpdateRsInfo : public autil::legacy::Jsonizable {
    UpdateRsInfo() {
        port = 0;
    }
    
    std::string ip;
    int32_t port;
    std::string op;
    std::string status;
    std::string site;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("ip", ip);
        json.Jsonize("port", port, 0);
        json.Jsonize("op", op, op);
        json.Jsonize("site", site, site);
        json.Jsonize("status", status, status);
    }
};

class UpdateVipRequest : public RequestBase {
public:
    UpdateVipRequest() {
        port = 0;
        protocol = "TCP";
    }
    ~UpdateVipRequest() {}

    std::string applyUser;
    std::string ip;
    int32_t port;
    std::string protocol;
    std::string changeOrderId;
    std::vector<UpdateRsInfo> rs;

protected:
    void _toKVMap(std::map<std::string, std::string> &kvs) const {
        kvs["applyUser"] = applyUser;
        kvs["ip"] = ip;
        kvs["port"] = autil::StringUtil::toString(port);
        kvs["protocol"] = protocol;
        kvs["changeOrderId"] = changeOrderId;
        kvs["rs"] = autil::legacy::ToJsonString(rs);
    }
};

struct ResponseBase : public autil::legacy::Jsonizable {
    std::string reqId;
    std::string errCode;
    std::string errMsg;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("reqId", reqId, reqId);
        json.Jsonize("errCode", errCode, errCode);
        json.Jsonize("errMsg", errMsg, errMsg);
    }
};

typedef ResponseBase UpdateVipResponse;

class GetRsRequest : public RequestBase {
public:
    std::string ip;
    int32_t port;
    std::string protocol;

    GetRsRequest() {
        protocol = "TCP";
    }

    ~GetRsRequest() {}

protected:
    void _toKVMap(std::map<std::string, std::string> &kvs) const {
        kvs["ip"] = ip;
        kvs["port"] = autil::StringUtil::toString(port);
        kvs["protocol"] = protocol;
    }
};

struct RsInfo : public autil::legacy::Jsonizable {
    std::string hostname;
    std::string vsIp;
    int32_t vsPort;
    std::string vsProtocol;
    std::string rsIp;
    int32_t rsPort;
    std::string site;
    std::string status;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("hostname", hostname, hostname);
        json.Jsonize("vsIp", vsIp, vsIp);
        json.Jsonize("vsPort", vsPort, vsPort);
        json.Jsonize("vsProtocol", vsProtocol, vsProtocol);
        json.Jsonize("rsIp", rsIp, rsIp);
        json.Jsonize("rsPort", rsPort, rsPort);
        json.Jsonize("site", site, site);
        json.Jsonize("status", status, status);
    }
};

struct GetRsResponse : public ResponseBase {
    std::vector<RsInfo> data;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        ResponseBase::Jsonize(json);
        json.Jsonize("data", data, data);
    }
};

typedef GetRsRequest GetRsStatusRequest;

struct RsStatus : public autil::legacy::Jsonizable {
    std::string rsIp;
    int32_t rsPort;
    std::string hc;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("rsIp", rsIp, rsIp);
        json.Jsonize("rsPort", rsPort, 0);
        json.Jsonize("hc", hc, hc);
    }
};

struct GetRsStatusResponseData : public autil::legacy::Jsonizable {
    std::string vsIp;
    int32_t vsPort;
    std::string protocol;
    std::vector<RsStatus> rs;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("vsIp", vsIp, vsIp);
        json.Jsonize("vsPort", vsPort, 0);
        json.Jsonize("protocol", protocol, std::string("TCP"));
        json.Jsonize("rs", rs, rs);
    }
};

struct GetRsStatusResponse : public ResponseBase {
    GetRsStatusResponseData data;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        ResponseBase::Jsonize(json);
        json.Jsonize("data", data);
    }
};

class LVSServiceAdapter : public ServiceAdapter
{
public:
    LVSServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                      const std::string &id = "");
    ~LVSServiceAdapter();
private:
    LVSServiceAdapter(const LVSServiceAdapter &);
    LVSServiceAdapter& operator=(const LVSServiceAdapter &);

public:
    /* override */
    bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_LVS;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_lvsServiceConfig, true);
    }

    /* override */
    bool recover();

    /* override */
    bool canSync();
    
public:
    /* override */
    nodespec_t getServiceNodeSpec(const ServiceNode &node) const;
    
    /* override */
    bool doAddNodes(const ServiceNodeMap &nodes);
    
    /* override */
    bool doDelNodes(const ServiceNodeMap &nodes);
    
    /* override */
    bool doGetNodes(ServiceNodeMap *nodes);

    /* override */
    bool doUpdateNodes(const ServiceNodeMap &nodes) {
        return true;
    }

    virtual std::string genChangeOrderId(const std::string &op);

public: // for test
    void setHttpClient(const HttpClientPtr &httpClientPtr) {
        _httpClientPtr = httpClientPtr;
    }

protected:
    ServiceNodeMap truncateNodes(const ServiceNodeMap &nodes);

    bool call(const std::string &api, const RequestBase *request,
              ResponseBase *response);
    
    std::string getApiUrl(const std::string &api);

    bool updateVip(const ServiceNodeMap &nodes,
                   const std::string &op,
                   const std::string &status);

    bool getParamData(const std::string &url,
                      const RequestBase *request,
                      std::string &paramData);


    bool genSignature(const std::string &uri,
                      const std::map<std::string, std::string> &params,
                      std::string &signature);

    bool genSignature(const std::string &uri, const std::string &paramData,
                      std::string &signature);

    std::string extractUri(const std::string &url);

    void filterNodes(const ServiceNodeMap &nodes,
                     ServiceNodeMap *toDisableNodes,
                     ServiceNodeMap *toDelNodes);

    ServiceNodeMap filterAddNodes(const ServiceNodeMap &nodes);
    
private:
    ServiceNodeManagerPtr _serviceNodeManagerPtr;
    LVSServiceConfig _lvsServiceConfig;
    HttpClientPtr _httpClientPtr;
    void *_signContext;
    int64_t _lastSyncTime;
private:
CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(LVSServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_LVSSERVICEADAPTER_H
