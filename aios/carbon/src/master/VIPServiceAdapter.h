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
#ifndef CARBON_VIPSERVICEADAPTER_H
#define CARBON_VIPSERVICEADAPTER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"

#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

struct IPInfo : public autil::legacy::Jsonizable {
    std::string ip;
    int32_t port;
    int32_t weight;
    bool valid;
    std::string unit;
    std::string hostname;
    int32_t checkRT;
    std::string cluster;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("ip", ip, std::string());
        json.Jsonize("port", port, 0);
        json.Jsonize("weight", weight, 0);
        json.Jsonize("valid", valid, false);
        json.Jsonize("unit", unit, std::string());
        json.Jsonize("hostname", hostname, std::string());
        json.Jsonize("checkRT", checkRT, 0);
        json.Jsonize("cluster", cluster, std::string());
    }
};

struct VIPServiceConfig : public autil::legacy::Jsonizable
{
public:
    VIPServiceConfig() {
        port = 0;
        weight = 1.0f;
    }
    ~VIPServiceConfig() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("domain", domain, std::string());
        json.Jsonize("token", token, std::string());
        json.Jsonize("port", port, 0);
        json.Jsonize("jmenvDom", jmenvDom, std::string());
        json.Jsonize("jmenvUrl", jmenvUrl, std::string());
        json.Jsonize("weight", weight, 1.0f);
    }
public:
    std::string domain;
    std::string token;
    std::string jmenvDom;
    std::string jmenvUrl;
    int32_t port;
    float weight;
};
    
class VIPServiceAdapter : public ServiceAdapter
{
public:
    VIPServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                      const std::string &id = "");
    ~VIPServiceAdapter();
private:
    VIPServiceAdapter(const VIPServiceAdapter &);
    VIPServiceAdapter& operator=(const VIPServiceAdapter &);
public:
    /* override */
    bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_VIP;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_vipServiceConfig, true);
    }

    /* override */
    bool recover();
    
public:
    //public for test
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
    
public:
    //public for test
    bool getAvailableVIPServer();

    bool splitUrl(const std::string &url, std::string *hostSpec,
                  std::string *uri);

    bool getServerList(std::vector<std::string> *servers);

public:
    //for test
    void setHttpClient(HttpClientPtr httpClientPtr) {
        _httpClientPtr = httpClientPtr;
    }
    std::string getVipServerSpec() {
        return _vipServerSpec;
    }
    void  setVipServerSpec(const std::string &vipServerSpec) {
        _vipServerSpec = vipServerSpec;
    }
    
    VIPServiceConfig getServiceConfig() const {
        return _vipServiceConfig;
    }
private:
    bool doQuery(const std::string &uri, const std::string& qs, bool get, std::string &content);

    void filterNodes(ServiceNodeMap *nodes);
    
private:
    ServiceNodeManagerPtr _serviceNodeManagerPtr;
    VIPServiceConfig _vipServiceConfig;
    HttpClientPtr _httpClientPtr;
    std::string _vipServerSpec;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(VIPServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_VIPSERVICEADAPTER_H
