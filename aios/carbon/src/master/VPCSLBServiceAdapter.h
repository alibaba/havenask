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
#ifndef CARBON_VPCSLBSERVICEADAPTER_H
#define CARBON_VPCSLBSERVICEADAPTER_H

#include "carbon/Log.h"
#include "common/common.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

struct VPCSLBServiceConfig : public autil::legacy::Jsonizable {
    VPCSLBServiceConfig() {
        slbApiEndpoint = "slb.aliyuncs.com";
        ecsApiEndpoint = "ecs.aliyuncs.com";
        serverType = "eni";
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("slbApiEndpoint", slbApiEndpoint, slbApiEndpoint);
        json.Jsonize("ecsApiEndpoint", ecsApiEndpoint, ecsApiEndpoint);
        json.Jsonize("accessKeyId", accessKeyId, accessKeyId);
        json.Jsonize("accessKeySecret", accessKeySecret, accessKeySecret);
        json.Jsonize("serverType", serverType, serverType);
        json.Jsonize("lbId", lbId, lbId);
        json.Jsonize("listenerPort", listenerPort, listenerPort);
        json.Jsonize("servicePort", servicePort, servicePort);
        json.Jsonize("vServerGroupId", vServerGroupId, vServerGroupId);
        json.Jsonize("regionId", regionId, regionId);
        json.Jsonize("vpcId", vpcId, vpcId);
        json.Jsonize("resourceGroupId", resourceGroupId, resourceGroupId);
        json.Jsonize("vswitchId", vswitchId, vswitchId);
    }

    std::string slbApiEndpoint;
    std::string ecsApiEndpoint;
    std::string accessKeyId;
    std::string accessKeySecret;
    
    std::string serverType; // 'ecs' or 'eni'
    std::string lbId;
    std::string listenerPort;
    std::string servicePort;
    std::string vServerGroupId;
    std::string regionId;
    std::string vpcId;
    std::string resourceGroupId;
    std::string vswitchId;
};

typedef std::map<std::string, std::string> ParamMap;

struct VPCResponse : public autil::legacy::Jsonizable {
    virtual ~VPCResponse() {}

    std::string requestId;
    std::string message;
    std::string code;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("RequestId", requestId, requestId);
        json.Jsonize("Code", code, code);
        json.Jsonize("Message", message, message);
    }
};

class Caller {
public:
    static std::string HTTP_METHOD_GET;
    static std::string HTTP_METHOD_POST;
public:
    Caller(const HttpClientPtr &httpClientPtr,
           const std::string &httpMethod = HTTP_METHOD_GET);
    virtual ~Caller() {}
public:
    bool call(const ParamMap &params,
              VPCResponse *response);
protected:
    virtual bool extendParams(ParamMap &params) const = 0;
    virtual std::string getAddress() const = 0;
    virtual bool hasError(const VPCResponse *response) const = 0;
private:
    HttpClientPtr _httpClientPtr;
    std::string _httpMethod;
private:
    CARBON_LOG_DECLARE();
};

class VPCCaller : public Caller {
public:
    VPCCaller(const HttpClientPtr &httpClientPtr, const VPCSLBServiceConfig &config,
              const std::string &httpMethod = HTTP_METHOD_GET)
        : Caller(httpClientPtr, httpMethod)
        , _config(config)
    {}
    
protected:
    virtual bool extendParams(ParamMap &params) const;
    virtual std::string getApiVersion() const = 0;
    virtual bool hasError(const VPCResponse *response) const;
private:
    std::string formatTimestamp() const;
    std::string signature(const std::string &httpMethod, const ParamMap &params) const;
    
protected:
    VPCSLBServiceConfig _config;
private:
    CARBON_LOG_DECLARE();
};
    
class SLBCaller : public VPCCaller {
public:
    SLBCaller(const HttpClientPtr &httpClientPtr, const VPCSLBServiceConfig &config,
              const std::string &httpMethod = HTTP_METHOD_GET)
        : VPCCaller(httpClientPtr, config, httpMethod)
    {}
protected:
    std::string getAddress() const;
    std::string getApiVersion() const;
    bool hasError(const VPCResponse *response) const;
private:
    CARBON_LOG_DECLARE();
};

class ECSCaller : public VPCCaller {
public:
    ECSCaller(const HttpClientPtr &httpClientPtr, const VPCSLBServiceConfig &config,
              const std::string &httpMethod = HTTP_METHOD_POST)
        : VPCCaller(httpClientPtr, config, httpMethod)
    {}
protected:
    std::string getAddress() const;
    std::string getApiVersion() const;
};

struct NetworkInterfaceSet : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("NetworkInterfaceId", nifId, nifId);
        json.Jsonize("InstanceId", instanceId, instanceId);
        json.Jsonize("PrivateIpAddress", ip, ip);
    }

    std::string instanceId;
    std::string nifId;
    std::string ip;
};

struct NetworkInterfaceSets : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("NetworkInterfaceSet", nifs, nifs);
    }

    std::vector<NetworkInterfaceSet> nifs;
};

struct GetEniResponse : public VPCResponse {
    GetEniResponse() {
        pageNum = 0;
        pageSize = 0;
        totalCount = 0;
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        VPCResponse::Jsonize(json);
        json.Jsonize("PageNumber", pageNum, pageNum);
        json.Jsonize("PageSize", pageSize, pageSize);
        json.Jsonize("TotalCount", totalCount, totalCount);
        json.Jsonize("NetworkInterfaceSets", nifSets, nifSets);
    }
    
    int32_t pageNum;
    int32_t pageSize;
    int32_t totalCount;
    NetworkInterfaceSets nifSets;
};

struct SLBBackendServer : public autil::legacy::Jsonizable {
    SLBBackendServer() {
        weight = 100;
        type = "eni";
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("ServerId", serverId, serverId);
        json.Jsonize("Type", type, type);
        json.Jsonize("Port", port, port);
        json.Jsonize("Weight", weight, weight);
    }

    std::string serverId;
    std::string type;
    std::string port;
    int32_t weight;
};

struct SLBBackendServerStatus : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("ServerId", serverId, serverId);
        json.Jsonize("ServerHealthStatus", healthStatus, healthStatus);
        json.Jsonize("EniHost", eniHost, eniHost);
        json.Jsonize("ServerIp", serverIp, serverIp);
        json.Jsonize("Type", type, type);
    }

    std::string serverId;
    std::string healthStatus;
    std::string eniHost;
    std::string serverIp;
    std::string type;
};

struct SLBResponse : public VPCResponse {
    std::string requestId;
    std::string message;
    std::string code;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        VPCResponse::Jsonize(json);
        json.Jsonize("RequestId", requestId, requestId);
        json.Jsonize("Code", code, code);
        json.Jsonize("Message", message, message);
    }
};

struct GetBackendServerResponse : public SLBResponse {
    struct SLBBackendServerStatuses : autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
            json.Jsonize("BackendServer", servers, servers);
        }
        std::vector<SLBBackendServerStatus> servers;
    };
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        SLBResponse::Jsonize(json);
        json.Jsonize("BackendServers", backendServers, backendServers);
    }
    SLBBackendServerStatuses backendServers;
};

struct AddBackendServerResponse : public SLBResponse {
};

struct DelBackendServerResponse : public SLBResponse {
};

class ServerIdCache {
public:
    ServerIdCache(const HttpClientPtr &httpClientPtr, const VPCSLBServiceConfig &config);
public:
    /* virtual for mock */
    virtual std::string getServerId(const std::string &ip);

    /* virtual for mock */
    virtual std::string getIp(const std::string &serverId);
    
private:
    void updateCache();
    std::map<std::string, std::string> getAllEnis();
    std::string doGet(const std::map<std::string,
            std::string> &cache, const std::string &key);
    void setCache(const std::map<std::string, std::string> &ip2serverIds);
    
private:
    std::map<std::string, std::string> _serverId2IpMap;
    std::map<std::string, std::string> _ip2ServerIdMap;
    ECSCaller _ecsCaller;
    VPCSLBServiceConfig _config;
private:
    CARBON_LOG_DECLARE();
};

class VPCSLBServiceAdapter : public ServiceAdapter
{
public:
    VPCSLBServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                         const std::string &id = "");
    ~VPCSLBServiceAdapter();
private:
    VPCSLBServiceAdapter(const VPCSLBServiceAdapter &);
    VPCSLBServiceAdapter& operator=(const VPCSLBServiceAdapter &);
public:
    /* override */
    bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_SLB;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_vpcSlbServiceConfig, true);
    }

    /* override */
    bool recover();

    /* override */
    bool canSync() {return true;}

public:
    /* override */
    nodespec_t getServiceNodeSpec(const ServiceNode &node) const {
        return node.ip;
    }
    
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

private:
    void setHttpClient(const HttpClientPtr &httpClientPtr) {
        _httpClientPtr = httpClientPtr;
    }

    ServiceNodeMap truncateNodes(const ServiceNodeMap &nodes);

    std::string formatBackendServers(const ServiceNodeMap &nodes);

    std::vector<std::string> getServerIds(const ServiceNodeMap &nodes);

    std::shared_ptr<ServerIdCache> getServerIdCache() const {
        return _serverIdCachePtr;
    }
    
private:
    HttpClientPtr _httpClientPtr;
    ServiceNodeManagerPtr _serviceNodeManagerPtr;
    VPCSLBServiceConfig _vpcSlbServiceConfig;
    std::shared_ptr<ServerIdCache> _serverIdCachePtr;
    std::shared_ptr<SLBCaller> _slbCallerPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(VPCSLBServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_VPCSLBSERVICEADAPTER_H
