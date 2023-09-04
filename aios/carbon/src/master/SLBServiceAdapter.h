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
#ifndef CARBON_SLBSERVICEADAPTER_H
#define CARBON_SLBSERVICEADAPTER_H

#include "carbon/Log.h"
#include "common/common.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"

BEGIN_CARBON_NAMESPACE(master);

// API doc: https://yuque.antfin.com/slbdoc/ua48c6/hv6obk#cdpokt
struct SLBServiceConfig : autil::legacy::Jsonizable {
    SLBServiceConfig() {
        apiUrl = "http://albapi-cn-hangzhou-pri.slb.aliyuncs.com/slb/api";
        frontendPort = 0;
        rsPort = 0;
    }
    
    std::string apiUrl;
    std::string accessId;
    std::string accessKey;
    std::string idkp;
    std::string region;
    std::string lbid;
    std::string bid;
    std::string rsType; // optional
    int frontendPort;
    int rsPort;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("apiUrl", apiUrl, apiUrl);
        json.Jsonize("accessId", accessId, accessId);
        json.Jsonize("accessKey", accessKey, accessKey);
        json.Jsonize("idkp", idkp, idkp);
        json.Jsonize("region", region, region);
        json.Jsonize("lbid", lbid, lbid);
        json.Jsonize("bid", bid, bid);
        json.Jsonize("frontendPort", frontendPort, frontendPort);
        json.Jsonize("rsPort", rsPort, rsPort);
        json.Jsonize("rsType", rsType, rsType);
    }
};

struct RealServer : autil::legacy::Jsonizable {
    RealServer() {
        weight = 100;
        port = 0;
    }
    
    std::string rsIp;
    int weight;
    int port;
    std::string rsType;
    std::string tunnelId;
    std::string vgwIp;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("rs_ip", rsIp, rsIp);
        json.Jsonize("weight", weight, weight);
        json.Jsonize("port", port, port);

        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            if (rsType != "") {
                json.Jsonize("rs_type", rsType, rsType);
                json.Jsonize("tunnel_id", tunnelId, tunnelId);
                json.Jsonize("vgw_ip", vgwIp, vgwIp);
            }
        }
    }
};

struct Response : autil::legacy::Jsonizable {
    int code;
    std::string msg;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("code", code, code);
        json.Jsonize("msg", msg, msg);
    }
};

struct GetRsListResponse : public Response {
    struct Data : public autil::legacy::Jsonizable {
        std::vector<RealServer> rsList;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
            json.Jsonize("rs_list", rsList, rsList);
        }
    };

    Data data;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        Response::Jsonize(json);
        json.Jsonize("data", data);
    }
};

struct RealServerStatus : public autil::legacy::Jsonizable {
    std::string rsIp;
    std::string status;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("address", rsIp, rsIp);
        json.Jsonize("status", status, status);
    }
};

struct GetRsHealthResponse : public Response {
    struct Data : public autil::legacy::Jsonizable {
        struct VipStatus : public autil::legacy::Jsonizable {
            std::vector<RealServerStatus> rsStatusList;
            int frontendPort;
            void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
                json.Jsonize("frontend_port", frontendPort);
                json.Jsonize("rs_list", rsStatusList);
            }
        };

        std::vector<VipStatus> vipStatusList;

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
            json.Jsonize("vips", vipStatusList);
        }
    };

    Data data;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        Response::Jsonize(json);
        json.Jsonize("data", data);
    }
};

class SLBServiceAdapter : public ServiceAdapter
{
public:
    SLBServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                      const std::string &id = "");
    ~SLBServiceAdapter();
private:
    SLBServiceAdapter(const SLBServiceAdapter &);
    SLBServiceAdapter& operator=(const SLBServiceAdapter &);
public:
    /* override */
    bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_SLB;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_slbServiceConfig, true);
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

    virtual bool fillRsServer(const ServiceNode &node, RealServer &rs);

public:
    bool doOperateNodes(const std::string &action, const ServiceNodeMap &nodes);

    virtual std::string formatTimestamp();

    std::string signature(const std::map<std::string, std::string> &params);

    ServiceNodeMap truncateNodes(const ServiceNodeMap &nodes);

    bool call(const std::string &action,
              const std::map<std::string, std::string> &args,
              Response *response);

public:
    void setHttpClient(const HttpClientPtr &httpClientPtr) {
        _httpClientPtr = httpClientPtr;
    }

protected:
    SLBServiceConfig _slbServiceConfig;
    HttpClientPtr _httpClientPtr;
    ServiceNodeManagerPtr _serviceNodeManagerPtr;    
private:
CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SLBServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SLBSERVICEADAPTER_H
