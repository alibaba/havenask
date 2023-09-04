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
#ifndef CARBON_TUNNELSLBSERVICEADAPTER_H
#define CARBON_TUNNELSLBSERVICEADAPTER_H

#include "carbon/Log.h"
#include "common/common.h"
#include "master/SLBServiceAdapter.h"
#include "master/VPCSLBServiceAdapter.h"

BEGIN_CARBON_NAMESPACE(master);

struct TunnelSLBServiceConfig : autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("name", name, name);
        
        json.Jsonize("vpcAccessKeyId", vpcAccessKeyId, vpcAccessKeyId);
        json.Jsonize("vpcAccessKeySecret", vpcAccessKeySecret, vpcAccessKeySecret);
        json.Jsonize("ecsApiEndpoint", ecsApiEndpoint, ecsApiEndpoint);
        json.Jsonize("vpcId", vpcId, vpcId);
        
        json.Jsonize("innerVpcEndPoint", innerVpcEndPoint, innerVpcEndPoint);
        json.Jsonize("innerVpcResourceUid", innerVpcResourceUid, innerVpcResourceUid);
        json.Jsonize("innerVpcResourceBid", innerVpcResourceBid, innerVpcResourceBid);
        json.Jsonize("innerVpcAccessKeyId", innerVpcAccessKeyId, innerVpcAccessKeyId);
        json.Jsonize("innerVpcAccessKeySecret", innerVpcAccessKeySecret, innerVpcAccessKeySecret);
    }

    std::string name;

    std::string vpcAccessKeyId;
    std::string vpcAccessKeySecret;
    std::string ecsApiEndpoint;
    std::string vpcId;

    std::string innerVpcEndPoint;
    std::string innerVpcResourceUid;
    std::string innerVpcResourceBid;
    std::string innerVpcAccessKeyId;
    std::string innerVpcAccessKeySecret;
};

class TunnelSLBServiceAdapter : public SLBServiceAdapter
{
public:
    TunnelSLBServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                            const std::string &id);
    ~TunnelSLBServiceAdapter();
private:
    TunnelSLBServiceAdapter(const TunnelSLBServiceAdapter &);
    TunnelSLBServiceAdapter& operator=(const TunnelSLBServiceAdapter &);
public:
    /* override */
    bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_TUNNEL_SLB;
    }

    std::string getServiceConfigStr() const {
        return (SLBServiceAdapter::getServiceConfigStr() + 
                autil::legacy::ToJsonString(_tunnelSlbServiceConfig, true));
    }

public:
    /* override */
    bool fillRsServer(const ServiceNode &node, RealServer &rs);

private:
    bool getVgwIpAndTunnelId(const ServiceNode &node,
                             std::string &vgwIp,
                             std::string &tunnelId);

    bool getEcsId(const std::string &ip, std::string &ecsId);
    
    bool getVgwIpAndTunnelId(const std::string &ecsId,
                             std::string &vgwIp,
                             std::string &tunnelId);

private:
    TunnelSLBServiceConfig _tunnelSlbServiceConfig;
        
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(TunnelSLBServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_TUNNELSLBSERVICEADAPTER_H
