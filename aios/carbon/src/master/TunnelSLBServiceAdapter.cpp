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
#include "master/TunnelSLBServiceAdapter.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, TunnelSLBServiceAdapter);

#define LOG_PREFIX _id.c_str()

TunnelSLBServiceAdapter::TunnelSLBServiceAdapter(
        const ServiceNodeManagerPtr &serviceNodeManagerPtr,
        const string &id)
    : SLBServiceAdapter(serviceNodeManagerPtr, id)
{
    
}

TunnelSLBServiceAdapter::~TunnelSLBServiceAdapter() {
}

bool TunnelSLBServiceAdapter::doInit(const ServiceConfig &config) {
    try {
        FromJsonString(_slbServiceConfig, config.configStr);
        FromJsonString(_tunnelSlbServiceConfig, config.configStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_PREFIX_LOG(ERROR, "tunnel slb service config from json fail, exception[%s]",
                          e.what());
        return  false;
    }
    return true;
}

bool TunnelSLBServiceAdapter::fillRsServer(const ServiceNode &node, RealServer &rs) {
    if (!SLBServiceAdapter::fillRsServer(node, rs)) {
        return false;
    }
    
    string vgwIp;
    string tunnelId;
    if (!getVgwIpAndTunnelId(node, vgwIp, tunnelId)) {
        return false;
    }
    
    if (rs.rsType.empty()) {
        rs.rsType = "vpc";
    }
    rs.vgwIp = vgwIp;
    rs.tunnelId = tunnelId;
    
    return true;
}

class InnerVpcCaller : public VPCCaller {
public:
    InnerVpcCaller(const HttpClientPtr &httpClientPtr,
                   const VPCSLBServiceConfig &config,
                   const TunnelSLBServiceConfig &tunnelSlbConfig,
                   const std::string &httpMethod = HTTP_METHOD_POST)
        : VPCCaller(httpClientPtr, config, httpMethod)
    {
        _tunnelSlbConfig = tunnelSlbConfig;
    }

    string getAddress() const {
        return _tunnelSlbConfig.innerVpcEndPoint;
    }
    
    string getApiVersion() const {
        return "2016-04-28";
    }

private:
    TunnelSLBServiceConfig _tunnelSlbConfig;
};

class GetVgwIpResponse : public VPCResponse {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        VPCResponse::Jsonize(json);
        json.Jsonize("VgwIp", vgwIp, vgwIp);
    }
    
    std::string vgwIp;
};

class GetTunnelIdResponse : public VPCResponse {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("VpcId", vpcId, vpcId);
        json.Jsonize("TunnelId", tunnelId, tunnelId);
    }

    std::string vpcId;
    std::string tunnelId;
};

bool TunnelSLBServiceAdapter::getEcsId(const string &ip, string &ecsId) {
    VPCSLBServiceConfig config;
    config.ecsApiEndpoint = _tunnelSlbServiceConfig.ecsApiEndpoint;
    config.accessKeyId = _tunnelSlbServiceConfig.vpcAccessKeyId;
    config.accessKeySecret = _tunnelSlbServiceConfig.vpcAccessKeySecret;
    config.vpcId = _tunnelSlbServiceConfig.vpcId;
    
    ECSCaller ecsCaller(_httpClientPtr, config);
    ParamMap params;
    params["Action"] = "DescribeNetworkInterfaces";
    params["RegionId"] = _slbServiceConfig.region;
    params["VpcId"] = _tunnelSlbServiceConfig.vpcId;
    params["PrimaryIpAddress"] = ip;

    GetEniResponse response;
    if (!ecsCaller.call(params, &response)) {
        CARBON_LOG(ERROR, "get ecs info failed.");
        return false;
    }

    const auto &nifs = response.nifSets.nifs;
    if (nifs.size() == 0) {
        CARBON_LOG(ERROR, "get empty eni infos.");
        return false;
    }
            
    ecsId = response.nifSets.nifs[0].instanceId;
    return true;
}

bool TunnelSLBServiceAdapter::getVgwIpAndTunnelId(
        const ServiceNode &node, string &vgwIp, string &tunnelId)
{
    string ecsId;
    if (!getEcsId(node.ip, ecsId)) {
        return false;
    }

    return getVgwIpAndTunnelId(ecsId, vgwIp, tunnelId);
}

bool TunnelSLBServiceAdapter::getVgwIpAndTunnelId(
        const string &ecsId, string &vgwIp, string &tunnelId)
{
    VPCSLBServiceConfig innerVpcConfig;
    innerVpcConfig.slbApiEndpoint = _tunnelSlbServiceConfig.innerVpcEndPoint;
    innerVpcConfig.accessKeyId = _tunnelSlbServiceConfig.innerVpcAccessKeyId;
    innerVpcConfig.accessKeySecret = _tunnelSlbServiceConfig.innerVpcAccessKeySecret;
    InnerVpcCaller innerVpcCaller(_httpClientPtr, innerVpcConfig, _tunnelSlbServiceConfig);

    map<string, string> params;
    params["Action"] = "InnerVpcCloudQueryVgwVipByEcsId";
    params["EcsId"] = ecsId;
    params["Format"] = "JSON";
    params["RegionId"] = _slbServiceConfig.region;

    GetVgwIpResponse vgwResponse;
    if (!innerVpcCaller.call(params, &vgwResponse)) {
        CARBON_LOG(ERROR, "get vgw ip failed.");
        return false;
    }

    vgwIp = vgwResponse.vgwIp;
    size_t pos = vgwIp.find('/');
    if (pos != string::npos) {
        vgwIp = vgwIp.substr(0, pos);
    }

    params.clear();
    params["Action"] = "InnerVpcCloudQuerySimpleVpcInfo";
    params["VpcId"] = _tunnelSlbServiceConfig.vpcId;
    params["Format"] = "JSON";
    params["RegionId"] = _slbServiceConfig.region;
    params["ResourceBid"] = _tunnelSlbServiceConfig.innerVpcResourceBid;
    params["ResourceUid"] = _tunnelSlbServiceConfig.innerVpcResourceUid;

    GetTunnelIdResponse tunnelIdResponse;
    if (!innerVpcCaller.call(params, &tunnelIdResponse)) {
        CARBON_LOG(ERROR, "get tunnel id failed.");
        return false;
    }

    tunnelId = tunnelIdResponse.tunnelId;
    
    return true;
}

END_CARBON_NAMESPACE(master);

