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
#include "aios/network/gig/multi_call/config/SubscribeClustersConfig.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, SubscribeClustersConfig);

void VIPDomainConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("vip_domain", _vipDomain, _vipDomain);
    json.Jsonize("vip_port_type", _vipPortType, _vipPortType);
    json.Jsonize("tcp_port_diff", _tcpPortDiff, _tcpPortDiff);
    json.Jsonize("arpc_port_diff", _arpcPortDiff, _arpcPortDiff);
    json.Jsonize("http_port_diff", _httpPortDiff, _httpPortDiff);
    json.Jsonize("grpc_port_diff", _grpcPortDiff, _grpcPortDiff);
    json.Jsonize("grpc_stream_port_diff", _grpcStreamPortDiff,
                 _grpcStreamPortDiff);
    json.Jsonize("rdma_arpc_port_diff", _rdmaArpcPortDiff, _rdmaArpcPortDiff);
}

bool VIPDomainConfig::operator==(const VIPDomainConfig &rhs) const {
    return _vipDomain == rhs._vipDomain &&
           _vipPortType == rhs._vipPortType &&
           _tcpPortDiff == rhs._tcpPortDiff &&
           _arpcPortDiff == rhs._arpcPortDiff &&
           _httpPortDiff == rhs._httpPortDiff &&
           _grpcPortDiff == rhs._grpcPortDiff &&
           _grpcStreamPortDiff == rhs._grpcStreamPortDiff &&
           _rdmaArpcPortDiff == rhs._rdmaArpcPortDiff;
}

void SubscribeClustersConfig::Jsonize(
    autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("cm2_clusters", cm2Clusters, cm2Clusters);
    json.Jsonize("cm2_no_topoinfo_clusters", cm2NoTopoClusters,
                 cm2NoTopoClusters);
    json.Jsonize("vip_domains", vipDomains, vipDomains);
    json.Jsonize("istio_biz_clusters", istioBizClusters, istioBizClusters);
}

bool SubscribeClustersConfig::operator==(
    const SubscribeClustersConfig &rhs) const {
    return cm2Clusters == rhs.cm2Clusters &&
           cm2NoTopoClusters == cm2NoTopoClusters &&
           vipDomains == rhs.vipDomains &&
           istioBizClusters == rhs.istioBizClusters;
}

void SubscribeClustersConfig::addVipDomain(const std::string domain) {
    VIPDomainConfig domainConfig(domain);
    vipDomains.push_back(domainConfig);
}

bool SubscribeClustersConfig::checkSub(bool cm2Sub, bool vipSub,
                                       bool istioSub) const {
    bool check = true;
    if (!cm2Sub && hasCm2Sub()) {
        AUTIL_LOG(
            ERROR,
            "cm2 clusters not dealed, cm2Clusters:%s, cm2NoTopoClusters:%s",
            StringUtil::toString(cm2Clusters).c_str(),
            StringUtil::toString(cm2NoTopoClusters).c_str());
        check = false;
    }
    if (!vipSub && hasVipSub()) {
        AUTIL_LOG(ERROR, "vip domains not dealed:%s",
                  ToJsonString(vipDomains).c_str());
        check = false;
    }
    if (!istioSub && hasIstioSub()) {
        AUTIL_LOG(ERROR, "istio biz clusters not dealed:%s",
                  StringUtil::toString(istioBizClusters).c_str());
        check = false;
    }
    return check;
}

} // namespace multi_call
