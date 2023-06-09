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
#ifndef ISEARCH_MULTI_CALL_GIGSUBSCRIBECLUSTERSCONF_H
#define ISEARCH_MULTI_CALL_GIGSUBSCRIBECLUSTERSCONF_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "autil/legacy/jsonizable.h"
#include <string>
#include <vector>

namespace multi_call {

class VIPDomainConfig : public autil::legacy::Jsonizable {
public:
    VIPDomainConfig(const std::string &domain = "")
        : _vipDomain(domain), _vipPortType(MC_PROTOCOL_HTTP), _tcpPortDiff(0),
          _arpcPortDiff(0), _httpPortDiff(0), _grpcPortDiff(0),
          _grpcStreamPortDiff(0), _rdmaArpcPortDiff(0) {}
    ~VIPDomainConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const VIPDomainConfig &rhs) const;

public:
    const std::string &getVipDomain() const { return _vipDomain; }
    ProtocolType getVipPortType() const { return _vipPortType; }
    int32_t getTcpPortDiff() const { return _tcpPortDiff; }
    int32_t getArpcPortDiff() const { return _arpcPortDiff; }
    int32_t getHttpPortDiff() const { return _httpPortDiff; }
    int32_t getGrpcPortDiff() const { return _grpcPortDiff; }
    int32_t getGrpcStreamPortDiff() const { return _grpcStreamPortDiff; }
    int32_t getRdmaArpcPortDiff() const { return _rdmaArpcPortDiff; }

private:
    std::string _vipDomain;
    ProtocolType _vipPortType;
    int32_t _tcpPortDiff;
    int32_t _arpcPortDiff;
    int32_t _httpPortDiff;
    int32_t _grpcPortDiff;
    int32_t _grpcStreamPortDiff;
    int32_t _rdmaArpcPortDiff;
};

class SubscribeClustersConfig : public autil::legacy::Jsonizable {
public:
    SubscribeClustersConfig() {}
    ~SubscribeClustersConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const SubscribeClustersConfig &rhs) const;

public:
    void addVipDomain(const std::string domain);
    bool checkSub(bool cm2Sub, bool vipSub, bool istioSub) const;
    bool hasCm2Sub() const {
        return !cm2Clusters.empty() || !cm2NoTopoClusters.empty();
    }
    bool hasVipSub() const {
        return !vipDomains.empty();
    }
    bool hasIstioSub() const {
        return !istioBizClusters.empty();
    }

public:
    std::vector<std::string> cm2Clusters;
    std::vector<std::string> cm2NoTopoClusters;
    std::vector<VIPDomainConfig> vipDomains;
    std::vector<std::string> istioBizClusters;
    LocalConfig localConfig;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SubscribeClustersConfig);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSUBSCRIBECLUSTERSCONF_H
