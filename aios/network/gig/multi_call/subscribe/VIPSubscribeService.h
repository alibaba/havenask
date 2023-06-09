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
#ifndef ISEARCH_MULTI_CALL_VIPSUBSCRIBESERVICE_H
#define ISEARCH_MULTI_CALL_VIPSUBSCRIBESERVICE_H

#include <any>
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/config/SubscribeClustersConfig.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"
#include "autil/Lock.h"

namespace multi_call {

class VIPSubscribeService : public SubscribeService {
public:
    VIPSubscribeService(const VipConfig &config);
    ~VIPSubscribeService();

private:
    VIPSubscribeService(const VIPSubscribeService &);
    VIPSubscribeService &operator=(const VIPSubscribeService &);

public:
    // It is necessary to destroy static resources explicitly.
    // Because both vipclient and alog are singleton,
    // alog is depended by vipclient,
    // but the destruct order is unexpected.
    static void destroy();

public:
    bool init() override;
    SubscribeType getType() override { return ST_VIP; }
    bool clusterInfoNeedUpdate() override;
    bool getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs) override;

    bool addSubscribe(const std::vector<std::string> &names) override;
    bool deleteSubscribe(const std::vector<std::string> &names) override;
    bool
    addSubscribeDomainConfig(const std::vector<VIPDomainConfig> &vipDomains);
    bool
    deleteSubscribeDomainConfig(const std::vector<VIPDomainConfig> &vipDomains);

private:
    virtual bool queryAllIp(const std::string &clusterName,
                            std::any hosts) const;

private:
    autil::ReadWriteLock _configLock;
    VipConfig _config;
    std::unordered_map<std::string, VIPDomainConfig> _domainConfigs;

private:
    static uint32_t QUERY_TIMEOUT;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(VIPSubscribeService);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_VIPSUBSCRIBESERVICE_H
