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
#ifndef MULTI_CALL_SERVICE_ISTIOSUBSCRIBERSERVICE_H
#define MULTI_CALL_SERVICE_ISTIOSUBSCRIBERSERVICE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/XdsClient.h"
#include "aios/network/gig/multi_call/subscribe/XdsStore.h"

namespace multi_call {

class IstioSubscribeService : public SubscribeService
{
public:
    IstioSubscribeService(const IstioConfig &config, const IstioMetricReporterPtr reporter);
    ~IstioSubscribeService();
    IstioSubscribeService(const IstioSubscribeService &) = delete;
    IstioSubscribeService &operator=(const IstioSubscribeService &) = delete;

    bool init() override;
    SubscribeType getType() override {
        return ST_ISTIO;
    }
    bool clusterInfoNeedUpdate() override;
    bool getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs) override;
    bool addSubscribe(const std::vector<std::string> &names) override;
    bool deleteSubscribe(const std::vector<std::string> &names) override;
    bool checkSubscribeWork() override;
    bool stopSubscribe() override;

private:
    IstioConfig _config;

    std::shared_ptr<XdsClient> _xdsClient;
    std::shared_ptr<XdsStore> _xdsStore;

    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(IstioSubscribeService);
} // namespace multi_call

#endif // MULTI_CALL_SERVICE_ISTIOSUBSCRIBERSERVICE_H
