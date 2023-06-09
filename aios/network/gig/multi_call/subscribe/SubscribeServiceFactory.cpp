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
#include "aios/network/gig/multi_call/subscribe/SubscribeServiceFactory.h"
#include "aios/network/gig/multi_call/subscribe/CM2SubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/IstioSubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/LocalSubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/VIPSubscribeService.h"

using namespace std;

namespace multi_call {

SubscribeServicePtr
SubscribeServiceFactory::createCm2Sub(const Cm2Config &cm2Config) {
    return SubscribeServicePtr(new CM2SubscribeService(cm2Config));
}

SubscribeServicePtr
SubscribeServiceFactory::createVipSub(const VipConfig &vipConfig) {
    return SubscribeServicePtr(new VIPSubscribeService(vipConfig));
}

SubscribeServicePtr SubscribeServiceFactory::createIstioSub(
    const IstioConfig &istioConfig, const IstioMetricReporterPtr &reporter) {
    return SubscribeServicePtr(
        new IstioSubscribeService(istioConfig, reporter));
}

SubscribeServicePtr
SubscribeServiceFactory::createLocalSub(const LocalConfig &localConfig) {
    return SubscribeServicePtr(new LocalSubscribeService(localConfig));
}

} // namespace multi_call
