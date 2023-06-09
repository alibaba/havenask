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
#include "aios/network/gig/multi_call/subscribe/IstioSubscribeService.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, IstioSubscribeService);

IstioSubscribeService::IstioSubscribeService(
    const IstioConfig &config, const IstioMetricReporterPtr reporter) {

    _xdsStore = make_shared<XdsStore>(config, reporter);
    _xdsClient = make_shared<XdsClient>(config, reporter, _xdsStore);
}

IstioSubscribeService::~IstioSubscribeService() {
    bool stopped = stopSubscribe();
    while (!stopped) {
        stopped = stopSubscribe();
        AUTIL_LOG(WARN, "stop subscribe failed.");
    }
}

bool IstioSubscribeService::init() {
    if (!_xdsClient->init()) {
        return false;
    }
    if (!_xdsStore->init()) {
        return false;
    }
    return true;
};

bool IstioSubscribeService::clusterInfoNeedUpdate() {
    // first check in client if there is full data.
    if (!_xdsClient->isSynced()) {
        AUTIL_LOG(WARN, "XdsClient is not synced.");
        if (!_xdsStore->isServeable()) {
            AUTIL_LOG(WARN, "_xdsStore is not ready, maybe failed to load from "
                            "cache, can not clusterInfoNeedUpdate.");
            return false;
        }
    }
    return _xdsStore->clusterInfoNeedUpdate();
};

bool IstioSubscribeService::getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs) {
    // first check in client if there is full data.
    if (_xdsClient->isSynced() == false) {
        AUTIL_LOG(WARN, "XdsClient is not synced.");
        if (!_xdsStore->isServeable()) {
            AUTIL_LOG(WARN, "_xdsStore is not ready, maybe failed to load from "
                            "cache, can not getClusterInfoMap.");
            return false;
        }
    }
    return _xdsStore->getClusterInfoMap(&topoNodeVec);
};

bool IstioSubscribeService::addSubscribe(
    const std::vector<std::string> &names) {
    return _xdsClient->addSubscribe(names);
};
bool IstioSubscribeService::deleteSubscribe(
    const std::vector<std::string> &names) {
    return _xdsClient->deleteSubscribe(names);
};

bool IstioSubscribeService::checkSubscribeWork() {
    return _xdsClient->checkSubscribeWork();
};

bool IstioSubscribeService::stopSubscribe() {
    return _xdsClient->stopSubscribe() && _xdsStore->stop();
};

} // namespace multi_call
