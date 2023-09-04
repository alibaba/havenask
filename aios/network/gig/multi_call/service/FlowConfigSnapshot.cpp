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
#include "aios/network/gig/multi_call/service/FlowConfigSnapshot.h"

using namespace std;
using namespace autil::legacy::json;
using namespace autil::legacy;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, FlowConfigSnapshot);

static const std::string DEFAULT_FLOW_CONFIG_NAME = "default_flow_control_strategy";
FlowConfigSnapshot::FlowConfigSnapshot() {
    _configMap.reset(new FlowControlConfigMap());
    _defaultConfig.reset(new FlowControlConfig());
}

FlowConfigSnapshot::~FlowConfigSnapshot() {
}

void FlowConfigSnapshot::updateDefaultFlowConfig(const FlowControlConfigPtr &flowControlConfig) {
    if (flowControlConfig) {
        _defaultConfig = flowControlConfig;
    } else {
        _defaultConfig.reset(new FlowControlConfig());
    }
}

void FlowConfigSnapshot::updateFlowConfig(const std::string &strategy,
                                          const FlowControlConfigPtr &flowControlConfig) {
    if (!flowControlConfig) {
        _configMap->erase(strategy);
    } else {
        if (DEFAULT_FLOW_CONFIG_NAME == strategy) {
            _defaultConfig = flowControlConfig;
        } else {
            (*_configMap)[strategy] = flowControlConfig;
        }
    }
}

FlowConfigSnapshotPtr FlowConfigSnapshot::clone() {
    FlowConfigSnapshotPtr snapshot(new FlowConfigSnapshot());
    auto iter = _configMap->begin();
    for (; iter != _configMap->end(); ++iter) {
        FlowControlConfigPtr configPtr(iter->second->clone());
        (*(snapshot->_configMap))[iter->first] = configPtr;
    }
    snapshot->_defaultConfig = FlowControlConfigPtr(_defaultConfig->clone());
    return snapshot;
}

FlowControlConfigPtr FlowConfigSnapshot::getFlowControlConfig(const std::string &strategy) const {
    FlowControlConfigPtr config;
    if (getFlowControlConfig(strategy, config)) {
        return config;
    } else {
        return _defaultConfig;
    }
}

bool FlowConfigSnapshot::getFlowControlConfig(const std::string &strategy,
                                              FlowControlConfigPtr &config) const {
    auto it = _configMap->find(strategy);
    if (it != _configMap->end()) {
        config = it->second;
        return true;
    } else {
        return false;
    }
}

void FlowConfigSnapshot::getFlowControlSwitch(const vector<string> &strategyVec,
                                              bool &earlyTermination, bool &retry,
                                              bool &singleRetry) const {
    earlyTermination = false;
    retry = false;
    singleRetry = false;
    for (vector<string>::const_iterator it = strategyVec.begin(); it != strategyVec.end(); ++it) {
        const auto &strategy = *it;
        const auto &configPtr = getFlowControlConfig(strategy);
        if (configPtr) {
            earlyTermination = earlyTermination || configPtr->etEnabled();
            retry = retry || configPtr->retryEnabled();
            singleRetry = singleRetry || configPtr->singleRetryEnabled();
        }
    }
}

std::string FlowConfigSnapshot::toString() const {
    return ToJsonString(_configMap, true);
}

} // namespace multi_call
