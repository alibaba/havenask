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
#include "aios/network/gig/multi_call/controller/ControllerBase.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ControllerBase);

ControllerBase::ControllerBase(const std::string &controllerName)
    : _controllerName(controllerName), _hasServerAgent(false),
      _serverFilterReady(false), _localFilter(FILTER_INIT_LIMIT),
      _serverValueVersion(0), _serverValueDelay(INVALID_FILTER_VALUE),
      _serverValue(INVALID_FILTER_VALUE),
      _loadBalanceServerValue(INVALID_FILTER_VALUE),
      _serverAvgWeight(INVALID_FILTER_VALUE) {}

ControllerBase::~ControllerBase() {}

bool ControllerBase::isValid() const {
    return _localFilter.isValid() || serverValueReady();
}

int64_t ControllerBase::count() const { return _localFilter.count(); }

bool ControllerBase::hasServerAgent() const { return _hasServerAgent; }

void ControllerBase::setHasServerAgent(bool value) { _hasServerAgent = value; }

bool ControllerBase::serverFilterReady() const { return _serverFilterReady; }

void ControllerBase::setServerValueVersion(int64_t version) {
    if (_serverValueVersion > 0) {
        _serverValueDelay = std::max(0l, version - _serverValueVersion);
    }
    _serverValueVersion = version;
}

int64_t ControllerBase::serverValueVersion() const {
    return _serverValueVersion;
}

float ControllerBase::serverValueDelayMs() const {
    auto delay = _serverValueDelay;
    if (INVALID_FILTER_VALUE != delay) {
        return delay / FACTOR_US_TO_MS;
    } else {
        return INVALID_FILTER_VALUE;
    }
}

void ControllerBase::setServerFilterReady(bool value) {
    _serverFilterReady = value;
}

void ControllerBase::setServerValue(float value) { _serverValue = value; }

void ControllerBase::clearServerValue() {
    setServerValue(INVALID_FILTER_VALUE);
    setLoadBalanceServerValue(INVALID_FILTER_VALUE);
    _serverValueDelay = INVALID_FILTER_VALUE;
    setServerAvgWeight(INVALID_FILTER_VALUE);
}

bool ControllerBase::serverValueReady() const {
    return INVALID_FILTER_VALUE != _serverValue;
}

float ControllerBase::serverValue() const { return _serverValue; }

void ControllerBase::setLoadBalanceServerValue(float value) {
    _loadBalanceServerValue = value;
}

float ControllerBase::loadBalanceServerValue() const {
    return _loadBalanceServerValue;
}

void ControllerBase::setServerAvgWeight(float weight) {
    _serverAvgWeight = weight;
}

float ControllerBase::serverAvgWeight() const { return _serverAvgWeight; }

void ControllerBase::updateLocal(float value) { _localFilter.update(value); }

float ControllerBase::localValue() const { return _localFilter.output(); }

bool ControllerBase::needProbe() const {
    return (count() < 10) || (hasServerAgent() && !serverFilterReady());
}

CompareFlag ControllerBase::compare(const ControllerBase &rhs,
                                    float allow) const {
    int32_t sum = isValid() | (rhs.isValid() << 1u);
    switch (sum) {
    case 2:
        return WorseCompareFlag;
    case 1:
        return BetterCompareFlag;
    case 3: {
        auto thisOut = controllerOutput();
        auto rhsOut = rhs.controllerOutput();
        if (thisOut <= (rhsOut + allow) && thisOut >= (rhsOut - allow)) {
            return UnknownCompareFlag;
        } else {
            return CompareFlag(thisOut < rhsOut);
        }
    }
    case 0:
    default:
        return UnknownCompareFlag;
    }
}

bool ControllerBase::invalidServerVersion(
    const AverageLatency &avgLatency) const {
    return avgLatency.has_version() && // gig-2.1 compatible logic
           serverValueVersion() >= avgLatency.version();
}

} // namespace multi_call
