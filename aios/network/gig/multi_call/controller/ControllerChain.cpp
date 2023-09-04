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
#include "aios/network/gig/multi_call/controller/ControllerChain.h"

using namespace std;
using namespace autil;

namespace multi_call {

void ControllerChain::toString(std::string &ret) const {
    auto errorLocal = trimSmall(errorRatioController.localValue());
    auto errorServer = INVALID_FLOAT_OUTPUT_VALUE;
    auto lbError = INVALID_FLOAT_OUTPUT_VALUE;
    if (errorRatioController.serverValueReady()) {
        errorServer = trimSmall(errorRatioController.serverValue());
        lbError = trimSmall(errorRatioController.loadBalanceServerValue());
    }

    auto degradeLocal = trimSmall(degradeRatioController.localValue());
    auto degradeServer = INVALID_FLOAT_OUTPUT_VALUE;
    auto lbDegrade = INVALID_FLOAT_OUTPUT_VALUE;
    if (degradeRatioController.serverValueReady()) {
        degradeServer = trimSmall(degradeRatioController.serverValue());
        lbDegrade = trimSmall(degradeRatioController.loadBalanceServerValue());
    }

    auto latencyLocal = trimSmall(latencyController.localValue());
    auto latencyServerSum = INVALID_FLOAT_OUTPUT_VALUE;
    auto loadBalanceLatencyServer = INVALID_FLOAT_OUTPUT_VALUE;
    auto loadBalanceLatencyServerSum = INVALID_FLOAT_OUTPUT_VALUE;
    auto netLatency = trimSmall(latencyController.netLatency());
    auto delayMs = latencyController.serverValueDelayMs();
    auto serverAvgWeight = latencyController.serverAvgWeight();
    if (INVALID_FILTER_VALUE == delayMs) {
        delayMs = INVALID_FLOAT_OUTPUT_VALUE;
    }
    if (INVALID_FILTER_VALUE == serverAvgWeight) {
        serverAvgWeight = INVALID_FLOAT_OUTPUT_VALUE;
    }
    if (latencyController.serverValueReady()) {
        latencyServerSum = trimSmall(netLatency + latencyController.serverValue());
        loadBalanceLatencyServer = latencyController.loadBalanceServerValue();
        loadBalanceLatencyServerSum = trimSmall(netLatency + loadBalanceLatencyServer);
    }

    ret += "S: ";
    ret += StringUtil::fToString(latencyServerSum);
    ret += " LatSum ";
    ret += StringUtil::fToString(errorServer);
    ret += " E ";
    ret += StringUtil::fToString(degradeServer);
    ret += " D ";
    ret += StringUtil::fToString(delayMs);
    ret += " Delay ";
    ret += StringUtil::fToString(serverAvgWeight);
    ret += " serverWeight ";
    ret += StringUtil::toString(targetWeight);
    ret += " targetWeight";
    ret += ", LBS: ";
    ret += StringUtil::fToString(loadBalanceLatencyServerSum);
    ret += " LBLatSum ";
    ret += StringUtil::fToString(loadBalanceLatencyServer);
    ret += " LBLat ";
    ret += StringUtil::fToString(lbError);
    ret += " LBE ";
    ret += StringUtil::fToString(lbDegrade);
    ret += " LBD";
    ret += ", L: ";
    ret += StringUtil::fToString(latencyLocal);
    ret += " Lat ";
    ret += StringUtil::fToString(errorLocal);
    ret += " E ";
    ret += StringUtil::fToString(degradeLocal);
    ret += " D";
    ret += ", netLat: ";
    ret += StringUtil::fToString(netLatency);
    ret += ", queryCount: ";
    ret += StringUtil::toString(errorRatioController.count());
    ret += ", hasServer: ";
    ret += StringUtil::toString(latencyController.hasServerAgent());
}

float ControllerChain::trimSmall(float value) {
    return value < 1e-3f ? 0.0f : value;
}

} // namespace multi_call
