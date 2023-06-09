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
#include "aios/network/gig/multi_call/controller/DegradeRatioController.h"
#include "aios/network/gig/multi_call/controller/ControllerChain.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include <math.h>

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, DegradeRatioController);

DegradeRatioController::DegradeRatioController()
    : RatioControllerBase("degradeRatio", DEGRADE_RATIO_WEIGHT_DEC_STEP) {}

void DegradeRatioController::updateLocalRatio(
    const QueryResultStatistic &stat) {
    float degrade = stat.isDecWeight() ? MAX_RATIO : MIN_RATIO;
    updateLocal(degrade);
}

const ServerRatioFilter &DegradeRatioController::getServerRatioFilter(
    const GigResponseInfo &agentInfo) const {
    return agentInfo.degrade_ratio();
}

const ServerRatioFilter &DegradeRatioController::getMirrorServerRatioFilter(
    const PropagationStatDef &stat) const {
    return stat.degrade();
}

bool DegradeRatioController::isLegal(const ControllerChain *bestChain,
                                     const MetricLimits &metricLimits) const {
    if (!bestChain) {
        return true;
    }
    const auto &best = bestChain->degradeRatioController;
    if (&best == this) {
        return true;
    }
    // compare is valid
    auto flag = compare(best, DEGRADE_RATIO_TOLERANCE);
    return WorseCompareFlag != flag;
}

float DegradeRatioController::controllerOutput() const {
    if (serverValueReady()) {
        auto ret = serverValue();
        if (ret != INVALID_FILTER_VALUE) {
            return ret;
        }
    }
    return localValue();
}

float DegradeRatioController::controllerLoadBalanceOutput() const {
    if (serverValueReady()) {
        auto ret = loadBalanceServerValue();
        if (ret != INVALID_FILTER_VALUE) {
            return ret;
        }
    }
    return INVALID_FILTER_VALUE;
}

float DegradeRatioController::getLegalLimit() const {
    if (!serverValueReady()) {
        return INVALID_FILTER_VALUE;
    }
    auto output = serverValue();
    return output + DEGRADE_RATIO_TOLERANCE;
}

float DegradeRatioController::updateLoadBalance(ControllerFeedBack &feedBack) {
    if (!hasServerAgent()) {
        return MAX_PERCENT;
    }
    if (feedBack.mirrorResponse) {
        updateServerRatioMirror(feedBack.stat);
    }
    if (!serverFilterReady()) {
        return MAX_PERCENT;
    }
    float bestRatio = INVALID_FILTER_VALUE;
    if (!getBestRatio(feedBack, bestRatio)) {
        return MAX_PERCENT;
    }
    auto returnPercent =
        calculateWeightPercent(controllerLoadBalanceOutput(), bestRatio);
    return std::max(LOAD_BALANCE_MIN_PERCENT, returnPercent);
}

bool DegradeRatioController::getBestRatio(const ControllerFeedBack &feedBack,
                                          float &bestRatio) const {
    if (!feedBack.bestChain) {
        return false;
    }
    const auto &bestController = feedBack.bestChain->degradeRatioController;
    if (&bestController == this || !bestController.isValid()) {
        return false;
    }
    if (INVALID_FILTER_VALUE != feedBack.bestLoadBalanceDegradeRatio) {
        bestRatio = feedBack.bestLoadBalanceDegradeRatio;
    } else {
        bestRatio = bestController.controllerLoadBalanceOutput();
    }
    return true;
}

float DegradeRatioController::calculateWeightPercent(float thisRatio,
                                                     float bestRatio) {
    if (INVALID_FILTER_VALUE == thisRatio ||
        INVALID_FILTER_VALUE == bestRatio || thisRatio <= bestRatio) {
        return MAX_PERCENT;
    } else {
        return std::max(MIN_PERCENT,
                        (MAX_RATIO + bestRatio - thisRatio) / MAX_RATIO);
    }
}

} // namespace multi_call
