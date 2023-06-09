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
#include "aios/network/gig/multi_call/controller/ErrorRatioController.h"
#include "aios/network/gig/multi_call/controller/ControllerChain.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ErrorRatioController);

ErrorRatioController::ErrorRatioController()
    : RatioControllerBase("errorRatio", ERROR_RATIO_WEIGHT_DEC_STEP) {}

void ErrorRatioController::updateLocalRatio(const QueryResultStatistic &stat) {
    float isFailed = stat.isFailed() ? MAX_RATIO : MIN_RATIO;
    updateLocal(isFailed);
}

const ServerRatioFilter &ErrorRatioController::getServerRatioFilter(
    const GigResponseInfo &agentInfo) const {
    return agentInfo.error_ratio();
}

const ServerRatioFilter &ErrorRatioController::getMirrorServerRatioFilter(
    const PropagationStatDef &stat) const {
    return stat.error();
}

bool ErrorRatioController::isLegal(const ControllerChain *bestChain,
                                   const MetricLimits &metricLimits) const {
    if (!bestChain) {
        return true;
    }
    const auto &best = bestChain->errorRatioController;
    if (&best == this) {
        return true;
    }
    auto allow = metricLimits.errorRatioLimit * MAX_RATIO;
    auto flag = compare(best, allow);
    return WorseCompareFlag != flag;
}

float ErrorRatioController::controllerOutput() const {
    if (serverValueReady()) {
        return serverValue();
    } else {
        return localValue();
    }
}

float ErrorRatioController::getLegalLimit(
    const FlowControlConfigPtr &flowControlConfig) const {
    if (!isValid()) {
        return INVALID_FILTER_VALUE;
    }
    auto output = controllerOutput();
    return output + flowControlConfig->errorRatioLimit * MAX_RATIO;
}

void ErrorRatioController::updateLoadBalance(ControllerFeedBack &feedBack) {
    if (!hasServerAgent()) {
        return;
    }
    if (feedBack.mirrorResponse) {
        updateServerRatioMirror(feedBack.stat);
    }
}

} // namespace multi_call
