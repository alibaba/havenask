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
#include "aios/network/gig/multi_call/controller/RatioControllerBase.h"
#include "aios/network/gig/multi_call/controller/ControllerChain.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, RatioControllerBase);

RatioControllerBase::RatioControllerBase(const std::string &controllerName,
                                         float decStep)
    : ControllerBase(controllerName), _decStep(decStep) {}

float RatioControllerBase::update(ControllerFeedBack &feedBack) {
    assert(!feedBack.mirrorResponse);
    updateServerRatio(feedBack.stat);
    updateLocalRatio(feedBack.stat);
    if (hasServerAgent()) {
        if (!serverFilterReady()) {
            // have server agent and server not ready
            return _decStep;
        }
    } else if (!isValid()) {
        // do not have server agent, assume valid
        return 0.0f;
    }
    // have agent and server filter ready or
    // do not have agent and local filter ready
    if (isLegal(feedBack.bestChain, feedBack.metricLimits)) {
        return 0.0f;
    } else {
        return _decStep;
    }
}

void RatioControllerBase::updateServerRatio(const QueryResultStatistic &stat) {
    if (!stat.agentInfo) {
        clearServerValue();
        return;
    }
    setHasServerAgent(true);
    const auto &agentInfo = *stat.agentInfo;
    const auto &avgLatency = agentInfo.avg_latency();
    if (invalidServerVersion(avgLatency)) {
        return;
    }
    const ServerRatioFilter &serverRatioFilter =
        getServerRatioFilter(agentInfo);
    setServerFilterReady(serverRatioFilter.filter_ready());
    if (serverFilterReady()) {
        doUpdateServerRatio(serverRatioFilter);
        setServerValueVersion(avgLatency.version());
    } else {
        clearServerValue();
    }
}

void RatioControllerBase::updateServerRatioMirror(
    const QueryResultStatistic &stat) {
    if (!stat.validateMirror()) {
        return;
    }
    const auto &propagationStat = stat.getMirrorStat();
    const auto &avgLatency = propagationStat.latency();
    if (invalidServerVersion(avgLatency)) {
        return;
    }
    setServerFilterReady(avgLatency.filter_ready());
    if (serverFilterReady()) {
        const auto &serverRatioFilter = getMirrorServerRatioFilter(propagationStat);
        doUpdateServerRatio(serverRatioFilter);
        setServerValueVersion(avgLatency.version());
    } else {
        clearServerValue();
    }
}

void RatioControllerBase::doUpdateServerRatio(
    const ServerRatioFilter &serverRatioFilter) {
    setServerValue(serverRatioFilter.ratio());
    if (serverRatioFilter.has_load_balance_ratio()) {
        setLoadBalanceServerValue(serverRatioFilter.load_balance_ratio());
    } else {
        // gig-2.1 compatible logic
        setLoadBalanceServerValue(serverRatioFilter.ratio());
    }
}

} // namespace multi_call
