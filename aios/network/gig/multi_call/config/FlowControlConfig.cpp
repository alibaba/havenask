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
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, FlowControlConfig);

FlowControlConfig *FlowControlConfig::clone() {
    return new FlowControlConfig(*this);
}

void FlowControlConfig::validate() {
    errorRatioLimit = getValidPercent(errorRatioLimit, "error_limit_ratio");
    etTriggerPercent = getValidPercent(etTriggerPercent, "et percent");
    retryTriggerPercent = getValidPercent(retryTriggerPercent, "retry percent");
    latencyTimeWindowSize = getValidInteger(latencyTimeWindowSize, "window_size");
    if (beginServerDegradeLatency > beginDegradeLatency) {
        AUTIL_LOG(ERROR,
                  "beginServerDegradeLatency [%u] is great than "
                  "beginDegradeLatency [%u]",
                  beginServerDegradeLatency, beginDegradeLatency);
        beginDegradeLatency = beginServerDegradeLatency;
    }
    if (beginDegradeLatency > fullDegradeLatency) {
        AUTIL_LOG(ERROR, "beginDegradeLatency [%u] is great than fullDegradeLatency [%u]",
                  beginDegradeLatency, fullDegradeLatency);
        fullDegradeLatency = beginDegradeLatency;
    }

    beginServerDegradeErrorRatio =
        getValidPercent(beginServerDegradeErrorRatio, "begin_server_degrade_error_ratio");
    beginDegradeErrorRatio = getValidPercent(beginDegradeErrorRatio, "begin_degrade_error_ratio");
    fullDegradeErrorRatio = getValidPercent(fullDegradeErrorRatio, "full_degrade_error_ratio");
    if (beginServerDegradeErrorRatio > beginDegradeErrorRatio) {
        AUTIL_LOG(ERROR,
                  "beginServerDegradeErrorRatio [%f] is great"
                  " than beginDegradeErrorRatio [%f]",
                  beginServerDegradeErrorRatio, beginDegradeErrorRatio);
        beginDegradeErrorRatio = beginServerDegradeErrorRatio;
    }
    if (beginDegradeErrorRatio > fullDegradeErrorRatio) {
        AUTIL_LOG(ERROR,
                  "beginDegradeErrorRatio [%f] is great "
                  "than fullDegradeErrorRatio [%f]",
                  beginDegradeErrorRatio, fullDegradeErrorRatio);
        fullDegradeErrorRatio = beginDegradeErrorRatio;
    }
}

bool FlowControlConfig::operator==(const FlowControlConfig &rhs) const {
    return probePercent == rhs.probePercent && errorRatioLimit == rhs.errorRatioLimit &&
           latencyUpperLimitPercent == rhs.latencyUpperLimitPercent &&
           latencyUpperLimitMs == rhs.latencyUpperLimitMs &&
           beginServerDegradeLatency == rhs.beginServerDegradeLatency &&
           beginDegradeLatency == rhs.beginDegradeLatency &&
           fullDegradeLatency == rhs.fullDegradeLatency &&
           etTriggerPercent == rhs.etTriggerPercent && etWaitTimeFactor == rhs.etWaitTimeFactor &&
           etMinWaitTime == rhs.etMinWaitTime && retryTriggerPercent == rhs.retryTriggerPercent &&
           retryWaitTimeFactor == rhs.retryWaitTimeFactor &&
           beginServerDegradeErrorRatio == rhs.beginServerDegradeErrorRatio &&
           beginDegradeErrorRatio == rhs.beginDegradeErrorRatio &&
           fullDegradeErrorRatio == rhs.fullDegradeErrorRatio;
}

} // namespace multi_call
