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
#include "aios/network/gig/multi_call/controller/WarmUpController.h"

#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, WarmUpController);

float WarmUpController::update(ControllerFeedBack &feedBack) {
    if (!feedBack.stat.agentInfo) {
        return 0.0f;
    }
    const auto &agentInfo = *feedBack.stat.agentInfo;
    if (agentInfo.has_warm_up_status() && !agentInfo.warm_up_status().finished()) {
        _isFinished = false;
        return -MAX_WEIGHT_FLOAT;
    } else {
        _isFinished = true;
        return 0.0f;
    }
}

} // namespace multi_call
