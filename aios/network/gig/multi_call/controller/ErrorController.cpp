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
#include "aios/network/gig/multi_call/controller/ErrorController.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ErrorController);

ErrorController::ErrorController() {
}

ErrorController::~ErrorController() {
}

float ErrorController::update(ControllerFeedBack &feedBack) {
    if (feedBack.stat.isFailed()) {
        return -min(2.0f * ControllerParam::WEIGHT_UPDATE_STEP, MAX_WEIGHT_UPDATE_STEP_FLOAT);
    } else {
        return 0.0f;
    }
}

} // namespace multi_call
