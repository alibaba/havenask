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
#include "aios/network/gig/multi_call/common/ControllerParam.h"

#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, ControllerParam);

float ControllerParam::WEIGHT_UPDATE_STEP = MIN_WEIGHT_UPDATE_STEP_FLOAT;
int64_t ControllerParam::HEARTBEAT_UNHEALTH_INTERVAL =
    DEFAULT_HEARTBEAT_UNHEALTH_INTERVAL * FACTOR_US_TO_MS;

void ControllerParam::logParam() {
    JsonMap paramMap;
    paramMap["MAX_WEIGHT"] = Any(MAX_WEIGHT);
    paramMap["MIN_WEIGHT"] = Any(MIN_WEIGHT);
    paramMap["MAX_WEIGHT_FLOAT"] = Any(MAX_WEIGHT_FLOAT);
    paramMap["MIN_WEIGHT_FLOAT"] = Any(MIN_WEIGHT_FLOAT);
    paramMap["MIN_INTERNAL_TARGET_WEIGHT_FLOAT"] = Any(MIN_INTERNAL_TARGET_WEIGHT_FLOAT);
    paramMap["MAX_WEIGHT_UPDATE_STEP_FLOAT"] = Any(MAX_WEIGHT_UPDATE_STEP_FLOAT);
    paramMap["MIN_WEIGHT_UPDATE_STEP_FLOAT"] = Any(MIN_WEIGHT_UPDATE_STEP_FLOAT);
    paramMap["WEIGHT_DEC_LIMIT"] = Any(WEIGHT_DEC_LIMIT);
    paramMap["MAX_PROBE_PERCENT"] = Any(MAX_PROBE_PERCENT);
    paramMap["FILTER_INIT_LIMIT"] = Any(FILTER_INIT_LIMIT);
    paramMap["LOAD_BALANCE_FILTER_INIT_LIMIT"] = Any(LOAD_BALANCE_FILTER_INIT_LIMIT);
    paramMap["NET_LATENCY_FILTER_INIT_LIMIT"] = Any(NET_LATENCY_FILTER_INIT_LIMIT);
    paramMap["ERROR_RATIO_WEIGHT_DEC_STEP"] = Any(ERROR_RATIO_WEIGHT_DEC_STEP);
    paramMap["DEGRADE_RATIO_WEIGHT_DEC_STEP"] = Any(DEGRADE_RATIO_WEIGHT_DEC_STEP);
    paramMap["LATENCY_WEIGHT_DEC_STEP"] = Any(LATENCY_WEIGHT_DEC_STEP);
    paramMap["DEFAULT_PROBE_PERCENT"] = Any(DEFAULT_PROBE_PERCENT);
    paramMap["DEFAULT_LATENCY_UPPER_LIMIT_PERCENT"] = Any(DEFAULT_LATENCY_UPPER_LIMIT_PERCENT);
    paramMap["DEFAULT_LATENCY_UPPER_LIMIT_MS"] = Any(DEFAULT_LATENCY_UPPER_LIMIT_MS);
    paramMap["MAX_PERCENT"] = Any(MAX_PERCENT);
    paramMap["MIN_PERCENT"] = Any(MIN_PERCENT);
    paramMap["DEFAULT_DEGRADE_LATENCY"] = Any(DEFAULT_DEGRADE_LATENCY);
    paramMap["DEFAULT_WAIT_TIME_FACTOR"] = Any(DEFAULT_WAIT_TIME_FACTOR);
    paramMap["INVALID_FILTER_VALUE"] = Any(INVALID_FILTER_VALUE);
    paramMap["MIN_RATIO"] = Any(MIN_RATIO);
    paramMap["MAX_RATIO"] = Any(MAX_RATIO);
    paramMap["DEGRADE_RATIO_TOLERANCE"] = Any(DEGRADE_RATIO_TOLERANCE);
    paramMap["FACTOR_US_TO_MS"] = Any(FACTOR_US_TO_MS);
    paramMap["INVALID_FLOAT_OUTPUT_VALUE"] = Any(INVALID_FLOAT_OUTPUT_VALUE);
    paramMap["MAX_BEST_CHAIN_QUEUE_SIZE"] = Any(MAX_BEST_CHAIN_QUEUE_SIZE);
    paramMap["SATURATION_FACTOR"] = Any(SATURATION_FACTOR);
    paramMap["WEIGHT_UPDATE_STEP"] = Any(WEIGHT_UPDATE_STEP);
    paramMap["HEARTBEAT_UNHEALTH_INTERVAL"] = Any(HEARTBEAT_UNHEALTH_INTERVAL / FACTOR_US_TO_MS);
    AUTIL_LOG(INFO, "ControllerParam:\n%s", ToJsonString(paramMap, true).c_str());
}

} // namespace multi_call
