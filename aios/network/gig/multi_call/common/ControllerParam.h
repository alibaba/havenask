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
#ifndef ISEARCH_MULTI_CALL_CONTROLLERPARAM_H
#define ISEARCH_MULTI_CALL_CONTROLLERPARAM_H

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/TimeUtility.h"

namespace multi_call {

// const
constexpr WeightTy MAX_WEIGHT = 100;
constexpr WeightTy MIN_WEIGHT = 0;
constexpr float MAX_WEIGHT_FLOAT = 100.0f;
constexpr float MIN_WEIGHT_FLOAT = 0.0f;
constexpr float MIN_INTERNAL_TARGET_WEIGHT_FLOAT = 10.0f;

constexpr float MAX_WEIGHT_UPDATE_STEP_FLOAT = 3.0f;
constexpr float MIN_WEIGHT_UPDATE_STEP_FLOAT = 1.0f;
constexpr float WEIGHT_DEC_LIMIT = 7.0f;
constexpr int64_t MAX_PROBE_PERCENT = 5;
constexpr float LOAD_BALANCE_MIN_PERCENT = 0.1f;

// filter window size
constexpr int32_t FILTER_INIT_LIMIT = 300;
constexpr int32_t LOAD_BALANCE_FILTER_INIT_LIMIT = 200;
constexpr int32_t NET_LATENCY_FILTER_INIT_LIMIT = 40;
constexpr int32_t BEST_FILTER_INIT_LIMIT = 100;

// dec weight step
constexpr float ERROR_RATIO_WEIGHT_DEC_STEP = -3.0f;
constexpr float DEGRADE_RATIO_WEIGHT_DEC_STEP = -4.0f;
constexpr float LATENCY_WEIGHT_DEC_STEP = -3.0f;

// default values
// percent
constexpr float DEFAULT_PROBE_PERCENT = 0.05f;
constexpr float DEFAULT_LATENCY_UPPER_LIMIT_PERCENT = 10000000.0f;
constexpr float DEFAULT_LATENCY_UPPER_LIMIT_MS = 10000000.0f;
constexpr float MAX_PERCENT = 1.0f;
constexpr float MIN_PERCENT = 0.0f;

// ms
constexpr uint32_t DEFAULT_DEGRADE_LATENCY = 1000000;
constexpr uint32_t DEFAULT_WAIT_TIME_FACTOR = 1000;

constexpr float LOAD_BALANCE_TIME_FACTOR_MS = 200.0f;

constexpr float INVALID_FILTER_VALUE = 1e20f; // very large value

constexpr RatioTy MIN_RATIO = 0;
constexpr RatioTy MAX_RATIO = 100;

constexpr float DEGRADE_RATIO_TOLERANCE = 10.0f;
constexpr float ERROR_RATIO_TOLERANCE = 10.0f;

constexpr float FACTOR_US_TO_MS = 1000.0f;

constexpr float INVALID_FLOAT_OUTPUT_VALUE = -1.0f;

constexpr size_t MAX_BEST_CHAIN_QUEUE_SIZE = 10u;

constexpr float SATURATION_FACTOR = 3.0f;

constexpr int64_t FACTOR_S_TO_US = 1000000;

constexpr float EPSILON = 1e-6f;

constexpr int32_t PAYLOAD_SIZE = 10;

constexpr int32_t DEFAULT_HEARTBEAT_UNHEALTH_INTERVAL = 20 * 1000; // 20s

constexpr int32_t DEFAULT_RETRY_LIMIT_PER_SECOND = -1; // no limit
constexpr int32_t DEFAULT_LATENCY_TIME_WINDOW_SIZE = 0;

struct ControllerParam {
public:
    static void logParam();

public:
    // runtime
    static float WEIGHT_UPDATE_STEP;
    static int64_t HEARTBEAT_UNHEALTH_INTERVAL;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONTROLLERPARAM_H
