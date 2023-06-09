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
#pragma once

#include <cstdint>
#include <limits>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"

namespace isearch {
namespace config {

class ServiceDegradationCondition : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationCondition() {
        workerQueueSizeDegradeThreshold = std::numeric_limits<uint32_t>::max();
        workerQueueSizeRecoverThreshold = 0;
        duration = 0;
    }
    ~ServiceDegradationCondition() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("worker_queue_size_degrade_threshold",
                     workerQueueSizeDegradeThreshold, workerQueueSizeDegradeThreshold);
        json.Jsonize("worker_queue_size_recover_threshold",
                     workerQueueSizeRecoverThreshold, workerQueueSizeRecoverThreshold);
        json.Jsonize("duration", duration, duration);
    }
public:
    uint32_t workerQueueSizeDegradeThreshold;
    uint32_t workerQueueSizeRecoverThreshold;
    int64_t duration; // ms
};

class ServiceDegradationRequest : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationRequest() {
        rankSize = 0;
        rerankSize = 0;
    }
    ~ServiceDegradationRequest() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("rank_size", rankSize, rankSize);
        json.Jsonize("rerank_size", rerankSize, rerankSize);
    }
public:
    uint32_t rankSize;
    uint32_t rerankSize;
};

class ServiceDegradationConfig : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationConfig() {
    }
    ~ServiceDegradationConfig() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("condition", condition, condition);
        json.Jsonize("request", request, request);
    }
public:
    ServiceDegradationCondition condition;
    ServiceDegradationRequest request;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ServiceDegradationConfig> ServiceDegradationConfigPtr;

} // namespace config
} // namespace isearch
