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

#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"

namespace anet {

class ANetMetricReporterConfig : public autil::legacy::Jsonizable {
public:
    ANetMetricReporterConfig(bool enable = false) : enableANetMetric(enable) {
        enableANetMetric = autil::EnvUtil::getEnv("ANET_METRIC_ENABLE", enableANetMetric);
        intervalMs = autil::EnvUtil::getEnv("ANET_METRIC_INTERVAL_MS", intervalMs);
    }
    ~ANetMetricReporterConfig() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("enable_anet_metric", enableANetMetric, enableANetMetric);
        json.Jsonize("interval_ms", intervalMs, intervalMs);
    }

public:
    bool enableANetMetric{false};
    int64_t intervalMs{1000};
};

} // namespace anet