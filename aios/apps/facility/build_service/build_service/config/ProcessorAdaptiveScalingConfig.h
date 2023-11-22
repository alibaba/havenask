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

#include <stddef.h>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

/*
  "processor_rule_config" : {
      "adaptive_scaling_config" :
      {
          "max_count" : 128,
          "enable_adaptive_scaling" : true,
          "cpu_adjust_strategy" : {
              "enabled" : true,
              "expect_max_ratio" : 80,
              "expect_min_ratio" : 40,
              "sample_window_in_sec" : 3600 //单位s，过去1h内cpu不在预期ratio中会触发调整
          },
          "latency_adjust_strategy" : {
              "enabled" : true,
              "gap_in_sec" : 600,    // 单位s
              "percent" : 30  // 比例，如超过30%的Processor超过gap，则触发调整
          }
      }
    }
*/

struct ProcessorAdaptiveScalingConfig : public autil::legacy::Jsonizable {
    struct CpuStragegy : public autil::legacy::Jsonizable {
        size_t cpuMaxRatio = 80;
        size_t cpuMinRatio = 40;
        size_t sampleWindow = 3600; // 1h
        bool enabled = true;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("expect_min_ratio", cpuMinRatio, cpuMinRatio);
            json.Jsonize("expect_max_ratio", cpuMaxRatio, cpuMaxRatio);
            json.Jsonize("sample_window_in_sec", sampleWindow, sampleWindow);
            json.Jsonize("enabled", enabled, enabled);
        }
        bool validate() const
        {
            if (cpuMaxRatio > 100 || cpuMinRatio > cpuMaxRatio) {
                AUTIL_LOG(ERROR, "cpu ratio min [%zu] or max [%zu] is invalid", cpuMinRatio, cpuMaxRatio);
                return false;
            }
            if (sampleWindow == 0) {
                AUTIL_LOG(ERROR, "sample window should more than 0");
                return false;
            }
            return true;
        }
    };
    struct LatencyStrategy : public autil::legacy::Jsonizable {
        size_t latencyGap = 600; // 600s
        size_t delayedWorkerPercent = 30;
        bool enabled = true;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("gap_in_sec", latencyGap, latencyGap);
            json.Jsonize("delayed_worker_percent", delayedWorkerPercent, delayedWorkerPercent);
            json.Jsonize("enabled", enabled, enabled);
        }
        bool validate() const
        {
            if (delayedWorkerPercent > 100) {
                AUTIL_LOG(ERROR, "percent [%zu] is invalid", delayedWorkerPercent);
                return false;
            }
            return true;
        }
    };

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;

    size_t maxCount = 0;
    bool enableAdaptiveScaling = false;
    CpuStragegy cpuStrategy;
    LatencyStrategy latencyStrategy;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorAdaptiveScalingConfig);

}} // namespace build_service::config
