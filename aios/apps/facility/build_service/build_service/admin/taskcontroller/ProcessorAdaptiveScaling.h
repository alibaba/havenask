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

#include "build_service/admin/LocatorDetectStrategy.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ProcessorAdaptiveScaling
{
private:
    struct LoactorStatus {
        bool needScaling;
        int64_t reportTime;
        int64_t locator;
    };

public:
    ProcessorAdaptiveScaling(const config::ProcessorAdaptiveScalingConfig& config, const std::string& src,
                             bool adaptiveScalingFixedMax);
    ~ProcessorAdaptiveScaling() = default;

public:
    // return <partitionCount, parallelNum>
    std::optional<std::pair<uint32_t, uint32_t>> handle(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                                        uint32_t currentPartitionCount, uint32_t currentParallelNum);

private:
    std::optional<std::pair<uint32_t, uint32_t>>
    handleByCpu(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager);
    std::optional<std::pair<uint32_t, uint32_t>>
    handleByLocator(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager);
    int32_t getLocatorScaleOutFactor(size_t allWorkerCount);

private:
    const constexpr static int64_t LOCATOR_SPEED_THRESHOLD = 3;                    // 1秒能追3秒的数据
    const constexpr static int64_t LOCATOR_CHECK_INTERVAL = 30 * 1000 * 1000;      // 30s
    const constexpr static int64_t WORKER_START_THREADHOLD = 5 * 60 * 1000 * 1000; // 5min
    config::ProcessorAdaptiveScalingConfig _config;
    LocatorDetectStrategy _detector;
    std::string _src;
    bool _adaptiveScalingFixedMax = false;
    bool _canScaleDown = true;
    std::map<proto::Range, LoactorStatus> _lastDelayLocatorStatus;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorAdaptiveScaling);

}} // namespace build_service::admin
