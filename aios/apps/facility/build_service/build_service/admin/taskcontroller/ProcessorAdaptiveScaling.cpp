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
#include "build_service/admin/taskcontroller/ProcessorAdaptiveScaling.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "build_service/admin/CpuSpeedDetectStrategy.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/WorkerNode.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(taskcontroller, ProcessorAdaptiveScaling);

ProcessorAdaptiveScaling::ProcessorAdaptiveScaling(const config::ProcessorAdaptiveScalingConfig& config,
                                                   const std::string& src, bool adaptiveScalingFixedMax)
    : _config(config)
    , _detector(LocatorDetectStrategy::CURRENT_MICROSECOND_CONDITION, config.latencyStrategy.latencyGap * 1000 * 1000)
    , _src(src)
    , _adaptiveScalingFixedMax(adaptiveScalingFixedMax)
{
    BS_LOG(INFO, "processor adaptive scaling start, adaptiveScalingFixedMax [%d], maxCount [%zu]",
           _adaptiveScalingFixedMax, _config.maxCount);
}

std::optional<std::pair<uint32_t, uint32_t>>
ProcessorAdaptiveScaling::handleByLocator(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager)
{
    auto latencyStrategy = _config.latencyStrategy;
    if (_src != config::SWIFT_READ_SRC || !latencyStrategy.enabled) {
        return std::nullopt;
    }
    const auto& runningNodes = nodeStatusManager->GetRunningNodes({});
    size_t delayedWorkerCount = 0;
    size_t judgeWorkerCount = 0;
    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    for (auto& [_, nodeMap] : runningNodes) {
        if (nodeMap.empty()) {
            continue;
        }
        assert(1 == nodeMap.size());
        const auto& [__, nodeStatus] = *nodeMap.cbegin();
        std::string workIdentifier = nodeStatus->roleName + "@" + nodeStatus->ip;
        if (nodeStatus->workflowErrorExceedThreadhold) {
            BS_LOG(INFO, "node [%s] workFlow error exceed threadhold, will not judge", workIdentifier.c_str());
            continue;
        }

        const auto& node = nodeStatus->GetWorkerNode<proto::ProcessorNodes>();
        if (node->isFinished() || node->isSuspended() || nodeStatus->nodeStartedTime == -1) {
            continue;
        }

        // 资源池场景，worker启动很快，退避不查的等待时间也缩短
        int64_t ignoreCheckTime = WORKER_START_THREADHOLD;
        int64_t workerStartTime = nodeStatus->nodeStartedTime - nodeStatus->firstCreatedTime;
        if (workerStartTime > 0 && workerStartTime < WORKER_START_THREADHOLD) {
            ignoreCheckTime = workerStartTime;
        }
        if (currentTime - nodeStatus->nodeStartedTime < ignoreCheckTime) {
            BS_INTERVAL_LOG(100, INFO, "node [%s] just start [%ld], will not judge locator", workIdentifier.c_str(),
                            nodeStatus->nodeStartedTime);
            continue;
        }

        ++judgeWorkerCount;
        const auto& range = node->getPartitionId().range();
        auto reportTime = nodeStatus->lastReportTime;
        int64_t locatorOffset = nodeStatus->locatorOffset;

        if (_detector.detect(locatorOffset, workIdentifier)) {
            _canScaleDown = false;
            // 存在延迟并且延迟在近段时间下降速率不及预期，才会进行干预
            if (!_lastDelayLocatorStatus.count(range)) {
                _lastDelayLocatorStatus[range] = {false, reportTime, locatorOffset};
            } else {
                auto [needScaling, lastReport, lastLocator] = _lastDelayLocatorStatus[range];
                // // 当前时间距离上次时间过短，无法判断locator的减低，以上次状态为准
                if (reportTime - lastReport >= LOCATOR_CHECK_INTERVAL && locatorOffset > lastLocator) {
                    // 这里要求locator有进展才进行判断，在消息被正常消费但是下游没commit的情况下locator是可能不变的，这种情况会误判
                    double speed = 1.0 * (locatorOffset - lastLocator) / (reportTime - lastReport);
                    needScaling = speed < LOCATOR_SPEED_THRESHOLD ? true : false;
                    _lastDelayLocatorStatus[range] = {needScaling, reportTime, locatorOffset};
                }
                if (needScaling) {
                    delayedWorkerCount++;
                    BS_LOG(WARN, "worker is [%s] delayed as last locator,ts [%ld, %ld], now locator, ts[%ld, %ld]",
                           workIdentifier.c_str(), lastLocator, lastReport, locatorOffset, reportTime);
                }
            }
        } else {
            auto iter = _lastDelayLocatorStatus.find(range);
            if (iter != _lastDelayLocatorStatus.end()) {
                _lastDelayLocatorStatus.erase(iter);
            }
        }
    }

    size_t allWorkerCount = nodeStatusManager->GetAllNodeGroups().size();

    if (latencyStrategy.delayedWorkerPercent == 0) {
        if (delayedWorkerCount > 0) {
            BS_LOG(INFO, "delayedWorkerCount [%lu], more than work count [%lu] percent [%zu], scale out to [%ld]",
                   delayedWorkerCount, allWorkerCount, latencyStrategy.delayedWorkerPercent, _config.maxCount);
            return std::make_pair(_config.maxCount, 1);
        }
        return std::nullopt;
    }

    if (delayedWorkerCount * 100 >= latencyStrategy.delayedWorkerPercent * allWorkerCount) {
        BS_LOG(INFO, "delayedWorkerCount [%lu], more than work count [%lu] percent [%zu], scale out to [%ld]",
               delayedWorkerCount, allWorkerCount, latencyStrategy.delayedWorkerPercent, _config.maxCount);
        return std::make_pair(_config.maxCount, 1);
    }
    BS_INTERVAL_LOG(150, INFO,
                    "processor locator scaling, do nothing, allWorkerCount [%lu], delayedWorkerCount [%lu], "
                    "judgeWorkerCount [%lu]",
                    allWorkerCount, delayedWorkerCount, judgeWorkerCount);
    return std::nullopt;
}

std::optional<std::pair<uint32_t, uint32_t>>
ProcessorAdaptiveScaling::handleByCpu(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager)
{
    if (_src != config::SWIFT_READ_SRC || !_config.cpuStrategy.enabled) {
        return std::nullopt;
    }

    std::string key = "cpu_speed";
    std::vector<int64_t> allCpuSpeed;
    const auto& runningNodes = nodeStatusManager->GetRunningNodes();
    nodeStatusManager->GetStartedNodesAttr(runningNodes, key, &allCpuSpeed);
    CpuSpeedDetectStrategy strategy(allCpuSpeed, CpuSpeedDetectStrategy::CPU_FIXED_THREADHOLD);
    size_t validWorkerCount = 0;
    size_t totalCPURatio = 0;
    for (auto& [_, nodeMap] : runningNodes) {
        if (nodeMap.empty()) {
            continue;
        }
        assert(1 == nodeMap.size());
        const auto& [__, nodeStatus] = *nodeMap.cbegin();
        const auto& node = nodeStatus->GetWorkerNode<proto::ProcessorNodes>();
        if (node->isFinished() || node->isSuspended()) {
            continue;
        }
        CpuSpeedDetectStrategy::Node nodeInfo(nodeStatus->cpuSpeed, nodeStatus->ip, nodeStatus->roleName);
        auto cpuRatioInWindow = nodeStatus->cpuRatioInWindow;
        if (cpuRatioInWindow != -1 && !strategy.detect(nodeInfo)) {
            totalCPURatio += cpuRatioInWindow;
            validWorkerCount++;
        }
    }

    size_t allWorkerCount = nodeStatusManager->GetAllNodeGroups().size();
    // 50% valid then check
    if (validWorkerCount * 2 < allWorkerCount) {
        BS_INTERVAL_LOG(150, INFO,
                        "valid worker [%zu] is less than half of total worker [%zu], will not do scale by cpu",
                        validWorkerCount, allWorkerCount);
        return std::nullopt;
    }

    assert(validWorkerCount > 0);
    int32_t avgCpuRatio = totalCPURatio / validWorkerCount;
    if (avgCpuRatio >= _config.cpuStrategy.cpuMinRatio && avgCpuRatio <= _config.cpuStrategy.cpuMaxRatio) {
        BS_INTERVAL_LOG(
            150, INFO,
            "procesor cpu scale do nothing, allWorkerCount [%lu], validWorkerCount [%lu], avg cpu ratio [%d]",
            allWorkerCount, validWorkerCount, avgCpuRatio);
        return std::nullopt;
    }

    if (avgCpuRatio < _config.cpuStrategy.cpuMinRatio && !_canScaleDown) {
        BS_LOG(INFO, "cpu is [%d], but has deploy worker, will not scale down", avgCpuRatio);
        return std::nullopt;
    }

    int32_t expectCpuRatio =
        std::max((_config.cpuStrategy.cpuMinRatio + _config.cpuStrategy.cpuMaxRatio) / 2, (size_t)1);
    size_t expectWorkerCount =
        std::min(_config.maxCount, std::max(avgCpuRatio * allWorkerCount / expectCpuRatio, (size_t)1));
    BS_LOG(INFO, "try scale by cpu, worker count from [%lu] to [%lu], current cpu avg is [%d], expect [%lu, %lu]",
           allWorkerCount, expectWorkerCount, avgCpuRatio, _config.cpuStrategy.cpuMinRatio,
           _config.cpuStrategy.cpuMaxRatio);
    return std::make_pair(expectWorkerCount, 1);
}

std::optional<std::pair<uint32_t, uint32_t>>
ProcessorAdaptiveScaling::handle(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                 uint32_t currentPartitionCount, uint32_t currentParallelNum)
{
    _canScaleDown = true;
    if (_adaptiveScalingFixedMax) {
        if (currentPartitionCount == _config.maxCount && currentParallelNum == 1) {
            return std::nullopt;
        }
        return std::make_pair(_config.maxCount, 1);
    }

    size_t allWorkerCount = nodeStatusManager->GetAllNodeGroups().size();
    if (allWorkerCount == 0) {
        return std::nullopt;
    }
    for (auto f : {&ProcessorAdaptiveScaling::handleByLocator, &ProcessorAdaptiveScaling::handleByCpu}) {
        auto ret = (this->*f)(nodeStatusManager);
        if (ret.has_value()) {
            if (ret.value().first != currentPartitionCount || ret.value().second != currentParallelNum) {
                return ret;
            }
            return std::nullopt;
        }
    }

    if (currentPartitionCount * currentParallelNum > _config.maxCount) {
        BS_LOG(INFO, "current processor [%d, %d] more than max count, will scale to [%ld, %d]", currentPartitionCount,
               currentParallelNum, _config.maxCount, 1);
        return std::make_pair(_config.maxCount, 1);
    }
    return std::nullopt;
}
}} // namespace build_service::admin
