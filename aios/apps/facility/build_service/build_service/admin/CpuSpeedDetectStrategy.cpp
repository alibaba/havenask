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
#include "build_service/admin/CpuSpeedDetectStrategy.h"

#include "build_service/util/StatisticUtil.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CpuSpeedDetectStrategy);

int64_t CpuSpeedDetectStrategy::INVALID_CPU_SPEED = -1;
std::string CpuSpeedDetectStrategy::BOXPLOT_DETECT = "BOXPLOT";
std::string CpuSpeedDetectStrategy::MISS_DETECT = "MISS";
std::string CpuSpeedDetectStrategy::FIXED_DETECT = "FIXED";
std::string CpuSpeedDetectStrategy::FALSE_DETECT = "FALSE";
float CpuSpeedDetectStrategy::MISS_DETECT_RATIO = 0.4;
float CpuSpeedDetectStrategy::FALSE_DETECT_RATIO = 0.7;

CpuSpeedDetectStrategy::CpuSpeedDetectStrategy(const std::vector<int64_t>& allCpuSpeed, int64_t fixedValueThreshold)
    : _allCpuSpeed(allCpuSpeed)
    , _fixedValueThreshold(fixedValueThreshold)
{
}

bool CpuSpeedDetectStrategy::detect(Node& node)
{
    if (unlikely(_allCpuSpeed.empty())) {
        return false;
    }
    std::string triggeredStrategy = "";
    if (triggerBoxplotDetect(node)) {
        if (triggerFalseDetect(node)) {
            return false;
        }
        triggeredStrategy = BOXPLOT_DETECT;
    } else if (triggerMissDetect(node)) {
        triggeredStrategy = MISS_DETECT;
    } else if (triggerFixedValueDetect(node)) {
        triggeredStrategy = FIXED_DETECT;
    }
    if (!triggeredStrategy.empty()) {
        BS_LOG(WARN, "slow node detected. trigger [%s], cpu_speed[%ld], comment[%s], ip[%s], role_name[%s]",
               triggeredStrategy.c_str(), node.cpuSpeed, node.comments[triggeredStrategy].c_str(), node.ip.c_str(),
               node.roleName.c_str());
        return true;
    }
    return false;
}

bool CpuSpeedDetectStrategy::triggerBoxplotDetect(Node& node)
{
    if (_boxplotThreshold == INVALID_CPU_SPEED) {
        // do not change the coefficient unless you know what you are doing.
        auto [minBound, _] = util::StatisticUtil::CalculateBoxplotBound(_allCpuSpeed, /*coefficient=*/1.5);
        _boxplotThreshold = minBound;
    }
    node.comments[BOXPLOT_DETECT] = "threshold=" + std::to_string(_boxplotThreshold);
    return node.cpuSpeed < _boxplotThreshold;
}

bool CpuSpeedDetectStrategy::triggerMissDetect(Node& node)
{
    if (_missDetectThreshold == INVALID_CPU_SPEED) {
        int64_t p75 = util::StatisticUtil::GetPercentile(_allCpuSpeed, /*percent=*/0.75);
        _missDetectThreshold = p75 * MISS_DETECT_RATIO;
    }
    node.comments[MISS_DETECT] = "threshold=" + std::to_string(_missDetectThreshold);
    return node.cpuSpeed < _missDetectThreshold;
}

bool CpuSpeedDetectStrategy::triggerFalseDetect(Node& node)
{
    if (_falseDetectThreshold == INVALID_CPU_SPEED) {
        int64_t p75 = util::StatisticUtil::GetPercentile(_allCpuSpeed, /*percent=*/0.75);
        _falseDetectThreshold = p75 * FALSE_DETECT_RATIO;
    }
    node.comments[FALSE_DETECT] = "threshold=" + std::to_string(_falseDetectThreshold);
    return node.cpuSpeed > _falseDetectThreshold;
}

bool CpuSpeedDetectStrategy::triggerFixedValueDetect(Node& node)
{
    node.comments[FIXED_DETECT] = "threshold=" + std::to_string(_fixedValueThreshold);
    return node.cpuSpeed < _fixedValueThreshold;
}

}} // namespace build_service::admin
