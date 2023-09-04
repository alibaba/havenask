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
#include "indexlib/framework/lifecycle/LifecycleStrategy.h"

#include "autil/StringUtil.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentStatistics.h"

std::string indexlib::framework::LifecycleStrategy::CalculateLifecycle(
    const indexlibv2::framework::SegmentStatistics& segmentStatistic,
    const indexlib::file_system::LifecycleConfig& lifecycleConfig)
{
    for (const auto& pattern : lifecycleConfig.GetPatterns()) {
        const std::string& statisticField = pattern->statisticField;
        if (pattern->statisticType == indexlib::file_system::LifecyclePatternBase::INTEGER_TYPE) {
            std::pair<int64_t, int64_t> segRange;
            if (!segmentStatistic.GetStatistic(statisticField, segRange)) {
                continue;
            }
            auto typedPattern = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(pattern);
            assert(typedPattern);
            std::pair<int64_t, int64_t> configRange = typedPattern->range;
            if (pattern->isOffset) {
                configRange.first += typedPattern->baseValue;
                configRange.second += typedPattern->baseValue;
            }
            if (indexlib::file_system::LifecycleConfig::IsRangeOverLapped(configRange, segRange)) {
                return pattern->lifecycle;
            }
        } else if (pattern->statisticType == indexlib::file_system::LifecyclePatternBase::STRING_TYPE) {
            std::string segValue;
            if (!segmentStatistic.GetStatistic(statisticField, segValue)) {
                continue;
            }
            auto typedPattern = std::dynamic_pointer_cast<indexlib::file_system::StringLifecyclePattern>(pattern);
            assert(typedPattern);
            auto iter = std::find(typedPattern->range.begin(), typedPattern->range.end(), segValue);
            if (iter != typedPattern->range.end()) {
                return pattern->lifecycle;
            }
        }
        // TODO: 如果类型超>=3种则考虑在pattern基类加一个match方法
    }
    return "";
}
