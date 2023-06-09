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
#include "indexlib/index_base/lifecycle_strategy.h"

#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/framework/lifecycle/DynamicLifecycleStrategy.h"
#include "indexlib/framework/lifecycle/StaticLifecycleStrategy.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;

namespace indexlib::index_base {
AUTIL_LOG_SETUP(indexlib.index_base, LifecycleStrategyFactory);

class StaticLifecycleStrategy : public LifecycleStrategy
{
public:
    StaticLifecycleStrategy(const indexlib::file_system::LifecycleConfig& lifecycleConfig) : _strategy(lifecycleConfig)
    {
    }
    ~StaticLifecycleStrategy() {}

public:
    std::vector<std::pair<segmentid_t, std::string>>
    GetSegmentLifecycles(const indexlib::index_base::Version& version) override
    {
        std::vector<std::pair<segmentid_t, std::string>> ret;
        const auto& temperatureMetas = version.GetSegTemperatureMetas();
        for (const auto& meta : temperatureMetas) {
            ret.emplace_back(meta.segmentId, meta.segTemperature);
        }
        return ret;
    }

    string GetSegmentLifecycle(const indexlib::index_base::Version& version,
                               const indexlib::index_base::SegmentData* segmentData) override
    {
        auto segId = segmentData->GetSegmentId();
        auto segDir = segmentData->GetDirectory();
        if (indexlib::index_base::RealtimeSegmentDirectory::IsRtSegmentId(segId) ||
            indexlib::index_base::JoinSegmentDirectory::IsJoinSegmentId(segId)) {
            return LIFECYCLE_HOT;
        }
        indexlib::index_base::SegmentTemperatureMeta meta;
        if (version.GetSegmentTemperatureMeta(segId, meta)) {
            return meta.segTemperature;
        }
        // to be compatible with old version static lifecycle info
        indexlib::file_system::IndexFileList segmentFileList;
        if (indexlib::index_base::SegmentFileListWrapper::Load(segDir, segmentFileList)) {
            return segmentFileList.lifecycle;
        }
        return "";
    }

    std::vector<std::pair<segmentid_t, std::string>>
    GetSegmentLifecycles(const shared_ptr<indexlibv2::framework::SegmentDescriptions>& segDescriptions) override
    {
        return _strategy.GetSegmentLifecycles(segDescriptions);
    }

    std::string CalculateLifecycle(const indexlibv2::framework::SegmentStatistics& segmentStatistic) const override
    {
        return _strategy.CalculateLifecycle(segmentStatistic);
    }

private:
    static inline const string LIFECYCLE_HOT = "HOT";
    indexlib::framework::StaticLifecycleStrategy _strategy;
};

class DynamicLifecycleStrategy : public LifecycleStrategy
{
public:
    DynamicLifecycleStrategy(const indexlib::file_system::LifecycleConfig& lifecycleConfig) : _strategy(lifecycleConfig)
    {
    }
    ~DynamicLifecycleStrategy() {}

public:
    std::vector<std::pair<segmentid_t, std::string>>
    GetSegmentLifecycles(const indexlib::index_base::Version& version) override
    {
        std::vector<std::pair<segmentid_t, std::string>> ret;
        const auto& segStatistics = version.GetSegmentStatisticsVector();
        for (const auto& segStats : segStatistics) {
            auto temperature = CalculateLifecycle(segStats);
            if (!temperature.empty()) {
                ret.emplace_back(segStats.GetSegmentId(), temperature);
            }
        }
        return ret;
    }

    string GetSegmentLifecycle(const indexlib::index_base::Version& version,
                               const indexlib::index_base::SegmentData* segmentData) override
    {
        auto segId = segmentData->GetSegmentId();
        indexlibv2::framework::SegmentStatistics segmentStatistics;
        if (!version.GetSegmentStatistics(segId, &segmentStatistics)) {
            return "";
        }
        return CalculateLifecycle(segmentStatistics);
    }

    std::vector<std::pair<segmentid_t, std::string>>
    GetSegmentLifecycles(const shared_ptr<indexlibv2::framework::SegmentDescriptions>& segDescriptions) override
    {
        return _strategy.GetSegmentLifecycles(segDescriptions);
    }

    std::string CalculateLifecycle(const indexlibv2::framework::SegmentStatistics& segmentStatistic) const override
    {
        return _strategy.CalculateLifecycle(segmentStatistic);
    }

private:
    indexlib::framework::DynamicLifecycleStrategy _strategy;
};

const string LifecycleStrategyFactory::DYNAMIC_STRATEGY = "dynamic";
const string LifecycleStrategyFactory::STATIC_STRATEGY = "static";

unique_ptr<LifecycleStrategy>
LifecycleStrategyFactory::CreateStrategy(const indexlib::file_system::LifecycleConfig& lifecycleConfig,
                                         const map<string, string>& parameters)
{
    const auto& strategy = lifecycleConfig.GetStrategy();
    if (strategy == DYNAMIC_STRATEGY) {
        indexlib::file_system::LifecycleConfig newConfig = lifecycleConfig;
        if (!newConfig.InitOffsetBase(parameters)) {
            IE_LOG(ERROR, "set lifecycle config template parameter failed");
            return nullptr;
        }
        unique_ptr<LifecycleStrategy> strategy(new DynamicLifecycleStrategy(newConfig));
        return strategy;
    }
    if (strategy == STATIC_STRATEGY) {
        unique_ptr<LifecycleStrategy> strategy(new StaticLifecycleStrategy(lifecycleConfig));
        return strategy;
    }
    AUTIL_LOG(ERROR, "Invalid LifyecycleStrategy type [%s]", strategy.c_str());
    return nullptr;
}

} // namespace indexlib::index_base
