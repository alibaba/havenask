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
#include "indexlib/merger/split_strategy/split_segment_strategy_factory.h"

#include <iosfwd>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/split_strategy/default_split_strategy.h"
#include "indexlib/merger/split_strategy/simple_split_strategy.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"
#include "indexlib/merger/split_strategy/temperature_split_strategy.h"
#include "indexlib/merger/split_strategy/test_split_strategy.h"
#include "indexlib/merger/split_strategy/time_series_split_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::config;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SplitSegmentStrategyFactory);

const std::string SplitSegmentStrategyFactory::SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX = "Factory_SplitSegmentStrategy";

SplitSegmentStrategyFactory::SplitSegmentStrategyFactory() {}

void SplitSegmentStrategyFactory::Init(SegmentDirectoryBasePtr segDir, IndexPartitionSchemaPtr schema,
                                       OfflineAttributeSegmentReaderContainerPtr attrReaders,
                                       const SegmentMergeInfos& segMergeInfos,
                                       std::function<index::SegmentMetricsUpdaterPtr()> segAttrUpdaterGenerator,
                                       std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                                       const util::MetricProviderPtr& provider)
{
    mSegDir = segDir;
    mSchema = schema;
    mAttrReaders = attrReaders;
    mSegMergeInfos = segMergeInfos;
    mSegAttrUpdaterGenerator = std::move(segAttrUpdaterGenerator);
    mHintDocInfos = hintDocInfos;
    mMetricProvider = provider;
}

SplitSegmentStrategyFactory::~SplitSegmentStrategyFactory() {}

SplitSegmentStrategyPtr SplitSegmentStrategyFactory::CreateSplitStrategy(const string& strategyName,
                                                                         const util::KeyValueMap& parameters,
                                                                         const MergePlan& plan)
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
        INDEXLIB_FATAL_ERROR(UnSupported, "kv and kkv table do not support split segment strategy");
    }
    SplitSegmentStrategyPtr ret;
    if (strategyName.empty() || strategyName == SimpleSplitStrategy::STRATEGY_NAME) {
        ret.reset(new SimpleSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan, mHintDocInfos,
                                          mMetricProvider));
    } else if (strategyName == TestSplitStrategy::STRATEGY_NAME) {
        ret.reset(new TestSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan, mHintDocInfos,
                                        mMetricProvider));
    } else if (strategyName == TimeSeriesSplitStrategy::STRATEGY_NAME) {
        ret.reset(new TimeSeriesSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan, mHintDocInfos,
                                              mMetricProvider));
    } else if (strategyName == DefaultSplitStrategy::STRATEGY_NAME) {
        ret.reset(new DefaultSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan, mHintDocInfos,
                                           mMetricProvider));
    } else if (strategyName == TemperatureSplitStrategy::STRATEGY_NAME) {
        ret.reset(new TemperatureSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan, mHintDocInfos,
                                               mMetricProvider));
    }
    if (ret) {
        if (!ret->Init(parameters)) {
            IE_LOG(ERROR, "init strategy[%s] failed", strategyName.c_str());
            return SplitSegmentStrategyPtr();
        }
        ret->SetAttrUpdaterGenerator(mSegAttrUpdaterGenerator);
        return ret;
    }
    IE_LOG(ERROR, "strategy[%s] not found", strategyName.c_str());
    return SplitSegmentStrategyPtr();
}
}} // namespace indexlib::merger
