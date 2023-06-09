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
#ifndef __INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H
#define __INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H

#include <functional>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategy);
DECLARE_REFERENCE_CLASS(merger, MergePlan);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);

namespace indexlib { namespace merger {

class SplitSegmentStrategyFactory : public plugin::ModuleFactory
{
public:
    SplitSegmentStrategyFactory();
    virtual ~SplitSegmentStrategyFactory();

public:
    static const std::string SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX;

public:
    virtual void Init(index::SegmentDirectoryBasePtr segDir, config::IndexPartitionSchemaPtr schema,
                      index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      std::function<index::SegmentMetricsUpdaterPtr()> segAttrUpdaterGenerator,
                      std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                      const util::MetricProviderPtr& provider);

    void destroy() override { delete this; }

public:
    virtual SplitSegmentStrategyPtr CreateSplitStrategy(const std::string& strategyName,
                                                        const util::KeyValueMap& parameters, const MergePlan& plan);

protected:
    index::SegmentDirectoryBasePtr mSegDir;
    config::IndexPartitionSchemaPtr mSchema;
    index::OfflineAttributeSegmentReaderContainerPtr mAttrReaders;
    index_base::SegmentMergeInfos mSegMergeInfos;
    std::function<index::SegmentMetricsUpdaterPtr()> mSegAttrUpdaterGenerator;
    std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> mHintDocInfos;
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SplitSegmentStrategyFactory);
}} // namespace indexlib::merger

#endif //__INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H
