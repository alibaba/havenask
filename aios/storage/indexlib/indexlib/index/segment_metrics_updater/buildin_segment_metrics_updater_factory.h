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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater_factory.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, ModuleClassConfig);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace index {

class BuildinSegmentMetricsUpdaterFactory : public SegmentMetricsUpdaterFactory
{
public:
    BuildinSegmentMetricsUpdaterFactory();
    ~BuildinSegmentMetricsUpdaterFactory();

public:
    virtual index::SegmentMetricsUpdaterPtr CreateUpdater(const std::string& className,
                                                          const util::KeyValueMap& parameters,
                                                          const config::IndexPartitionSchemaPtr& schema,
                                                          const config::IndexPartitionOptions& options,
                                                          const util::MetricProviderPtr& metrics) override;

    virtual index::SegmentMetricsUpdaterPtr
    CreateUpdaterForMerge(const std::string& className, const util::KeyValueMap& parameters,
                          const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                          const index_base::SegmentMergeInfos& segMergeInfos,
                          const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                          const util::MetricProviderPtr& metrics) override;

    virtual index::SegmentMetricsUpdaterPtr
    CreateUpdaterForReCalculator(const std::string& className, const util::KeyValueMap& parameters,
                                 const config::IndexPartitionSchemaPtr& schema,
                                 const index_base::SegmentMergeInfo& segMergeInfo,
                                 const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                 const index::DeletionMapReaderPtr& deleteMapReader, segmentid_t segmentId,
                                 const util::MetricProviderPtr& metrics) override;

private:
    index::SegmentMetricsUpdaterPtr DoCreate(const std::string& updaterName, const util::MetricProviderPtr& metrics);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildinSegmentMetricsUpdaterFactory);
}} // namespace indexlib::index
