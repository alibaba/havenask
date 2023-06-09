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
#ifndef __INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H
#define __INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);

namespace indexlib { namespace index {

class SegmentMetricsUpdaterFactory : public plugin::ModuleFactory
{
public:
    SegmentMetricsUpdaterFactory();
    ~SegmentMetricsUpdaterFactory();

public:
    static const std::string SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX;

public:
    virtual index::SegmentMetricsUpdaterPtr CreateUpdater(const std::string& className,
                                                          const util::KeyValueMap& parameters,
                                                          const config::IndexPartitionSchemaPtr& schema,
                                                          const config::IndexPartitionOptions& options,
                                                          const util::MetricProviderPtr& metrics) = 0;

    virtual index::SegmentMetricsUpdaterPtr
    CreateUpdaterForMerge(const std::string& className, const util::KeyValueMap& parameters,
                          const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                          const index_base::SegmentMergeInfos& segMergeInfos,
                          const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                          const util::MetricProviderPtr& metrics) = 0;

    virtual index::SegmentMetricsUpdaterPtr
    CreateUpdaterForReCalculator(const std::string& className, const util::KeyValueMap& parameters,
                                 const config::IndexPartitionSchemaPtr& schema,
                                 const index_base::SegmentMergeInfo& segMergeInfo,
                                 const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                 const index::DeletionMapReaderPtr& deleteMapReader, segmentid_t segmentId,
                                 const util::MetricProviderPtr& metrics) = 0;

    void destroy() override { delete this; }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H
