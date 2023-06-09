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
#ifndef __SEGMENT_ATTRIBUTE_UPDATER_H
#define __SEGMENT_ATTRIBUTE_UPDATER_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);

namespace indexlib { namespace index {

class SegmentMetricsUpdater
{
public:
    SegmentMetricsUpdater(util::MetricProviderPtr metricProvider) : mMetricProvider(metricProvider) {}
    virtual ~SegmentMetricsUpdater() {}

public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const util::KeyValueMap& parameters) = 0;
    virtual bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                              const util::KeyValueMap& parameters) = 0;
    virtual bool InitForReCaculator(const config::IndexPartitionSchemaPtr& schema,
                                    const index_base::SegmentMergeInfo& segMergeInfo,
                                    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                    const index::DeletionMapReaderPtr& deleteMapReader, segmentid_t segmentId,
                                    const util::KeyValueMap& parameters)
    {
        assert(false);
        return false;
    }

public:
    virtual void Update(const document::DocumentPtr& doc) = 0;
    virtual void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) = 0;
    virtual void UpdateForCaculator(const std::string& checkpointDir, file_system::FenceContext* fenceContext)
    {
        assert(false);
    }
    virtual bool UpdateSegmentMetric(framework::SegmentMetrics& segMetric)
    {
        assert(false);
        return false;
    }
    virtual std::string GetUpdaterName() const = 0;
    virtual autil::legacy::json::JsonMap Dump() const = 0;
    virtual void ReportMetrics() { assert(false); }

protected:
    util::MetricProviderPtr mMetricProvider;
};

DEFINE_SHARED_PTR(SegmentMetricsUpdater);
}} // namespace indexlib::index

#endif // __SEGMENT_ATTRIBUTE_UPDATER_H
