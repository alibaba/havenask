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

#include "autil/legacy/any.h"
#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace index {

class LifecycleSegmentMetricsUpdater : public SegmentMetricsUpdater
{
public:
    LifecycleSegmentMetricsUpdater(util::MetricProviderPtr metricProvider)
        : SegmentMetricsUpdater(metricProvider)
        , mMaxMinUpdater(metricProvider)
        , mIsMerge(false)
    {
    }
    ~LifecycleSegmentMetricsUpdater();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const util::KeyValueMap& parameters) override;

public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) override;
    autil::legacy::json::JsonMap Dump() const override;
    std::string GetUpdaterName() const override { return LifecycleSegmentMetricsUpdater::UPDATER_NAME; }

public:
    static const std::string UPDATER_NAME;
    static const std::string LIFECYCLE_PARAM;
    static const std::string DEFAULT_LIFECYCLE_TAG;

public:
    static std::string GetAttrKey(const std::string& attrName)
    {
        return MaxMinSegmentMetricsUpdater::GetAttrKey(attrName);
    }

private:
    template <typename T>
    bool GetSegmentAttrValue(const framework::SegmentMetrics& metrics, T& maxValue, T& minValue) const;

    template <typename T>
    std::string GenerateLifeCycleTag() const;

    template <typename T>
    bool InitThresholdInfos();

private:
    MaxMinSegmentMetricsUpdater mMaxMinUpdater;
    bool mIsMerge;
    std::string mLifecycleParam;
    std::string mDefaultLifecycleTag;
    index_base::SegmentMergeInfos mSegMergeInfos;
    autil::legacy::Any mThresholdInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LifecycleSegmentMetricsUpdater);
}} // namespace indexlib::index
