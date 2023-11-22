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
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/temperature_evaluator.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace index {

class TemperatureSegmentMetricsUpdater : public SegmentMetricsUpdater
{
public:
    TemperatureSegmentMetricsUpdater(util::MetricProviderPtr metricProvider);
    ~TemperatureSegmentMetricsUpdater();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const util::KeyValueMap& parameters) override;
    bool InitForReCaculator(const config::IndexPartitionSchemaPtr& schema,
                            const index_base::SegmentMergeInfo& segMergeInfo,
                            const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                            const index::DeletionMapReaderPtr& deleteMapReader, segmentid_t segmentId,
                            const util::KeyValueMap& parameters) override;
    static TemperatureProperty StringToTemperatureProperty(const std::string& temperature);
    static TemperatureProperty Int64ToTemperatureProperty(int64_t temperature);

public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) override;
    void UpdateForCaculator(const std::string& checkpointDir, file_system::FenceContext* fenceContext) override;
    bool UpdateSegmentMetric(framework::SegmentMetrics& segMetric) override;
    autil::legacy::json::JsonMap Dump() const override;
    TemperatureProperty GetDocTemperatureProperty(segmentid_t oldSegId, docid_t oldLocalId);
    std::string GetUpdaterName() const override { return TemperatureSegmentMetricsUpdater::UPDATER_NAME; }
    void GetSegmentTemperatureMeta(index_base::SegmentTemperatureMeta& temperatureMeta);
    void ReportMetrics() override;

public:
    static const std::string UPDATER_NAME;

private:
    void UpdateMetrics(TemperatureProperty type, docid_t localDocId = INVALID_DOCID);
    std::string GenerateLifeCycleTag() const;
    std::string GenerateSegmentTemperatureStatus() const;

    std::string GetCheckpointFilePath(const std::string& checkpointDir, int32_t docType);
    void StoreCheckpoint(const std::string& checkpointDir, file_system::FenceContext* fenceContext);
    bool LoadFromCheckPoint(const std::string& checkpointDir);
    void InitDocDetail(uint32_t docCount);

private:
    int64_t mHotDocCount;
    int64_t mWarmDocCount;
    int64_t mColdDocCount;
    TemperatureEvaluator mEvaluator;
    std::vector<util::BitmapPtr> mDocDetail;
    segmentid_t mSegmentId;
    int64_t mTotalDocCount;
    TemperatureProperty mOldTemperature;
    bool mHasCalculator;
    index::DeletionMapReaderPtr mDeletionReader;
    IE_DECLARE_METRIC(SegmentTemperatureProperty);
    IE_DECLARE_METRIC(DocTemperatureProperty);

private:
    friend class TemperatureSegmentMetricsUpdaterTest;

private:
    static constexpr int32_t DOC_TYPE = 3;
    static constexpr int32_t DOC_TYPE_HOT = 0;
    static constexpr int32_t DOC_TYPE_WARM = 1;
    static constexpr int32_t DOC_TYPE_COLD = 2;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureSegmentMetricsUpdater);
}} // namespace indexlib::index
