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
#ifndef __INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H
#define __INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, ModuleClassConfig);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace index {

class MultiSegmentMetricsUpdater : public SegmentMetricsUpdater
{
public:
    using SegmentMetricsUpdaterVector = std::vector<std::shared_ptr<SegmentMetricsUpdater>>;

public:
    MultiSegmentMetricsUpdater(util::MetricProviderPtr metricProvider) : SegmentMetricsUpdater(metricProvider) {}
    ~MultiSegmentMetricsUpdater();

public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const util::KeyValueMap& parameters) override
    {
        assert(false);
        return false;
    }
    virtual bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                              const util::KeyValueMap& parameters) override
    {
        assert(false);
        return false;
    }

public:
    void Init(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
              const std::vector<config::ModuleClassConfig> updaterConfigs,
              const plugin::PluginManagerPtr& pluginManager);

    void InitForMerge(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const std::vector<config::ModuleClassConfig> updaterConfigs,
                      const plugin::PluginManagerPtr& pluginManager);

    bool InitForReCalculator(const config::IndexPartitionSchemaPtr& schema,
                             const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                             const index::DeletionMapReaderPtr& deleteMapReader,
                             const std::vector<config::ModuleClassConfig> updaterConfigs,
                             const index_base::SegmentMergeInfo& segMergeInfo, segmentid_t segmentId,
                             const plugin::PluginManagerPtr& pluginManager);
    SegmentMetricsUpdaterPtr GetMetricUpdate(const std::string& name);
    void ReportMetrics() override;

public:
    virtual void Update(const document::DocumentPtr& doc) override;
    virtual void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue) override;
    virtual void UpdateForCaculator(const std::string& checkpointDir, file_system::FenceContext* fenceContext) override;
    virtual bool UpdateSegmentMetric(framework::SegmentMetrics& segMetric) override;
    virtual autil::legacy::json::JsonMap Dump() const override;
    std::string GetUpdaterName() const override { return UPDATER_NAME; }
    int64_t GetSegmentMetricUpdaterSize() const;

public:
    static const std::string UPDATER_NAME;

private:
    SegmentMetricsUpdaterVector mSegMetricsUpdaters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentMetricsUpdater);
}} // namespace indexlib::index

#endif //__INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H
