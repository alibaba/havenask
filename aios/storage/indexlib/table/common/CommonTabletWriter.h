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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/Version.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace kmonitor {
class MetricsTags;
}

namespace indexlibv2::config {
class TabletOptions;
class TabletSchema;
} // namespace indexlibv2::config

namespace indexlib::document {
class Document;
}

namespace indexlibv2::framework {
class SegmentDumper;
class TabletData;
class MemSegment;
} // namespace indexlibv2::framework

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace indexlibv2::table {

#define REGISTER_BUILD_METRIC(metricName)                                                                              \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_buildResource.metricsManager->GetMetricsReporter(), metricName,              \
                                         "build/" #metricName, kmonitor::GAUGE)

class CommonTabletWriter : public framework::TabletWriter
{
public:
    CommonTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options);
    ~CommonTabletWriter() = default;

    Status Open(const std::shared_ptr<framework::TabletData>& tabletData, const framework::BuildResource& buildResource,
                const framework::OpenOptions& openOptions) override;
    Status Build(const std::shared_ptr<document::IDocumentBatch>& batch) override;
    std::unique_ptr<framework::SegmentDumper> CreateSegmentDumper() override;
    size_t GetTotalMemSize() const override;
    size_t GetBuildingSegmentDumpExpandSize() const override;
    void ReportMetrics() override;
    bool IsDirty() const override;

protected:
    static const float MIN_DUMP_MEM_LIMIT_RATIO;
    virtual Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                          const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions);
    virtual Status DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch);
    virtual Status DoBuildAndReportMetrics(const std::shared_ptr<document::IDocumentBatch>& batch);
    virtual int64_t EstimateMaxMemoryUseOfCurrentSegment() const;

    Status CheckMemStatus() const;
    int64_t GetMinDumpSegmentMemSize() const;
    indexlib::util::AccumulativeCounterPtr InitCounter(const std::string& nodePrefix, const std::string& counterName,
                                                       const std::shared_ptr<indexlib::util::CounterMap>& counterMap);
    void InitCounters(const std::shared_ptr<indexlib::util::CounterMap>& counterMap);
    void RegisterMetrics();
    void UpdateDocCounter(document::IDocumentBatch* batch);
    void ReportBuildDocumentMetrics(document::IDocumentBatch* batch);

private:
    virtual void RegisterTableSepecificMetrics();
    virtual void ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags);

protected:
    std::shared_ptr<config::TabletSchema> _schema;
    const config::TabletOptions* _options;
    std::shared_ptr<framework::TabletData> _tabletData;
    framework::BuildResource _buildResource;
    std::shared_ptr<framework::MemSegment> _buildingSegment;
    int64_t _startBuildTime;
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, buildMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, buildingSegmentMemoryUse);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, estimateMaxMemoryUseOfCurrentSegment);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, segmentBuildingTime);
    // counters
    indexlib::util::AccumulativeCounterPtr mAddDocCountCounter;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
