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

#include "indexlib/index/ann/aitheta2/impl/RealtimeIndexBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegment.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/util/PkDataHolder.h"
namespace indexlibv2::index::ann {

class RealtimeSegmentBuilder : public SegmentBuilder
{
public:
    RealtimeSegmentBuilder(const AithetaIndexConfig& indexConfig, const std::string& indexName,
                           const MetricReporterPtr& reporter);
    ~RealtimeSegmentBuilder();

public:
    bool Init(const SegmentBuildResourcePtr& segmentBuildResource) override;
    bool Build(const EmbeddingFieldData& data) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& directory,
              const std::vector<docid_t>* old2NewDocId = nullptr) override;

public:
    size_t EstimateCurrentMemoryUse() override { return _buildMemoryUse * kBuildMemoryScalingFactor; }
    size_t EstimateTempMemoryUseInDump() override { return 0; }
    size_t EstimateDumpFileSize() override { return _buildMemoryUse * kBuildMemoryScalingFactor; }

public:
    const RealtimeSegmentPtr GetRealtimeSegment() const { return _segment; }

private:
    RealtimeIndexBuilderPtr GetRealtimeIndexBuilder(index_id_t indexId);
    bool DoDelete(int64_t pk);
    bool DoBuild(const EmbeddingFieldData& data);

private:
    void InitBuildMetrics();
    size_t EstimateBuildMemoryUse();

protected:
    RealtimeIndexBuilderMap _indexBuilderMap;
    RealtimeSegmentPtr _segment;
    AiThetaContextHolderPtr _buildContextHolder;
    PkDataHolder _pkDataHolder;
    size_t _buildMemoryUse {0};

private:
    METRIC_DECLARE(_buildMemoryMetric);
    METRIC_DECLARE(_buildLatencyMetric);
    METRIC_DECLARE(_dumpLatencyMetric);
    METRIC_DECLARE(_buildQpsMetric);
    METRIC_DECLARE(_deleteQpsMetric);
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RealtimeSegmentBuilder> RealtimeSegmentBuilderPtr;

} // namespace indexlibv2::index::ann
