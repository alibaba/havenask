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

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedCkptManager.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingHolder.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataWriter.h"
namespace indexlibv2::index::ann {

class NormalIndexBuilder
{
public:
    NormalIndexBuilder(const AithetaIndexConfig& indexConfig,
                       const MetricReporterPtr& reporter = ann::MetricReporterPtr())
        : _indexConfig(indexConfig)
        , _metricReporter(reporter)
    {
    }
    ~NormalIndexBuilder() = default;

public:
    bool Init(size_t docCount);
    bool Train(std::shared_ptr<EmbeddingBufferBase>& buffer,
               std::shared_ptr<aitheta2::CustomizedCkptManager>& indexCkptManager);
    bool BuildAndDump(std::shared_ptr<EmbeddingBufferBase>& buffer,
                      std::shared_ptr<aitheta2::CustomizedCkptManager>& indexCkptManager,
                      IndexDataWriterPtr& indexDataWriter);
    const IndexMeta GetIndexMeta() const { return _indexMeta; }

private:
    void InitBuildMetrics();

private:
    AithetaIndexConfig _indexConfig;
    AiThetaBuilderPtr _builder;
    IndexMeta _indexMeta;
    MetricReporterPtr _metricReporter;
    METRIC_DECLARE(_trainLatencyMetric);
    METRIC_DECLARE(_buildLatencyMetric);
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalIndexBuilder> NormalIndexBuilderPtr;

} // namespace indexlibv2::index::ann
