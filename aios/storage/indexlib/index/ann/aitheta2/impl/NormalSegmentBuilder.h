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
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataHolder.h"
namespace indexlibv2::index::ann {

class NormalSegmentBuilder : public SegmentBuilder
{
public:
    NormalSegmentBuilder(const AithetaIndexConfig& indexConfig, const std::string& indexName, bool isMergeBuild,
                         const MetricReporterPtr& metricReporter)
        : SegmentBuilder(indexConfig, indexName, metricReporter)
        , _currentMemoryUse(0ul)
        , _isMergeBuild(isMergeBuild)
    {
    }
    ~NormalSegmentBuilder() {}

public:
    bool Init(const SegmentBuildResourcePtr&) override;
    bool Build(const EmbeddingFieldData& data) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& directory,
              const std::vector<docid_t>* old2NewDocId = nullptr) override;

public:
    bool BuildAndDump(const PkDataHolder& pkDataHolder, const EmbeddingDataHolder& embDataHolder,
                      const indexlib::file_system::DirectoryPtr& directory);

public:
    size_t EstimateCurrentMemoryUse() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }
    size_t EstimateTempMemoryUseInDump() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }
    size_t EstimateDumpFileSize() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }

private:
    void IncCurrentMemoryUse(size_t count) { _currentMemoryUse += count; }
    bool BuildAndDumpIndex(const EmbeddingDataHolder& embDataHolder, NormalSegment& segment);
    bool DumpEmbeddingData(const EmbeddingDataHolder& embDataHolder, NormalSegment& segment);

protected:
    EmbeddingDataHolderPtr _embDataHolder;
    size_t _currentMemoryUse;
    bool _isMergeBuild;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<NormalSegmentBuilder> NormalSegmentBuilderPtr;

} // namespace indexlibv2::index::ann
