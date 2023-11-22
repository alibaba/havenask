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
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedCkptManager.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingHolder.h"
namespace indexlibv2::index::ann {

class NormalSegmentBuilder : public SegmentBuilder
{
public:
    NormalSegmentBuilder(const AithetaIndexConfig& indexConfig, const std::string& indexName, bool isMergeBuild,
                         const MetricReporterPtr& metricReporter);
    ~NormalSegmentBuilder();

public:
    bool Init(const SegmentBuildResourcePtr&) override;
    bool Build(const IndexFields& data) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& directory,
              const std::vector<docid_t>* old2NewDocId = nullptr) override;

public:
    bool BuildAndDump(PkDataHolder& pkDataHolder, const std::shared_ptr<EmbeddingHolder>& embeddingHolder,
                      const std::shared_ptr<EmbeddingHolder>& trainEmbeddingHolder,
                      const indexlib::file_system::DirectoryPtr& directory);

    size_t EstimateCurrentMemoryUse() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }
    size_t EstimateTempMemoryUseInDump() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }
    size_t EstimateDumpFileSize() override { return _currentMemoryUse * kBuildMemoryScalingFactor; }

private:
    void IncCurrentMemoryUse(size_t count) { _currentMemoryUse += count; }
    bool DumpEmbedding(const std::shared_ptr<EmbeddingHolder>& embeddingHolder, NormalSegment& segment);
    std::shared_ptr<aitheta2::CustomizedCkptManager> CreateIndexCkptManager(int64_t indexId);
    bool BuildAndDumpIndex(const std::shared_ptr<EmbeddingHolder>& embeddingHolder,
                           const std::shared_ptr<EmbeddingHolder>& trainEmbeddingHolder, NormalSegment& segment);

protected:
    std::shared_ptr<EmbeddingHolder> _embeddingHolder;
    std::shared_ptr<NormalSegmentBuildResource> _buildResource;
    size_t _currentMemoryUse;
    bool _isMergeBuild;
    std::vector<std::shared_ptr<aitheta2::CustomizedCkptManager>> _ckptManagers;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<NormalSegmentBuilder> NormalSegmentBuilderPtr;

} // namespace indexlibv2::index::ann
