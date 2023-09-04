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
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataHolder.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataExtractor.h"

namespace indexlibv2::index::ann {

class EmbeddingAttrSegmentBase;

class NormalSegmentMerger
{
public:
    NormalSegmentMerger(const AithetaIndexConfig& config, const std::string& indexName,
                        const MetricReporterPtr& metricReporter)
        : _indexConfig(config)
        , _indexName(indexName)
        , _metricReporter(metricReporter)
    {
    }
    ~NormalSegmentMerger() = default;

public:
    bool Init();
    bool Merge(const NormalSegmentVector& segments,
               const std::vector<std::shared_ptr<EmbeddingAttrSegmentBase>>& embAttrSegments,
               const indexlib::file_system::DirectoryPtr& directory);
    void SetMergeTask(const MergeTask& mergeTask) { _mergeTask = mergeTask; }

public:
    static size_t EstimateMergeMemoryUse(const indexlib::file_system::DirectoryPtr& directory, size_t dimension);

protected:
    AithetaIndexConfig _indexConfig;
    std::string _indexName;
    AiThetaMeta _aiThetaMeta;
    MergeTask _mergeTask;
    MetricReporterPtr _metricReporter;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<NormalSegmentMerger> NormalSegmentMergerPtr;

} // namespace indexlibv2::index::ann
