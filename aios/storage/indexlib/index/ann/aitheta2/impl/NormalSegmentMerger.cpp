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
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentMerger.h"

#include "indexlib/index/ann/aitheta2/impl/NormalSegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/util/PkDataExtractor.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"
using namespace std;
using namespace indexlibv2::index::ann;

namespace indexlibv2::index::ann {

static const float kMergeMemoryScalingFactor = 2.5f;

bool NormalSegmentMerger::Init() { return ParamsInitializer::InitAiThetaMeta(_indexConfig, _aiThetaMeta); }

bool NormalSegmentMerger::Merge(const NormalSegmentVector& segments,
                                const std::vector<std::shared_ptr<EmbeddingAttrSegmentBase>>& embAttrSegments,
                                const indexlib::file_system::DirectoryPtr& dumpDiretory)
{
    PkDataHolder pkDataHolder;
    if (_indexConfig.buildConfig.storePrimaryKey) {
        PkDataExtractor pkDataExtractor(_mergeTask);
        ANN_CHECK(pkDataExtractor.Extract(segments, pkDataHolder), "extract failed");
    }

    EmbeddingDataHolder embDataHolder(_aiThetaMeta);
    EmbeddingDataExtractor embDataExtractor(_indexConfig, _mergeTask);
    ANN_CHECK(embDataExtractor.Extract(segments, embAttrSegments, embDataHolder), "extract failed");

    for (auto segment : segments) {
        segment->Close();
    }

    NormalSegmentBuilder builder(_indexConfig, _indexName, true, _metricReporter);
    ANN_CHECK(builder.Init(SegmentBuildResourcePtr()), "init builder failed");
    ANN_CHECK(builder.BuildAndDump(pkDataHolder, embDataHolder, dumpDiretory), "build failed");
    pkDataHolder.Clear();
    embDataHolder.Clear();

    AUTIL_LOG(INFO, "dump merged index to directory[%s]", dumpDiretory->DebugString().c_str());
    return true;
}

size_t NormalSegmentMerger::EstimateMergeMemoryUse(const indexlib::file_system::DirectoryPtr& directory,
                                                   size_t dimension)
{
    if (!SegmentMeta::IsExist(directory)) {
        return 0ul;
    }
    SegmentMeta segmentMeta;
    segmentMeta.Load(directory);
    return segmentMeta.GetDocCount() * sizeof(float) * dimension * kMergeMemoryScalingFactor;
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegmentMerger);
} // namespace indexlibv2::index::ann
