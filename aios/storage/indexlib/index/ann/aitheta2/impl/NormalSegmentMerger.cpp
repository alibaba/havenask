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
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaDumper.h"
#include "indexlib/index/ann/aitheta2/util/PkDataExtractor.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"
using namespace std;
using namespace indexlibv2::index::ann;

namespace indexlibv2::index::ann {

static const float kMergeMemoryScalingFactor = 2.5f;

bool NormalSegmentMerger::Init()
{
    METRIC_SETUP(_extractPkLatencyMetric, "indexlib.vector.offline.extract_pk_latency", kmonitor::GAUGE);
    METRIC_SETUP(_extractEmbeddingLatencyMetric, "indexlib.vector.offline.extract_embedding_latency", kmonitor::GAUGE);
    METRIC_SETUP(_mergeLatencyMetric, "indexlib.vector.offline.merge_latency", kmonitor::GAUGE);
    return true;
}

bool NormalSegmentMerger::Merge(const NormalSegmentVector& indexSegments,
                                const std::vector<std::shared_ptr<EmbeddingAttrSegment>>& embAttrSegments,
                                const std::shared_ptr<NormalSegmentMergeResource>& mergeResource,
                                const indexlib::file_system::DirectoryPtr& dumpDiretory)
{
    ScopedLatencyReporter reporter(_mergeLatencyMetric);

    PkDataHolder pkDataHolder;
    std::shared_ptr<EmbeddingHolder> embDataHolder;
    std::shared_ptr<EmbeddingHolder> trainEmbeddingHolder;
    {
        ScopedLatencyReporter reporter(_extractEmbeddingLatencyMetric);
        if (_indexConfig.buildConfig.storePrimaryKey) {
            ScopedLatencyReporter reporter(_extractPkLatencyMetric);
            PkDataExtractor pkExtractor(_mergeTask, _indexConfig.buildConfig.distributedBuild);
            ANN_CHECK(pkExtractor.Extract(indexSegments, pkDataHolder), "extract pk data failed");
        }
        EmbeddingExtractor embeddingExtractor(_indexConfig, _mergeTask, _indexConfig.buildConfig.distributedBuild);
        ANN_CHECK(embeddingExtractor.Extract(indexSegments, embAttrSegments, embDataHolder),
                  "extract embedding data failed");
        if (_indexConfig.buildConfig.distributedBuild && _indexConfig.buildConfig.builderName == QGRAPH_BUILDER) {
            // QGRAPH BUILDER 需要不区分聚心的全量数据做training
            EmbeddingExtractor trainEmbeddingExtractor(_indexConfig, MergeTask(), /*compactIndex*/ true);
            ANN_CHECK(trainEmbeddingExtractor.Extract(indexSegments, embAttrSegments, trainEmbeddingHolder),
                      "extract failed");
        }
    }
    for (auto segment : indexSegments) {
        segment->Close();
    }
    NormalSegmentBuilder builder(_indexConfig, _indexName, /*isMergeSegment*/ true, _metricReporter);
    ANN_CHECK(builder.Init(mergeResource), "init builder failed");
    ANN_CHECK(builder.BuildAndDump(pkDataHolder, embDataHolder, trainEmbeddingHolder, dumpDiretory), "build failed");
    pkDataHolder.Clear();
    embDataHolder->Clear();
    if (trainEmbeddingHolder) {
        trainEmbeddingHolder->Clear();
    }
    AUTIL_LOG(INFO, "dump merged index to directory[%s]", dumpDiretory->DebugString().c_str());
    return true;
}

bool NormalSegmentMerger::DistributedEndMerge(const std::vector<indexlib::file_system::DirectoryPtr>& directories,
                                              const indexlib::file_system::DirectoryPtr& outputDirectory)
{
    AiThetaReducerPtr reducer;
    ANN_CHECK(AiThetaFactoryWrapper::CreateReducer(_indexConfig, reducer), "create failed");
    vector<SegmentDataReaderPtr> totalReaders;
    for (const auto& directory : directories) {
        SegmentDataReaderPtr reader = std::make_shared<SegmentDataReader>();
        totalReaders.emplace_back(reader);
        if (!reader->Init(directory, false)) {
            AUTIL_LOG(ERROR, "segmentDataReader failed to be loaded");
            return false;
        }
        auto container =
            std::make_shared<aitheta2::CustomizedAiThetaContainer>(reader->GetIndexDataReader(kDefaultIndexId));
        ANN_CHECK_OK(container->init({}), "container init failed");
        ANN_CHECK_OK(container->load(), "container load failed");
        if (reducer->feed(container) != 0) {
            AUTIL_LOG(ERROR, "reducer failed to feed container");
            return false;
        }
    }
    if (reducer->reduce({}) != 0) {
        AUTIL_LOG(ERROR, "reducer failed to reduce");
        return false;
    }
    NormalSegment segment(outputDirectory, _indexConfig, false);
    auto segmentDataWriter = segment.GetSegmentDataWriter();
    auto indexDataWriter = segmentDataWriter->GetIndexDataWriter(kDefaultIndexId);
    if (!indexDataWriter) {
        AUTIL_LOG(ERROR, "failed to create indexDataWriter");
        return false;
    }

    ANN_CHECK_OK(aitheta2::CustomizedAiThetaDumper::dump(reducer, indexDataWriter), "dump failed");
    indexDataWriter->Close();
    IndexMeta indexMeta;
    indexMeta.docCount = reducer->stats().dumped_count();
    indexMeta.builderName = _indexConfig.buildConfig.builderName;
    indexMeta.searcherName = _indexConfig.searchConfig.searcherName;
    segment.AddIndexMeta(kDefaultIndexId, indexMeta);
    AUTIL_LOG(INFO, "dump merged segmentMeta successed, builderName [%s], searcherName [%s], docCount [%lu]",
              indexMeta.builderName.c_str(), indexMeta.searcherName.c_str(), indexMeta.docCount);
    if (!segment.DumpSegmentMeta()) {
        AUTIL_LOG(ERROR, "dump merged segmentMeta failed");
        return false;
    }
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
