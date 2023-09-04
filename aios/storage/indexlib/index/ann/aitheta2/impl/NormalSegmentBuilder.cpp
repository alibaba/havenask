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
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentBuilder.h"

#include "indexlib/index/ann/aitheta2/impl/NormalIndexBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataDumper.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

using namespace std;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

bool NormalSegmentBuilder::Init(const SegmentBuildResourcePtr&)
{
    AiThetaMeta aiThetaMeta;
    ANN_CHECK(ParamsInitializer::InitAiThetaMeta(_indexConfig, aiThetaMeta), "init aitheta meta failed");
    _embDataHolder.reset(new EmbeddingDataHolder(aiThetaMeta));
    return true;
}

bool NormalSegmentBuilder::Build(const EmbeddingFieldData& data)
{
    if (_pkDataHolder.IsExist(data.pk)) {
        docid_t docId = INVALID_DOCID;
        vector<index_id_t> indexIds;
        _pkDataHolder.Remove(data.pk, docId, indexIds);
        ANN_CHECK(_embDataHolder->Remove(docId, indexIds), "delete outdated docId[%d] primary key[%ld] failed", docId,
                  data.pk);
    } else {
        size_t estimatedMemoryCost = sizeof(float) * _indexConfig.dimension * data.indexIds.size();
        IncCurrentMemoryUse(estimatedMemoryCost);
    }

    _pkDataHolder.Add(data.pk, data.docId, data.indexIds);
    _embDataHolder->Add(data.embedding, data.docId, data.indexIds);

    AUTIL_INTERVAL_LOG(10000, INFO, "current pk count[%lu], repeated pk count[%lu]", _pkDataHolder.PkCount(),
                       _pkDataHolder.DeletedPkCount());
    return true;
}

bool NormalSegmentBuilder::Dump(const indexlib::file_system::DirectoryPtr& directory,
                                const std::vector<docid_t>* old2NewDocId)
{
    if (nullptr != old2NewDocId) {
        _embDataHolder->SetDocIdMapper(old2NewDocId);
        _pkDataHolder.SetDocIdMapper(old2NewDocId);
    }
    ANN_CHECK(BuildAndDump(_pkDataHolder, *_embDataHolder, directory), "build failed");
    _pkDataHolder.Clear();
    _embDataHolder->Clear();
    _currentMemoryUse = 0;
    return true;
}

bool NormalSegmentBuilder::BuildAndDump(const PkDataHolder& pkDataHolder, const EmbeddingDataHolder& embDataHolder,
                                        const indexlib::file_system::DirectoryPtr& directory)
{
    NormalSegment segment(directory, _indexConfig, false);
    segment.SetMergedSegment();

    ANN_CHECK(BuildAndDumpIndex(embDataHolder, segment), "build index and dump failed");
    ANN_CHECK(DumpPrimaryKey(pkDataHolder, segment), "dump primary key failed");
    ANN_CHECK(DumpEmbeddingData(embDataHolder, segment), "dump embedding data failed");
    ANN_CHECK(segment.DumpSegmentMeta(), "meta dump failed");

    ANN_CHECK(segment.Close(), "segment close failed");
    return true;
}

bool NormalSegmentBuilder::DumpEmbeddingData(const EmbeddingDataHolder& holder, NormalSegment& segment)
{
    if (!_indexConfig.buildConfig.storeEmbedding || _indexConfig.buildConfig.builderName == LINEAR_BUILDER) {
        AUTIL_LOG(INFO, "not need to store embedding data");
        return true;
    }
    EmbeddingDataDumper dumper;
    return dumper.Dump(holder, segment);
}

bool NormalSegmentBuilder::BuildAndDumpIndex(const EmbeddingDataHolder& embDataHolder, NormalSegment& segment)
{
    for (auto& [indexId, embData] : embDataHolder.GetEmbeddingDataMap()) {
        if (embData->count() == 0) {
            continue;
        }

        NormalIndexBuilder builder(_indexConfig, _metricReporter);
        auto segDataWriter = segment.GetSegmentDataWriter();
        ANN_CHECK(segDataWriter != nullptr, "get segment data writer failed");
        auto indexDataWriter = segDataWriter->GetIndexDataWriter(indexId);
        ANN_CHECK(indexDataWriter != nullptr, "create index data writer failed");
        ANN_CHECK(builder.BuildAndDump(embData, indexDataWriter), "index build failed");
        indexDataWriter->Close();

        auto& indexMeta = builder.GetIndexMeta();
        ANN_CHECK(segment.AddIndexMeta(indexId, indexMeta), "index dump failed");
    }

    return true;
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegmentBuilder);
} // namespace indexlibv2::index::ann
