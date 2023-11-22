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

#include "autil/Scope.h"
#include "indexlib/index/ann/aitheta2/impl/NormalIndexBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDumper.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

using namespace std;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

NormalSegmentBuilder::NormalSegmentBuilder(const AithetaIndexConfig& indexConfig, const std::string& indexName,
                                           bool isMergeBuild, const MetricReporterPtr& metricReporter)
    : SegmentBuilder(indexConfig, indexName, metricReporter)
    , _currentMemoryUse(0ul)
    , _isMergeBuild(isMergeBuild)
{
}
NormalSegmentBuilder::~NormalSegmentBuilder()
{
    for (auto ckptManager : _ckptManagers) {
        [[maybe_unused]] int error = ckptManager->cleanup();
    }
}

bool NormalSegmentBuilder::Init(const SegmentBuildResourcePtr& buildResource)
{
    AiThetaMeta aiThetaMeta;
    ANN_CHECK(ParamsInitializer::InitAiThetaMeta(_indexConfig, aiThetaMeta), "init aitheta meta failed");
    _embeddingHolder.reset(new EmbeddingHolder(aiThetaMeta));
    if (buildResource == nullptr) {
        return true;
    }
    _buildResource = std::dynamic_pointer_cast<NormalSegmentBuildResource>(buildResource);
    if (_buildResource == nullptr) {
        AUTIL_LOG(ERROR, "cast to NormalSegmentBuildResource failed");
        return false;
    }
    return true;
}

bool NormalSegmentBuilder::Build(const IndexFields& data)
{
    if (_pkHolder.IsExist(data.pk)) {
        docid_t docId = INVALID_DOCID;
        vector<index_id_t> indexIds;
        _pkHolder.Remove(data.pk, docId, indexIds);
        ANN_CHECK(_embeddingHolder->Delete(docId, indexIds), "delete outdated docId[%d] primary key[%ld] failed", docId,
                  data.pk);
    } else {
        size_t estimatedMemoryCost = sizeof(float) * _indexConfig.dimension * data.indexIds.size();
        IncCurrentMemoryUse(estimatedMemoryCost);
    }

    _pkHolder.Add(data.pk, data.docId, data.indexIds);
    _embeddingHolder->Add(reinterpret_cast<const char*>(data.embedding.get()), data.docId, data.indexIds);

    AUTIL_INTERVAL_LOG(10000, INFO, "current pk count[%lu], repeated pk count[%lu]", _pkHolder.PkCount(),
                       _pkHolder.DeletedPkCount());
    return true;
}

bool NormalSegmentBuilder::Dump(const indexlib::file_system::DirectoryPtr& directory,
                                const std::vector<docid_t>* old2NewDocId)
{
    if (nullptr != old2NewDocId) {
        _embeddingHolder->SetDocIdMapper(old2NewDocId);
        _pkHolder.SetDocIdMapper(old2NewDocId);
    }
    ANN_CHECK(BuildAndDump(_pkHolder, _embeddingHolder, nullptr, directory), "build failed");
    _pkHolder.Clear();
    _embeddingHolder->Clear();
    _currentMemoryUse = 0;
    return true;
}

bool NormalSegmentBuilder::BuildAndDump(PkDataHolder& pkDataHolder,
                                        const std::shared_ptr<EmbeddingHolder>& embeddingHolder,
                                        const std::shared_ptr<EmbeddingHolder>& trainEmbeddingHolder,
                                        const indexlib::file_system::DirectoryPtr& directory)
{
    NormalSegment segment(directory, _indexConfig, false);
    segment.SetMergedSegment(_isMergeBuild);

    ANN_CHECK(DumpPrimaryKey(pkDataHolder, segment), "dump primary key failed");
    ANN_CHECK(DumpEmbedding(embeddingHolder, segment), "dump embedding data failed");
    ANN_CHECK(BuildAndDumpIndex(embeddingHolder, trainEmbeddingHolder, segment), "build index and dump failed");
    ANN_CHECK(segment.DumpSegmentMeta(), "meta dump failed");

    ANN_CHECK(segment.Close(), "segment close failed");
    return true;
}

bool NormalSegmentBuilder::DumpEmbedding(const std::shared_ptr<EmbeddingHolder>& holder, NormalSegment& segment)
{
    if (!_indexConfig.buildConfig.storeEmbedding || _indexConfig.buildConfig.builderName == LINEAR_BUILDER ||
        _indexConfig.distanceType == HAMMING) {
        AUTIL_LOG(INFO, "not need to store embedding data");
        return true;
    }
    EmbeddingDumper dumper;
    return dumper.Dump(*holder, segment);
}

bool NormalSegmentBuilder::BuildAndDumpIndex(const std::shared_ptr<EmbeddingHolder>& embeddingHolder,
                                             const std::shared_ptr<EmbeddingHolder>& trainEmbeddingHolder,
                                             NormalSegment& segment)
{
    for (auto& [indexId, buffers] : embeddingHolder->GetMutableBufferMap()) {
        if (buffers->count() == 0) {
            AUTIL_LOG(WARN, "index id[%ld] has no emb data, skip build", indexId);
            continue;
        }

        NormalIndexBuilder builder(_indexConfig, _metricReporter);
        auto segDataWriter = segment.GetSegmentDataWriter();
        ANN_CHECK(segDataWriter != nullptr, "get segment data writer failed");
        auto indexDataWriter = segDataWriter->GetIndexDataWriter(indexId);
        ANN_CHECK(indexDataWriter != nullptr, "create index data writer failed");
        auto ckptManager = CreateIndexCkptManager(indexId);
        ANN_CHECK(builder.Init(buffers->count()), "builder init failed");
        if (!trainEmbeddingHolder) {
            ANN_CHECK(builder.Train(buffers, ckptManager), "train builder failed");
        } else {
            AUTIL_LOG(INFO, "builder index id [%lu]", indexId);
            auto iter = trainEmbeddingHolder->GetMutableBufferMap().find(kDefaultIndexId);
            ANN_CHECK(iter != trainEmbeddingHolder->GetMutableBufferMap().end(), "find train data failed");
            ANN_CHECK(builder.Train(iter->second, ckptManager), "train builder failed");
        }
        ANN_CHECK(builder.BuildAndDump(buffers, ckptManager, indexDataWriter), "index build failed");
        indexDataWriter->Close();

        auto& indexMeta = builder.GetIndexMeta();
        ANN_CHECK(segment.AddIndexMeta(indexId, indexMeta), "index dump failed");
    }

    return true;
}

std::shared_ptr<aitheta2::CustomizedCkptManager> NormalSegmentBuilder::CreateIndexCkptManager(int64_t indexId)
{
    if (!_buildResource) {
        return nullptr;
    }
    auto resoureDirectory = _buildResource->GetResourceDirectory();
    auto ckptManager = std::make_shared<aitheta2::CustomizedCkptManager>(resoureDirectory);
    aitheta2::IndexParams buildParams;
    if (!aitheta2::IndexParams::ParseFromBuffer(_indexConfig.buildConfig.indexParams, &buildParams)) {
        AUTIL_LOG(ERROR, "parse params from[%s] failed", _indexConfig.buildConfig.indexParams.c_str());
        return nullptr;
    }
    std::string ckptDirectoryName = "ckpt_" + std::to_string(indexId);
    [[maybe_unused]] int error = ckptManager->init(buildParams, ckptDirectoryName);
    _ckptManagers.push_back(ckptManager);
    return ckptManager;
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegmentBuilder);
} // namespace indexlibv2::index::ann
