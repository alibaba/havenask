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
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataExtractor.h"

#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingAttrSegment.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
namespace indexlibv2::index::ann {

bool EmbeddingDataExtractor::Extract(const std::vector<NormalSegmentPtr>& indexSegments,
                                     const std::vector<std::shared_ptr<EmbeddingAttrSegmentBase>>& embAttrSegments,
                                     EmbeddingDataHolder& result)
{
    for (size_t i = 0; i < indexSegments.size(); ++i) {
        auto segDataReader = indexSegments[i]->GetSegmentDataReader();
        ANN_CHECK(segDataReader, "get segment data embDataReader failed");
        if (segDataReader->GetEmbeddingDataReader()) {
            ANN_CHECK(ExtractFromEmbeddingData(indexSegments[i], result),
                      "extract from embedding data embDataReader failed");
        } else if (embAttrSegments[i] == nullptr) {
            ANN_CHECK(ExtractFromAithetaIndex(indexSegments[i], result), "extract from aitheta index failed.");
        } else {
            auto extractor = embAttrSegments[i]->CreateEmbeddingExtractor(_indexConfig, _mergeTask);
            ANN_CHECK(extractor->ExtractFromEmbeddingAttr(indexSegments[i], embAttrSegments[i], result),
                      "extract embedding from attr failed.");
        }
    }
    return true;
}

bool EmbeddingDataExtractor::ExtractFromEmbeddingData(const NormalSegmentPtr& indexSegment, EmbeddingDataHolder& result)
{
    auto segDataReader = indexSegment->GetSegmentDataReader();
    auto embDataReader = segDataReader->GetEmbeddingDataReader();
    embDataReader->Seek(0ul).GetOrThrow();

    auto& docIdMap = indexSegment->GetDocMapWrapper();
    while (embDataReader->Tell() < embDataReader->GetLength()) {
        EmbeddingDataHeader header {};
        ANN_CHECK(sizeof(header) == embDataReader->Read(&header, sizeof(header)).GetOrThrow(),
                  "read embedding data header failed");

        if (!_mergeTask.empty() && _mergeTask.find(header.indexId) == _mergeTask.end()) {
            int64_t size = (sizeof(docid_t) + sizeof(float) * _indexConfig.dimension) * header.count;
            int64_t offset = embDataReader->Tell() + size;
            embDataReader->Seek(offset).GetOrThrow();
            AUTIL_LOG(INFO, "ignore embedding data from index id[%ld]", header.indexId);
            continue;
        }

        for (uint64_t i = 0; i < header.count; ++i) {
            docid_t docId = INVALID_DOCID;
            ANN_CHECK(sizeof(docId) == embDataReader->Read(&docId, sizeof(docId)).GetOrThrow(), "read docId failed");

            docid_t newDocId = docIdMap->GetNewDocId(docId);
            if (newDocId == INVALID_DOCID) {
                size_t size = sizeof(float) * _indexConfig.dimension;
                int64_t offset = embDataReader->Tell() + size;
                embDataReader->Seek(offset).GetOrThrow();
                continue;
            }

            embedding_t embedding = nullptr;
            embedding.reset(new (std::nothrow) float[_indexConfig.dimension], [](float* p) { delete[] p; });
            if (nullptr == embedding) {
                AUTIL_LOG(ERROR, "alloc size[%lu] in type[float] failed", (size_t)_indexConfig.dimension);
                return false;
            }
            size_t length = sizeof(float) * _indexConfig.dimension;
            ANN_CHECK(length == embDataReader->Read(embedding.get(), length).GetOrThrow(), "read embedding failed");
            ANN_CHECK(result.Add(embedding, newDocId, header.indexId), "add doc failed");
        }
        AUTIL_LOG(INFO, "extract embedding data from index id[%ld]", header.indexId);
    }
    return true;
}

bool EmbeddingDataExtractor::ExtractFromAithetaIndex(const NormalSegmentPtr& indexSegment, EmbeddingDataHolder& result)
{
    for (auto& [indexId, indexMeta] : indexSegment->GetSegmentMeta().GetIndexMetaMap()) {
        if (!_mergeTask.empty() && _mergeTask.find(indexId) == _mergeTask.end()) {
            continue;
        }
        auto docIdMapWrapper = indexSegment->GetDocMapWrapper();
        AiThetaSearcherPtr searcher = CreateAithetaSearcher(indexSegment, indexMeta, indexId);
        ANN_CHECK(nullptr != searcher, "searcher is nullptr.");
        auto provider = searcher->create_provider();
        ANN_CHECK(nullptr != provider, "provider is nullptr.");
        auto iterator = provider->create_iterator();
        ANN_CHECK(nullptr != iterator, "iterator is nullptr.");
        for (; iterator->is_valid(); iterator->next()) {
            docid_t docId = static_cast<docid_t>(iterator->key());
            docid_t newDocId = docIdMapWrapper->GetNewDocId(docId);
            if (newDocId == INVALID_DOCID) {
                continue;
            }
            embedding_t embedding = nullptr;
            uint32_t dimension = _indexConfig.dimension;
            embedding.reset(new (std::nothrow) float[dimension], [](float* p) { delete[] p; });
            if (nullptr == embedding) {
                AUTIL_LOG(ERROR, "alloc size[%lu] in type[float] failed", (size_t)dimension);
                return false;
            }
            auto indexVal = reinterpret_cast<const float*>(iterator->data());
            std::copy_n(indexVal, dimension, embedding.get());
            ANN_CHECK(result.Add(embedding, newDocId, indexId), "add doc failed");
        }
    }
    return true;
}

bool EmbeddingDataExtractor::ExtractFromEmbeddingAttr(const NormalSegmentPtr& embIndexSegment,
                                                      std::shared_ptr<EmbeddingAttrSegmentBase> embAttrSegment,
                                                      EmbeddingDataHolder& result)
{
    AUTIL_LOG(ERROR, "ann index not support extract embedding from attribute");
    return false;
}

AiThetaSearcherPtr EmbeddingDataExtractor::CreateAithetaSearcher(const NormalSegmentPtr& indexSegment,
                                                                 const IndexMeta& indexMeta, index_id_t indexId)
{
    auto segDataReader = indexSegment->GetSegmentDataReader();
    if (segDataReader == nullptr) {
        AUTIL_LOG(ERROR, "get segment data reader failed");
        return nullptr;
    }
    auto indexDataReader = segDataReader->GetIndexDataReader(indexId);
    if (indexDataReader == nullptr) {
        AUTIL_LOG(ERROR, "get index data reader failed");
        return nullptr;
    }
    IndexMeta updatedIndexMeta = indexMeta;
    if (indexMeta.searcherName.find(GPU_PREFIX) != std::string::npos) {
        size_t pos = GPU_PREFIX.size();
        updatedIndexMeta.searcherName = indexMeta.searcherName.substr(pos);
    }
    AiThetaSearcherPtr searcher;
    if (!AiThetaFactoryWrapper::CreateSearcher(_indexConfig, updatedIndexMeta, indexDataReader, searcher)) {
        AUTIL_LOG(ERROR, "create searcher failed.");
        return nullptr;
    }
    return searcher;
}

AUTIL_LOG_SETUP(indexlib.index, EmbeddingDataExtractor);

} // namespace indexlibv2::index::ann
