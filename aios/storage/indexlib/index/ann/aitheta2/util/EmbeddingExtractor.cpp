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
#include "indexlib/index/ann/aitheta2/util/EmbeddingExtractor.h"

#include "autil/LambdaWorkItem.h"
#include "autil/ThreadPool.h"
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
namespace indexlibv2::index::ann {

bool EmbeddingExtractor::Extract(const std::vector<std::shared_ptr<NormalSegment>>& indexSegments,
                                 const std::vector<std::shared_ptr<EmbeddingAttrSegment>>& attrSegments,
                                 std::shared_ptr<EmbeddingHolder>& result)
{
    AiThetaMeta meta;
    ANN_CHECK(ParamsInitializer::InitAiThetaMeta(_indexConfig, meta), "init meta failed");
    bool hasMultiIndexId = false;
    for (const auto& seg : indexSegments) {
        if (seg->GetSegmentMeta().GetIndexMetaMap().size() > 1) {
            hasMultiIndexId = true;
            break;
        }
    }

    size_t threadCount = std::min(kDefaultExtractThreadCount, indexSegments.size());
    size_t poolCount = indexSegments.size() * 2;
    autil::ThreadPool pool(threadCount, poolCount, true, "emb_extract");
    ANN_CHECK(pool.start(), "start emb extract thread pool failed");

    std::vector<std::shared_ptr<EmbeddingHolder>> holders(indexSegments.size());
    for (auto& holder : holders) {
        holder = std::make_shared<EmbeddingHolder>(meta, hasMultiIndexId);
    }

    std::atomic<bool> status = true;
    for (size_t idx = 0; idx < indexSegments.size(); ++idx) {
        auto* item = new LambdaWorkItem([this, &indexSegments, &attrSegments, &holders, idx, &status]() {
            auto indexSegment = indexSegments[idx];
            auto attrSegment = attrSegments.empty() ? nullptr : attrSegments[idx];
            if (!DoExtract(indexSegment, attrSegment, holders[idx])) {
                status.store(false);
            }
        });
        if (ThreadPool::ERROR_NONE != pool.pushWorkItem(item)) {
            ThreadPool::dropItemIgnoreException(item);
            AUTIL_LOG(ERROR, "push work item failed, stop extract");
            pool.stop();
            return false;
        }
    }

    pool.stop();
    ANN_CHECK(status, "extract failed");
    result = std::make_shared<EmbeddingHolder>(meta, hasMultiIndexId);
    for (const auto& holder : holders) {
        ANN_CHECK(result->Steal(*holder), "steal holder failed");
    }
    return true;
}

bool EmbeddingExtractor::DoExtract(const std::shared_ptr<NormalSegment>& indexSegment,
                                   const std::shared_ptr<EmbeddingAttrSegment>& attrSegment,
                                   std::shared_ptr<EmbeddingHolder>& result)
{
    auto segmentDataReader = indexSegment->GetSegmentDataReader();
    ANN_CHECK(segmentDataReader, "get segment data reader failed");
    if (_indexConfig.distanceType != HAMMING) {
        if (segmentDataReader->GetEmbeddingDataReader()) {
            return ExtractFromEmbeddingFile(indexSegment, *result);
        } else if (attrSegment != nullptr) {
            return ExtractFromEmbeddingAttribute(indexSegment, attrSegment, *result);
        }
    }
    return ExtractFromAithetaIndex(indexSegment, *result);
}

bool EmbeddingExtractor::ExtractFromEmbeddingFile(const std::shared_ptr<NormalSegment>& segments,
                                                  EmbeddingHolder& result)
{
    auto segDataReader = segments->GetSegmentDataReader();
    auto embFileReader = segDataReader->GetEmbeddingDataReader();
    embFileReader->Seek(0ul).GetOrThrow();

    auto& docIdMap = segments->GetDocMapWrapper();
    while (embFileReader->Tell() < embFileReader->GetLength()) {
        EmbeddingFileHeader header {};
        ANN_CHECK(sizeof(header) == embFileReader->Read(&header, sizeof(header)).GetOrThrow(),
                  "read embedding data header failed");

        if (!_mergeTask.empty() && _mergeTask.find(header.indexId) == _mergeTask.end()) {
            int64_t size = (sizeof(docid_t) + sizeof(float) * _indexConfig.dimension) * header.count;
            int64_t offset = embFileReader->Tell() + size;
            embFileReader->Seek(offset).GetOrThrow();
            continue;
        }

        for (uint64_t i = 0; i < header.count; ++i) {
            docid_t docId = INVALID_DOCID;
            ANN_CHECK(sizeof(docId) == embFileReader->Read(&docId, sizeof(docId)).GetOrThrow(), "read failed");

            if (docIdMap) {
                docId = docIdMap->GetNewDocId(docId);
            }
            if (docId == INVALID_DOCID) {
                size_t size = sizeof(float) * _indexConfig.dimension;
                int64_t offset = embFileReader->Tell() + size;
                embFileReader->Seek(offset).GetOrThrow();
                continue;
            }

            string emb;
            size_t size = sizeof(float) * _indexConfig.dimension;
            emb.reserve(size);
            ANN_CHECK(size == embFileReader->Read(emb.data(), size).GetOrThrow(), "read failed");
            ANN_CHECK(result.Add(emb.data(), docId, _compactIndex ? kDefaultIndexId : header.indexId),
                      "add doc failed");
        }
        AUTIL_LOG(INFO, "end extract embedding with index id[%ld]", header.indexId);
    }
    return true;
}

bool EmbeddingExtractor::ExtractFromAithetaIndex(const std::shared_ptr<NormalSegment>& indexSegment,
                                                 EmbeddingHolder& result)
{
    for (auto& [indexId, indexMeta] : indexSegment->GetSegmentMeta().GetIndexMetaMap()) {
        if (!_mergeTask.empty() && _mergeTask.find(indexId) == _mergeTask.end()) {
            continue;
        }

        aitheta2::IndexSearcher::Pointer searcher;
        aitheta2::IndexHolder::Iterator::Pointer iterator;
        ANN_CHECK(CreateEmbeddingIterator(indexSegment, indexId, indexMeta, searcher, iterator),
                  "create iterator failed");

        auto docIdMapWrapper = indexSegment->GetDocMapWrapper();
        for (; iterator->is_valid(); iterator->next()) {
            docid_t docId = static_cast<docid_t>(iterator->key());
            if (docIdMapWrapper) {
                docId = docIdMapWrapper->GetNewDocId(docId);
            }
            if (docId == INVALID_DOCID) {
                continue;
            }
            auto emb = reinterpret_cast<const char*>(iterator->data());
            ANN_CHECK(result.Add(emb, docId, _compactIndex ? kDefaultIndexId : indexId), "add doc failed");
        }
    }
    return true;
}

bool EmbeddingExtractor::ExtractFromEmbeddingAttribute(const std::shared_ptr<NormalSegment>& indexSegment,
                                                       const std::shared_ptr<EmbeddingAttrSegment>& attrSegment,
                                                       EmbeddingHolder& result)
{
    std::map<docid_t, index_id_t> oldDocIds;
    for (auto& [indexId, indexMeta] : indexSegment->GetSegmentMeta().GetIndexMetaMap()) {
        if (!_mergeTask.empty() && _mergeTask.find(indexId) == _mergeTask.end()) {
            continue;
        }
        aitheta2::IndexSearcher::Pointer searcher;
        aitheta2::IndexHolder::Iterator::Pointer iterator;
        ANN_CHECK(CreateEmbeddingIterator(indexSegment, indexId, indexMeta, searcher, iterator),
                  "create iterator failed");

        for (; iterator->is_valid(); iterator->next()) {
            docid_t docId = static_cast<docid_t>(iterator->key());
            oldDocIds[docId] = indexId;
        }
    }
    return attrSegment->Extract(oldDocIds, indexSegment->GetDocMapWrapper(), _compactIndex, result);
}

bool EmbeddingExtractor::CreateEmbeddingIterator(const std::shared_ptr<NormalSegment>& indexSegment, index_id_t indexId,
                                                 const IndexMeta& indexMeta, aitheta2::IndexSearcher::Pointer& searcher,
                                                 aitheta2::IndexHolder::Iterator::Pointer& iterator)
{
    auto segDataReader = indexSegment->GetSegmentDataReader();
    ANN_CHECK(segDataReader, "get segment data reader failed");

    auto indexDataReader = segDataReader->GetIndexDataReader(indexId);
    ANN_CHECK(indexDataReader, "get index data reader[%ld]", indexId);

    IndexMeta updatedIndexMeta = indexMeta;
    if (indexMeta.searcherName.find(GPU_PREFIX) != std::string::npos) {
        size_t pos = GPU_PREFIX.size();
        updatedIndexMeta.searcherName = indexMeta.searcherName.substr(pos);
    }

    ANN_CHECK(AiThetaFactoryWrapper::CreateSearcher(_indexConfig, updatedIndexMeta, indexDataReader, searcher),
              "create searcher failed");
    auto provider = searcher->create_provider();
    ANN_CHECK(provider, "create  provider failed");
    iterator = provider->create_iterator();
    ANN_CHECK(iterator, "create iterator failed");
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, EmbeddingExtractor);

} // namespace indexlibv2::index::ann
