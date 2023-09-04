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
#include "indexlib/index/ann/aitheta2/AithetaQueryWrapper.h"

#include <cmath>
namespace indexlibv2::index::ann {

bool AithetaQueryWrapper::SerializeToString(std::string& word)
{
    if (!_aithetaQueries.SerializeToString(&word)) {
        return false;
    }
    word.append(MAGIC_NUMBER);
    return true;
}

bool AithetaQueryWrapper::IsAithetaQuery(const std::string& word)
{
    size_t magicNumberLen = std::strlen(AithetaQueryWrapper::MAGIC_NUMBER);
    if (word.size() < magicNumberLen) {
        return false;
    }
    size_t queryLen = word.size() - magicNumberLen;
    return word.substr(queryLen) == std::string(AithetaQueryWrapper::MAGIC_NUMBER);
}

bool AithetaQueryWrapper::ParseFromString(const std::string& word)
{
    if (!IsAithetaQuery(word)) {
        AUTIL_LOG(ERROR, "parse failed as [%s] is not custom index word", word.c_str());
        return false;
    }
    size_t magicNumberLen = std::strlen(MAGIC_NUMBER);
    size_t queryLen = word.size() - magicNumberLen;
    return _aithetaQueries.ParseFromString(word.substr(0, queryLen));
}

void AithetaQueryWrapper::AddAithetaQuery(const float* embeddings, uint32_t embeddingDim, uint32_t embeddingCnt,
                                          size_t topk, float scoreThreshold, const std::string& searchParams)
{
    int64_t defaultIndexId = std::numeric_limits<int64_t>::max();
    AddAithetaQuery(defaultIndexId, embeddings, embeddingDim, embeddingCnt, topk, scoreThreshold, searchParams);
}

void AithetaQueryWrapper::AddAithetaQuery(int64_t indexId, const float* embeddings, uint32_t embeddingDim,
                                          uint32_t embeddingCnt, size_t topk, float scoreThreshold,
                                          const std::string& searchParams)
{
    int32_t idx = (int32_t)_aithetaQueries.aithetaqueries_size() - 1;
    for (; idx >= 0; --idx) {
        // find first query which shares the same parameters: topk & indexid & scorethreshold
        if (_aithetaQueries.aithetaqueries(idx).indexid() == indexId &&
            _aithetaQueries.aithetaqueries(idx).topk() == topk &&
            _aithetaQueries.aithetaqueries(idx).scorethreshold() == scoreThreshold &&
            _aithetaQueries.aithetaqueries(idx).searchparams() == searchParams) {
            break;
        }
    }
    if (idx >= 0) {
        auto query = _aithetaQueries.mutable_aithetaqueries(idx);
        for (auto pointer = embeddings; pointer != embeddings + embeddingDim * embeddingCnt; ++pointer) {
            query->mutable_embeddings()->Add(*pointer);
        }
        size_t cnt = embeddingCnt + query->embeddingcount();
        query->set_embeddingcount(cnt);
        if (scoreThreshold != InvalidScoreThreshold) {
            query->set_scorethreshold(scoreThreshold);
            query->set_hasscorethreshold(true);
        } else {
            query->set_scorethreshold(InvalidScoreThreshold);
            query->set_hasscorethreshold(false);
        }
        if (!searchParams.empty()) {
            query->set_searchparams(searchParams);
        }
    } else {
        auto query = _aithetaQueries.add_aithetaqueries();
        query->set_indexid(indexId);
        query->set_topk(topk);
        query->set_embeddingcount(embeddingCnt);
        if (scoreThreshold != InvalidScoreThreshold) {
            query->set_scorethreshold(scoreThreshold);
            query->set_hasscorethreshold(true);
        } else {
            query->set_scorethreshold(InvalidScoreThreshold);
            query->set_hasscorethreshold(false);
        }
        if (!searchParams.empty()) {
            query->set_searchparams(searchParams);
        }
        *query->mutable_embeddings() = {embeddings, embeddings + embeddingDim * embeddingCnt};
    }
}

bool AithetaQueryWrapper::ValidateDimension(uint32_t expectedDim) const
{
    for (auto& query : _aithetaQueries.aithetaqueries()) {
        if (query.embeddings_size() == query.embeddingcount() * expectedDim) {
            continue;
        }
        AUTIL_LOG(ERROR,
                  "validate dimension failed, expected dimension[%u],"
                  " actual dimension[%d] with custom index query[%s]",
                  expectedDim, query.embeddings_size(), _aithetaQueries.DebugString().c_str());
        return false;
    }
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, AithetaQueryWrapper);

} // namespace indexlibv2::index::ann
