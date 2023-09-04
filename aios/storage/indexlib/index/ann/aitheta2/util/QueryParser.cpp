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
#include "indexlib/index/ann/aitheta2/util/QueryParser.h"

#include "autil/URLUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
namespace indexlibv2::index::ann {

Status QueryParser::Parse(const AithetaIndexConfig& indexConf, const std::string& term, AithetaQueries& query)
{
    AithetaQueryWrapper wrapper;
    if (AithetaQueryWrapper::IsAithetaQuery(term)) {
        if (!wrapper.ParseFromString(term)) {
            RETURN_STATUS_ERROR(InvalidArgs, "deserialize[%s] failed", term.c_str());
        }
        if (!wrapper.ValidateDimension(indexConf.dimension)) {
            RETURN_STATUS_ERROR(InvalidArgs, "validate dimension failed");
        }
    } else {
        auto str = URLUtil::decode(term);
        RETURN_IF_STATUS_ERROR(ParseFromRawString(indexConf, str, wrapper), "deserialize[%s] failed", term.c_str());
    }
    query.Swap(&wrapper.GetAithetaQueries());
    return Status::OK();
}

// query format: category_id0#embedding0;category_id1#embedding1&n=100&sf=0.1
Status QueryParser::ParseFromRawString(const AithetaIndexConfig& indexConf, const std::string& rawTerm,
                                       AithetaQueryWrapper& wrapper)
{
    string mutableStr = rawTerm;
    string searchParams;
    RETURN_IF_STATUS_ERROR(ParseValue(mutableStr, QueryParser::QUERY_SEARCH_PARAMS_KEY, searchParams),
                           "parse search params failed");

    float scoreThreshold = AithetaQueryWrapper::InvalidScoreThreshold;
    RETURN_IF_STATUS_ERROR(ParseValue(mutableStr, kScoreThresholdKey, scoreThreshold), "parse score threshold failed");

    uint32_t topk = 100u;
    RETURN_IF_STATUS_ERROR(ParseValue(mutableStr, kTopkKey, topk), "parse topk failed");
    std::map<index_id_t, std::vector<float>> embeddingMap;
    RETURN_IF_STATUS_ERROR(ParseEmbedding(indexConf, mutableStr, embeddingMap), "parse embedding failed");
    for (const auto& [indexId, embeddings] : embeddingMap) {
        size_t embeddingCnt = embeddings.size() / indexConf.dimension;
        auto embeddingsHead = embeddings.data();
        wrapper.AddAithetaQuery(indexId, embeddingsHead, indexConf.dimension, embeddingCnt, topk, scoreThreshold,
                                searchParams);
    }
    return Status::OK();
}

Status QueryParser::ParseEmbedding(const AithetaIndexConfig& indexConf, const std::string& queryStr,
                                   std::map<index_id_t, std::vector<float>>& embeddingMap)
{
    auto tokens = StringUtil::split(queryStr, kQueryDelimiter);
    for (const auto& token : tokens) {
        vector<index_id_t> indexIds;
        vector<float> embedding;
        auto subTokens = StringUtil::split(token, kIndexIdEmbeddingDelimiter);
        if (subTokens.size() == 1) {
            indexIds.push_back(kDefaultIndexId);
            StringUtil::fromString(subTokens[0], embedding, indexConf.embeddingDelimiter);
        } else if (subTokens.size() == 2) {
            StringUtil::fromString(subTokens[0], indexIds, kIndexIdDelimiter);
            StringUtil::fromString(subTokens[1], embedding, indexConf.embeddingDelimiter);
        }
        if (embedding.size() != indexConf.dimension) {
            RETURN_STATUS_ERROR(InvalidArgs, "dimension mismatch with index's");
        }
        for_each(indexIds.begin(), indexIds.end(), [&](index_id_t indexId) {
            auto& embeddings = embeddingMap[indexId];
            embeddings.insert(embeddings.end(), embedding.begin(), embedding.end());
        });
    }
    return Status::OK();
}

AUTIL_LOG_SETUP(indexlib.index, QueryParser);

} // namespace indexlibv2::index::ann
