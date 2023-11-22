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

#include "aios/storage/indexlib/index/ann/aitheta2/proto/AithetaQuery.pb.h"
#include "autil/Log.h"

namespace indexlibv2::index::ann {

class AithetaQueryWrapper
{
public:
    AithetaQueryWrapper() = default;
    ~AithetaQueryWrapper() = default;

public:
    void AddAithetaQuery(int64_t indexId, const float* embeddings, uint32_t embeddingDim, uint32_t embeddingCnt,
                         size_t topk, float scorethreshold = InvalidScoreThreshold,
                         const std::string& searchParams = "");

    void AddAithetaQuery(const float* embeddings, uint32_t embeddingDim, uint32_t embeddingCnt, size_t topk,
                         float scorethreshold = InvalidScoreThreshold, const std::string& searchParams = "");

    void AddQueryTag(const std::string& key, const std::string& value)
    {
        (*_aithetaQueries.mutable_querytags())[key] = value;
    }

public:
    // CustomIndexQuery will be seriazlized to Term::_word
    bool SerializeToString(std::string& word);
    bool ParseFromString(const std::string& word);
    static bool IsAithetaQuery(const std::string& word);
    bool ValidateDimension(uint32_t expectedDim) const;
    AithetaQueries& GetAithetaQueries() { return _aithetaQueries; }

public:
    static constexpr const float InvalidScoreThreshold = std::numeric_limits<float>::lowest();

private:
    static constexpr const char* MAGIC_NUMBER = "_aC93@#_";
    AithetaQueries _aithetaQueries;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
