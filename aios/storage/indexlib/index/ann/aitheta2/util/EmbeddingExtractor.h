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

#include "autil/Log.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingAttrSegment.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingHolder.h"

namespace indexlibv2::index::ann {
class EmbeddingAttrSegment;

class EmbeddingExtractor
{
public:
    EmbeddingExtractor(const AithetaIndexConfig& indexConfig, const MergeTask& mergeTask, bool compactIndex)
        : _indexConfig(indexConfig)
        , _mergeTask(mergeTask)
        , _compactIndex(compactIndex)
    {
    }
    ~EmbeddingExtractor() = default;

public:
    bool Extract(const std::vector<std::shared_ptr<NormalSegment>>& indexSegments,
                 const std::vector<std::shared_ptr<EmbeddingAttrSegment>>& attrSegments,
                 std::shared_ptr<EmbeddingHolder>& result);

private:
    bool DoExtract(const std::shared_ptr<NormalSegment>& indexSegment,
                   const std::shared_ptr<EmbeddingAttrSegment>& attrSegment, std::shared_ptr<EmbeddingHolder>& result);

    bool ExtractFromEmbeddingFile(const std::shared_ptr<NormalSegment>& embIndexSegment, EmbeddingHolder& result);
    bool ExtractFromEmbeddingAttribute(const std::shared_ptr<NormalSegment>& indexSegments,
                                       const std::shared_ptr<EmbeddingAttrSegment>& attrSegments,
                                       EmbeddingHolder& result);
    bool ExtractFromAithetaIndex(const std::shared_ptr<NormalSegment>& embIndexSegment, EmbeddingHolder& result);

    bool CreateEmbeddingIterator(const std::shared_ptr<NormalSegment>& segments, index_id_t indexId,
                                 const IndexMeta& indexMeta, aitheta2::IndexSearcher::Pointer& searcher,
                                 aitheta2::IndexHolder::Iterator::Pointer& iterator);

private:
    AithetaIndexConfig _indexConfig;
    MergeTask _mergeTask;
    bool _compactIndex = false;
    static constexpr size_t kDefaultExtractThreadCount = 8;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
