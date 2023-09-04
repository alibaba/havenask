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
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataHolder.h"

namespace indexlibv2::index::ann {
class EmbeddingAttrSegmentBase;

class EmbeddingDataExtractor
{
public:
    EmbeddingDataExtractor(const AithetaIndexConfig& indexConfig, const MergeTask& mergeTask)
        : _indexConfig(indexConfig)
        , _mergeTask(mergeTask)
    {
    }
    virtual ~EmbeddingDataExtractor() = default;

public:
    virtual bool Extract(const std::vector<NormalSegmentPtr>& embIndexSegments,
                         const std::vector<std::shared_ptr<EmbeddingAttrSegmentBase>>& embAttrSegments,
                         EmbeddingDataHolder& result);

    virtual bool ExtractFromEmbeddingAttr(const NormalSegmentPtr& embIndexSegment,
                                          std::shared_ptr<EmbeddingAttrSegmentBase> embAttrSegment,
                                          EmbeddingDataHolder& result);

protected:
    AiThetaSearcherPtr CreateAithetaSearcher(const NormalSegmentPtr& indexSegment, const IndexMeta& indexMeta,
                                             index_id_t indexId);

private:
    bool ExtractFromEmbeddingData(const NormalSegmentPtr& embIndexSegment, EmbeddingDataHolder& result);
    bool ExtractFromAithetaIndex(const NormalSegmentPtr& embIndexSegment, EmbeddingDataHolder& result);

protected:
    AithetaIndexConfig _indexConfig;
    MergeTask _mergeTask;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
