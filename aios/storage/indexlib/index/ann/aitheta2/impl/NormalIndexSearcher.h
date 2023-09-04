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

#include "autil/Lock.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/IndexSearcher.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/AiThetaContextHolder.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataHolder.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/ann/aitheta2/util/QueryParser.h"
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

namespace indexlibv2::index::ann {

class NormalIndexSearcher : public IndexSearcher
{
public:
    NormalIndexSearcher(const IndexMeta& meta, const AithetaIndexConfig& config, const IndexDataReaderPtr& reader,
                        const AiThetaContextHolderPtr& holder);
    ~NormalIndexSearcher();

public:
    bool Init() override;
    bool Search(const AithetaQuery& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& info,
                ResultHolder& resultHolder) override;

protected:
    bool ParseQueryParameter(const std::string& searchParams, AiThetaParams& aiThetaParams) const override;

protected:
    IndexMeta _indexMeta;
    IndexDataReaderPtr _indexDataReader;
    AiThetaSearcherPtr _indexSearcher;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalIndexSearcher> NormalIndexSearcherPtr;

typedef std::unordered_map<index_id_t, NormalIndexSearcherPtr> IndexSearcherMap;

} // namespace indexlibv2::index::ann
