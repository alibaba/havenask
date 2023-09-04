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

#include "autil/Scope.h"
#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfo.h"
#include "indexlib/index/ann/aitheta2/AithetaFilterCreator.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/AiThetaContextHolder.h"
#include "indexlib/index/ann/aitheta2/util/QueryParser.h"
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializerFactory.h"

namespace indexlibv2::index::ann {
class IndexSearcher
{
public:
    IndexSearcher(const AithetaIndexConfig& indexConfig, const std::string& searcherName,
                  const AiThetaContextHolderPtr& contextHolder);
    virtual ~IndexSearcher();

public:
    virtual bool Init() = 0;
    virtual bool Search(const AithetaQuery& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                        ResultHolder& holder) = 0;

public:
    void SetAithetaFilterCreator(const std::shared_ptr<AithetaFilterCreatorBase>& creator) { _filterCreator = creator; }
    void SetSegmentBaseDocId(docid_t segBaseDocId) { _segmentBaseDocId = segBaseDocId; }

protected:
    virtual bool ParseQueryParameter(const std::string& searchParams, AiThetaParams& aiThetaParams) const = 0;

protected:
    bool InitMeasure(const std::string& distanceType);
    template <typename T>
    bool SearchImpl(const T& searcher, const AithetaQuery& query,
                    const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo, ResultHolder& resultHolder) const;
    bool UpdateContext(const AithetaQuery& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                       bool isNewContext, AiThetaContext* context) const;
    void ClearContext(AiThetaContext* context) const;
    template <typename T>
    bool DoSearch(const T& searcher, const AithetaQuery& query, AiThetaContext* context) const;
    void MergeResult(const AiThetaContext* context, const AithetaQuery& query, ResultHolder& resultHolder) const;

protected:
    AithetaIndexConfig _indexConfig;
    std::string _searcherName;
    docid_t _segmentBaseDocId;
    std::shared_ptr<AithetaFilterCreatorBase> _filterCreator;
    IndexQueryMeta _queryMeta;
    AiThetaContextHolderPtr _contextHolder;
    AiThetaMeasurePtr _measure;
    AUTIL_LOG_DECLARE();
};

template <typename T>
bool IndexSearcher::SearchImpl(const T& searcher, const AithetaQuery& query,
                               const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                               ResultHolder& resultHolder) const
{
    bool isNew = false;
    AiThetaContext* context {nullptr};
    std::tie(isNew, context) = _contextHolder->CreateIfNotExist(searcher, _searcherName, query.searchparams());
    ANN_CHECK(context, "create context failed");

    ANN_CHECK(UpdateContext(query, searchInfo, isNew, context), "init context failed");
    autil::ScopeGuard guard([context, this]() { ClearContext(context); });

    ANN_CHECK(DoSearch(searcher, query, context), "index search failed");
    MergeResult(context, query, resultHolder);
    return true;
}

template <typename T>
bool IndexSearcher::DoSearch(const T& searcher, const AithetaQuery& query, AiThetaContext* context) const
{
    auto ctx = std::unique_ptr<AiThetaContext>(context);
    autil::ScopeGuard guard([&ctx]() { ctx.release(); });

    const void* data = query.embeddings().data();
    size_t queryCount = query.embeddingcount();
    if (!query.lrsearch()) {
        ANN_CHECK_OK(searcher->search_impl(data, _queryMeta, queryCount, ctx), "search failed, query[%s]",
                     query.DebugString().c_str());
    } else {
        ANN_CHECK_OK(searcher->search_bf_impl(data, _queryMeta, queryCount, ctx), "bf search failed, query[%s]",
                     query.DebugString().c_str());
    }

    assert(_measure != nullptr);
    if (!_measure->support_normalize()) {
        return true;
    }
    for (size_t i = 0; i < queryCount; ++i) {
        auto result = context->mutable_result(i);
        for (auto& it : *result) {
            _measure->normalize(it.mutable_score());
        }
    }
    return true;
}

} // namespace indexlibv2::index::ann
