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
#include "indexlib/index/ann/aitheta2/impl/IndexSearcher.h"

using namespace std;
namespace indexlibv2::index::ann {

IndexSearcher::IndexSearcher(const AithetaIndexConfig& config, const std::string& name,
                             const AiThetaContextHolderPtr& holder)
    : _indexConfig(config)
    , _searcherName(name)
    , _segmentBaseDocId(0)
    , _contextHolder(holder)
{
    ParamsInitializer::InitAiThetaMeta(_indexConfig, _aithetaMeta);
    _queryMeta.set_meta(_aithetaMeta.type(), _aithetaMeta.dimension());
}

IndexSearcher::~IndexSearcher() {}

bool IndexSearcher::InitMeasure(const std::string& distanceType)
{
    _measure = AiThetaFactory::CreateMeasure(distanceType);
    ANN_CHECK(_measure != nullptr, "create measure failed");
    ANN_CHECK_OK(_measure->init(_aithetaMeta, AiThetaParams()), "init measure[%s] failed", distanceType.c_str());
    auto query_measure = _measure->query_measure();
    if (query_measure != nullptr) {
        _measure = query_measure;
    }

    return true;
}

bool IndexSearcher::UpdateContext(const AithetaQuery& query,
                                  const std::shared_ptr<AithetaAuxSearchInfoBase>& auxiliarySearchInfo,
                                  bool isNewContext, AiThetaContext* context) const
{
    if (isNewContext && !query.searchparams().empty() && query.searchparams() != "{}") {
        AiThetaParams params;
        ANN_CHECK(ParseQueryParameter(query.searchparams(), params), "parse query parameters failed");
        ANN_CHECK_OK(context->update(params), "update ctx failed with[%s]", query.searchparams().c_str());
    }

    context->set_topk(query.topk());
    context->reset_filter();

    std::shared_ptr<AithetaFilterBase> filter = nullptr;
    if (nullptr != auxiliarySearchInfo) {
        auto searchInfo = std::dynamic_pointer_cast<AithetaAuxSearchInfo>(auxiliarySearchInfo);
        ANN_CHECK(searchInfo, "dynamic cast to AithetaAuxSearchInfo failed");
        filter = searchInfo->GetFilter();
    }

    AithetaFilterCreatorBase::AithetaFilterFunc func;
    if (nullptr != _filterCreator && _filterCreator->Create(_segmentBaseDocId, filter, func)) {
        context->set_filter(func);
    }

    return true;
}

void IndexSearcher::ClearContext(AiThetaContext* context) const { context->reset_filter(); }

void IndexSearcher::MergeResult(const AiThetaContext* context, const AithetaQuery& query,
                                ResultHolder& resultHolder) const
{
    bool hasThreshold = query.hasscorethreshold();
    float scoreThreshold = query.scorethreshold();
    size_t count = query.embeddingcount();
    for (size_t i = 0; i < count; ++i) {
        for (const auto& res : context->result(i)) {
            if (hasThreshold) {
                resultHolder.AppendResult(res.key(), res.score(), scoreThreshold);
            } else {
                resultHolder.AppendResult(res.key(), res.score());
            }
        }
    }

    ResultHolder::SearchStats stats = {context->stats().filtered_count(), context->stats().dist_calced_count()};
    resultHolder.MergeSearchStats(stats);
}

AUTIL_LOG_SETUP(indexlib.index, IndexSearcher);
} // namespace indexlibv2::index::ann
