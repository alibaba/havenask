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
#include "ha3/cava/scorer/CompoundScorer.h"

#include "autil/StringUtil.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

#include "ha3/cava/scorer/MLRScorer.h"
#include "ha3/cava/scorer/PSScorer.h"
#include "ha3/cava/scorer/WeightScorer.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/SortDescription.h"
#include "ha3/common/RankSortClause.h"
#include "ha3/common/RankSortDescription.h"

using namespace isearch::common;
using namespace isearch::rank;
using namespace isearch::compound_scorer;
using namespace suez::turing;

namespace plugins {

AUTIL_LOG_SETUP(ha3, CompoundScorer);

CompoundScorer *CompoundScorer::create(CavaCtx *ctx) {
    return SuezCavaAllocUtil::createNativeObject<CompoundScorer>(ctx);
}

bool CompoundScorer::init(CavaCtx *ctx, ha3::ScorerProvider *provider) {
    return init(ctx, provider, NULL);
}

bool CompoundScorer::init(CavaCtx *ctx, ha3::ScorerProvider *provider,
                          cava::lang::CString *sortFields)
{
    auto scoringProvider = provider->getScoringProviderImpl();

    bool isASC = false;
    SortClause* sortClause = scoringProvider->getRequest()->getSortClause();
    RankSortClause* rankSortClause = scoringProvider->getRequest()->getRankSortClause();
    if (rankSortClause && rankSortClause->getRankSortDescCount() > 0){
        const RankSortDescription* rankSortDesc =
            rankSortClause->getRankSortDesc(0);
        if (rankSortDesc){
            const std::vector<isearch::common::SortDescription*>& descVec =
                rankSortDesc->getSortDescriptions();
            for(size_t i=0; i<descVec.size(); ++i){
                if(descVec[i] && descVec[i]->isRankExpression()){
                    isASC = descVec[i]->getSortAscendFlag();
                    break;
                }
            }
        }
    } else if (sortClause) {
        const std::vector<isearch::common::SortDescription*>& descVec =
            sortClause->getSortDescriptions();
        for(size_t i=0; i<descVec.size(); ++i){
            if(descVec[i] && descVec[i]->isRankExpression()){
                isASC = descVec[i]->getSortAscendFlag();
                break;
            }
        }
    }

    PSScorer psScorer;
    std::string fields = sortFields ? sortFields->getStr() : "";
    MLRScorer mlrScorer(fields);
    WeightScorer weightScorer;

    psScorer.initWorker(_workers, scoringProvider, isASC);
    mlrScorer.initWorker(_workers, scoringProvider, isASC);
    weightScorer.initWorker(_workers, scoringProvider, isASC);
    return true;
}

score_t CompoundScorer::process(CavaCtx *ctx, ha3::MatchDoc *matchDoc)
{
    auto &doc = *(matchdoc::MatchDoc *)matchDoc;
    score_t lPS = (score_t)0;
    for(std::vector<WorkerType>::iterator iter = _workers.begin();
            iter != _workers.end() ; ++iter)
    {
        if (!(*iter)(lPS, doc)){
            break;
        }
    }
    return lPS;
}

}
