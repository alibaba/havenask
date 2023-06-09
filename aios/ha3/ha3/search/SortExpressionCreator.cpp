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
#include "ha3/search/SortExpressionCreator.h"

#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/RankSortClause.h"
#include "ha3/common/RankSortDescription.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/SortDescription.h"
#include "ha3/rank/RankProfile.h"
#include "ha3/search/SortExpression.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/RankAttributeExpression.h"

using namespace std;

using namespace isearch::common;
using namespace isearch::rank;
using namespace suez::turing;

namespace isearch {
namespace search {

AUTIL_LOG_SETUP(ha3, SortExpressionCreator);

SortExpressionCreator::SortExpressionCreator(
        AttributeExpressionCreator *attributeExprCreator,
        const RankProfile *rankProfile,
        common::Ha3MatchDocAllocator *matchDocAllocator,
        const MultiErrorResultPtr &errorResultPtr,
        autil::mem_pool::Pool *pool)
    : _attributeExprCreator(attributeExprCreator)
    , _rankProfile(rankProfile)
    , _matchDocAllocator(matchDocAllocator)
    , _errorResultPtr(errorResultPtr)
    , _rankExpression(NULL)
    , _pool(pool)
{ 
}

SortExpressionCreator::~SortExpressionCreator() {
    for (size_t i = 0; i < _needDelExpressions.size(); ++i) {
        POOL_DELETE_CLASS(_needDelExpressions[i]);
    }
    _needDelExpressions.clear();
}

bool SortExpressionCreator::init(const Request* request) {
    const SortClause *sortClause = request->getSortClause();
    const RankSortClause *rankSortClause = request->getRankSortClause();
    if (!sortClause && !rankSortClause) 
    {
        _rankSortExpressions.resize(1);
        AttributeExpression *rankExpr = createRankExpression();
        if (!rankExpr) {
            return false;
        }
        SortExpression *sortExpr = POOL_NEW_CLASS(_pool, 
                SortExpression, rankExpr);
        _rankSortExpressions[0].push_back(sortExpr);
        _needDelExpressions.push_back(sortExpr);
    } else if (rankSortClause) {
        if (!initRankSortExpressions(rankSortClause)) {
            return false;
        }
    } else if (sortClause) {
        if (!initSortExpressions(sortClause)) {
            return false;
        }
    }
    return true;    
}

bool SortExpressionCreator::initRankSortExpressions(
        const RankSortClause *rankSortClause)
{
    assert(rankSortClause);
    uint32_t rankSortDescCount = rankSortClause->getRankSortDescCount();
    _rankSortExpressions.resize(rankSortDescCount);
    for (size_t i = 0; i < rankSortDescCount; ++i) {
        const vector<SortDescription*> &sortDescs = 
            rankSortClause->getRankSortDesc(i)->getSortDescriptions();
        if (!doCreateSortExpressions(sortDescs, _rankSortExpressions[i])) {
            return false;
        }
    }
    return true;
}
    
bool SortExpressionCreator::initSortExpressions(const SortClause* sortClause) {
    assert(sortClause);
    _rankSortExpressions.resize(1);
    if (!doCreateSortExpressions(sortClause->getSortDescriptions(), 
                                 _rankSortExpressions[0])) 
    {
        return false;
    }
    return true;
}

bool SortExpressionCreator::doCreateSortExpressions(
        const vector<SortDescription *> &sortDescriptions,
        SortExpressionVector &sortExpressions)
{
    for (size_t i = 0; i < sortDescriptions.size(); ++i) {
        if (!createSingleSortExpression(sortDescriptions[i], sortExpressions)) {
            return false;
        }
    }
    return true;
}

bool SortExpressionCreator::createSingleSortExpression(
        const common::SortDescription *sortDescription,
        SortExpressionVector &sortExpressions) 
{
    AttributeExpression *expr = NULL;
    if (sortDescription->isRankExpression()) { //RANK
        if (!_rankExpression) { 
            expr = createRankExpression();
            if (!expr) {
                return false;
            }
        } else {
            expr = _rankExpression;
        }
    } else { //NORMAL
        expr = _attributeExprCreator->createAttributeExpression(
                sortDescription->getRootSyntaxExpr());
        if (!expr || !expr->allocate(_matchDocAllocator)) {
            string errorStr = "allocate sort expression FAILED!";
            AUTIL_LOG(WARN, "%s", errorStr.c_str());
            _errorResultPtr->addError(ERROR_SEARCH_CREATE_SORT_EXPRESSION,
                    errorStr);
            return false;
        }
    }
    
    assert(expr);
    SortExpression *sortExpr = createSortExpression(expr,
            sortDescription->getSortAscendFlag());
    sortExpressions.push_back(sortExpr);        
    return true;
}

RankAttributeExpression* SortExpressionCreator::createRankExpression() {
    if (!_rankProfile) {
        return NULL;
    }

    if (_rankExpression) {
        return _rankExpression;
    }

    _rankExpression = _rankProfile->createRankExpression(_pool);

    if (!_rankExpression
        || !_rankExpression->allocate(_matchDocAllocator))
    {
        string errorStr = "create rank attribute expression FAILED!";
        AUTIL_LOG(WARN, "%s", errorStr.c_str());
        _errorResultPtr->addError(ERROR_SEARCH_CREATE_SORT_EXPRESSION,
                                  errorStr);
        return NULL;
    }
    _attributeExprCreator->addNeedDeleteExpr(_rankExpression);
    return _rankExpression;
}

} // namespace search
} // namespace isearch
