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

#include <stddef.h>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/search/SortExpression.h"

namespace isearch {
namespace common {
class Ha3MatchDocAllocator;
class RankSortClause;
class Request;
class SortClause;
class SortDescription;
}  // namespace common
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionCreator;
class RankAttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class SortExpressionCreator
{
public:
    SortExpressionCreator(suez::turing::AttributeExpressionCreator *attributeExpressionCreator,
                          const rank::RankProfile *rankProfile,
                          common::Ha3MatchDocAllocator *matchDocAllocator,
                          const common::MultiErrorResultPtr &errorResultPtr,
                          autil::mem_pool::Pool *pool);
    ~SortExpressionCreator();
private:
    SortExpressionCreator(const SortExpressionCreator &);
    SortExpressionCreator& operator=(const SortExpressionCreator &);
public:
    bool init(const common::Request* request);
    suez::turing::RankAttributeExpression *createRankExpression();
public:
    suez::turing::RankAttributeExpression *getRankExpression() const {
        return _rankExpression;
    }
    const std::vector<SortExpressionVector> &getRankSortExpressions() const {
        return _rankSortExpressions;
    }
    SortExpression* createSortExpression(
            suez::turing::AttributeExpression* attributeExpression, bool sortFlag)
    {
        if (!attributeExpression) {
            return NULL;
        }
        SortExpression *sortExpr = POOL_NEW_CLASS(_pool,
                SortExpression, attributeExpression);
        sortExpr->setSortFlag(sortFlag);
        _needDelExpressions.push_back(sortExpr);
        return sortExpr;
    }
private:
    bool initSortExpressions(const common::SortClause *sortClause);
    bool initRankSortExpressions(const common::RankSortClause *rankSortClause);
    bool doCreateSortExpressions(
            const std::vector<common::SortDescription *> &sortDescriptions,
            SortExpressionVector &sortExpressions);
    bool createSingleSortExpression(
            const common::SortDescription *sortDescription,
            SortExpressionVector &sortExpressions);
public:
    // for test
    void setRankSortExpressions(const std::vector<SortExpressionVector>& v) {
        _rankSortExpressions = v;
    }
private:
    suez::turing::AttributeExpressionCreator *_attributeExprCreator;
    const rank::RankProfile *_rankProfile;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    common::MultiErrorResultPtr _errorResultPtr;

    suez::turing::RankAttributeExpression *_rankExpression;
    std::vector<SortExpressionVector> _rankSortExpressions;
    SortExpressionVector _needDelExpressions;
    autil::mem_pool::Pool *_pool;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SortExpressionCreator> SortExpressionCreatorPtr;

} // namespace search
} // namespace isearch

