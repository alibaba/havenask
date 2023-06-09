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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/search/SortExpression.h"
#include "ha3/search/SortExpressionCreator.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class DistinctDescription;
class Request;
}  // namespace common
namespace rank {
class ComboComparator;
class ComparatorCreator;
struct DistinctParameter;
}  // namespace rank
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionCreatorBase;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class HitCollectorManager
{
public:
    HitCollectorManager(
            suez::turing::AttributeExpressionCreatorBase *attrExprCreator,
            SortExpressionCreator *sortExpressionCreator,
            autil::mem_pool::Pool *pool,
            rank::ComparatorCreator *comparatorCreator,
            const common::Ha3MatchDocAllocatorPtr &matchDocAllocatorPtr,
            const common::MultiErrorResultPtr &errorResult);
    ~HitCollectorManager();
private:
    HitCollectorManager(const HitCollectorManager &);
    HitCollectorManager& operator=(const HitCollectorManager &);
public:
    bool init(const common::Request *request,uint32_t topK);
    
    rank::HitCollectorBase* getRankHitCollector() const { 
        return _rankHitCollector;
    }

    rank::ComparatorCreator *getComparatorCreator() {
        return _creator;
    }

    SortExpressionVector getFirstExpressions() const {
        SortExpressionVector firstExpressions;
        const std::vector<SortExpressionVector> &rankSortExpressions =
            _sortExpressionCreator->getRankSortExpressions();
        assert(rankSortExpressions.size());
        for (size_t i = 0; i < rankSortExpressions.size(); ++i) {
            assert(rankSortExpressions[i][0]);
            firstExpressions.push_back(rankSortExpressions[i][0]);
        }
        return firstExpressions;
    }


private:
    bool createRankHitCollector(const common::Request *request, uint32_t topK);

    bool doCreateOneDimensionHitCollector(
            const std::vector<SortExpressionVector> &rankSortExpressions,
            const common::Request *request, uint32_t topK);

    bool doCreateMultiDimensionHitCollector(
            const std::vector<SortExpressionVector> &rankSortExpressions,
            const common::Request *request, uint32_t topK);

    rank::HitCollectorBase *createDistinctHitCollector(
            const common::DistinctDescription *distDescription,
            uint32_t topK, autil::mem_pool::Pool *pool,
            const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            suez::turing::AttributeExpressionCreatorBase* attrExprCreator,
            SortExpressionCreator* sortExprCreator,
            suez::turing::AttributeExpression *sortExpr, bool sortFlag,
            rank::ComboComparator *comp,
            const common::MultiErrorResultPtr &errorResultPtr);

    bool setupDistinctParameter(
            const common::DistinctDescription *distDescription,
            rank::DistinctParameter &dp);    

    void setRankHitCollector(rank::HitCollectorBase *rankHitCollector) {
        POOL_DELETE_CLASS(_rankHitCollector);
        _rankHitCollector = rankHitCollector;
    }
private:
    suez::turing::AttributeExpressionCreatorBase *_attrExprCreator;
    SortExpressionCreator *_sortExpressionCreator;
    autil::mem_pool::Pool *_pool;
    rank::ComparatorCreator *_creator;
    common::Ha3MatchDocAllocatorPtr _matchDocAllocatorPtr;
    common::MultiErrorResultPtr _errorResultPtr;
    rank::HitCollectorBase *_rankHitCollector;
private:
    AUTIL_LOG_DECLARE();
private:
    friend class HitCollectorManagerTest;
};

typedef std::shared_ptr<HitCollectorManager> HitCollectorManagerPtr;

} // namespace search
} // namespace isearch

