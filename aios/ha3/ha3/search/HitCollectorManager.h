#ifndef ISEARCH_HITCOLLECTORMANAGER_H
#define ISEARCH_HITCOLLECTORMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/RankResource.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/rank/HitCollector.h>
#include <suez/turing/expression/framework/AttributeExpressionCreatorBase.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <ha3/common/MultiErrorResult.h>
#include <ha3/rank/ComparatorCreator.h>

BEGIN_HA3_NAMESPACE(search);

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
    HA3_LOG_DECLARE();
private:
    friend class HitCollectorManagerTest;
};

HA3_TYPEDEF_PTR(HitCollectorManager);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_HITCOLLECTORMANAGER_H
