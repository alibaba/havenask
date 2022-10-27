#include <ha3/search/HitCollectorManager.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/rank/MultiHitCollector.h>
#include <ha3/rank/NthElementCollector.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, HitCollectorManager);

HitCollectorManager::HitCollectorManager(
        AttributeExpressionCreatorBase *attrExprCreator,
        SortExpressionCreator *sortExpressionCreator,
        autil::mem_pool::Pool *pool,
        rank::ComparatorCreator *comparatorCreator,
        const common::Ha3MatchDocAllocatorPtr &matchDocAllocatorPtr,
        const MultiErrorResultPtr &errorResult) 
    : _attrExprCreator(attrExprCreator)
    , _sortExpressionCreator(sortExpressionCreator)
    , _pool(pool)
    , _creator(comparatorCreator)
    , _matchDocAllocatorPtr(matchDocAllocatorPtr)
    , _errorResultPtr(errorResult)
    , _rankHitCollector(NULL)
{ 
}

HitCollectorManager::~HitCollectorManager() {
    POOL_DELETE_CLASS(_rankHitCollector);
}

bool HitCollectorManager::init(const Request *request, uint32_t topK) {
    ConfigClause *configClause = request->getConfigClause();
    if (!createRankHitCollector(request, topK)) {
        HA3_LOG(WARN, "Create Rank HitCollector failed!");
        return false;
    }
    if (!configClause->isBatchScore()) {
        _rankHitCollector->disableBatchScore();
    }
    return true;
}

bool HitCollectorManager::createRankHitCollector(const Request *request, 
        uint32_t topK)
{
    const vector<SortExpressionVector> &rankSortExpressions =
        _sortExpressionCreator->getRankSortExpressions();
    if (rankSortExpressions.size() == 1) {
        return doCreateOneDimensionHitCollector(rankSortExpressions,
                request, topK);
    } else {
        return doCreateMultiDimensionHitCollector(rankSortExpressions,
                request, topK);
    }
    return true;
}

bool HitCollectorManager::doCreateOneDimensionHitCollector(
        const vector<SortExpressionVector> &rankSortExpressions,
        const Request *request, uint32_t topK)
{
    const SortExpressionVector sortExprs = rankSortExpressions[0];
    assert(sortExprs.size());
    ComboComparator *rankComp = _creator->createSortComparator(sortExprs);
    const vector<AttributeExpression*> &immEvaluateExprs =
        _creator->getImmEvaluateExpressions();
    assert(immEvaluateExprs.size());

    AttributeExpression *attrForHitCollector = NULL;
    if (immEvaluateExprs.size() == 1) {
        attrForHitCollector = *(immEvaluateExprs.begin());
    } else {
        attrForHitCollector = _attrExprCreator->createComboAttributeExpression(
                immEvaluateExprs);
    }

    HitCollectorBase *hitCollector = NULL;
    DistinctClause *distinctClause = request->getDistinctClause();
    DistinctDescription *distDescription = NULL;
    if (distinctClause) {
        distDescription = distinctClause->getDistinctDescription(0);
    }
    if (distDescription) {
        hitCollector = createDistinctHitCollector(distDescription,
                topK, _pool, _matchDocAllocatorPtr,
                _attrExprCreator, _sortExpressionCreator,
                attrForHitCollector, sortExprs[0]->getSortFlag(),
                rankComp, _errorResultPtr);
    } else {
        hitCollector = POOL_NEW_CLASS(_pool, NthElementCollector, topK, _pool,
                _matchDocAllocatorPtr, attrForHitCollector, rankComp);
    }
    if (!hitCollector) {
        POOL_DELETE_CLASS(rankComp);
        return false;
    }
    setRankHitCollector(hitCollector);
    return true;
}

bool HitCollectorManager::doCreateMultiDimensionHitCollector(
        const vector<SortExpressionVector> &rankSortExpressions,
        const Request *request, uint32_t topK)
{
    vector<ComboComparator*> rankComps =
        _creator->createRankSortComparators(rankSortExpressions);
    assert(rankComps.size() == rankSortExpressions.size());
        
    const vector<AttributeExpression*> &immEvaluateExprs =
        _creator->getImmEvaluateExpressions();
    assert(immEvaluateExprs.size());

    AttributeExpression *attrForHitCollector = NULL;
    if (immEvaluateExprs.size() == 1) {
        attrForHitCollector = *(immEvaluateExprs.begin());
    } else {
        attrForHitCollector = _attrExprCreator->createComboAttributeExpression(
                immEvaluateExprs);
    }
    MultiHitCollector *multiHitCollector = POOL_NEW_CLASS(_pool,
            MultiHitCollector, _pool, _matchDocAllocatorPtr,
            attrForHitCollector);
    
    uint32_t leftSize = topK;
    RankSortClause *rankSortClause = request->getRankSortClause();
    for (size_t i = 0; i < rankSortClause->getRankSortDescCount(); ++i) {
        const RankSortDescription *rankSortDesc =
            rankSortClause->getRankSortDesc(i);
        uint32_t size = uint32_t(rankSortDesc->getPercent() * topK / 100);
        size = min(size, leftSize);
        if (i == rankSortClause->getRankSortDescCount() - 1) {
            size = leftSize; // add all left size to last hit collector
        } else {
            leftSize -= size;
        }
        if (size == 0) {
            POOL_DELETE_CLASS(rankComps[i]);
            continue;
        }
        HitCollectorBase *hitCollector = POOL_NEW_CLASS(_pool, HitCollector,
                size, _pool, _matchDocAllocatorPtr, NULL, rankComps[i]);
        multiHitCollector->addHitCollector(hitCollector);
    }
    setRankHitCollector(multiHitCollector);
    return true;
}

HitCollectorBase* HitCollectorManager::createDistinctHitCollector(
        const DistinctDescription *distDescription,
        uint32_t topK, autil::mem_pool::Pool *pool,
        const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
        AttributeExpressionCreatorBase* attrExprCreator,
        SortExpressionCreator* sortExprCreator,
        AttributeExpression *attrExpr, bool sortFlag,
        ComboComparator *comp, const MultiErrorResultPtr &errorResultPtr)
{
    DistinctParameter dp;
    AttributeExpression *keyAttribute =
        attrExprCreator->createAttributeExpression(
                distDescription->getRootSyntaxExpr());
    if (!keyAttribute || !keyAttribute->allocate(allocatorPtr.get())) {
        if (errorResultPtr) {
            errorResultPtr->addError(ERROR_SEARCH_SETUP_DISTINCT,
                    "Create distinct key express error.");
        }
        HA3_LOG(WARN, "Create distinct key express error");
        return NULL;
    }

    dp.keyExpression = sortExprCreator->createSortExpression(
            keyAttribute, false);
    dp.distinctTimes = distDescription->getDistinctTimes();
    dp.distinctCount = distDescription->getDistinctCount();
    dp.maxItemCount = distDescription->getMaxItemCount();
    dp.reservedFlag = distDescription->getReservedFlag();
    dp.updateTotalHitFlag = distDescription->getUpdateTotalHitFlag();
    dp.gradeThresholds = distDescription->getGradeThresholds();
    dp.distinctInfoRef = allocatorPtr->declare<DistinctInfo>(DISTINCT_INFO_REF);
    dp.distinctInfoRef->setSerializeLevel(SL_NONE);
    const FilterClause *filterClause = distDescription->getFilterClause();
    dp.distinctFilter = Filter::createFilter(filterClause, attrExprCreator, pool);
    if (filterClause && !dp.distinctFilter) {
        HA3_LOG(WARN, "create distinct filter expression failed.");
        if (errorResultPtr) {
            errorResultPtr->addError(ERROR_SEARCH_SETUP_DISTINCT,
                    "create distinct filter expression failed.");
        }
        return NULL;
    }
    if (dp.distinctFilter) {
        AttributeExpression *filterExpr = 
            dp.distinctFilter->getAttributeExpression();
        if (!filterExpr->allocate(allocatorPtr.get())) {
            POOL_DELETE_CLASS(dp.distinctFilter);
            dp.distinctFilter = NULL;
            return NULL;
        }
    }

    return POOL_NEW_CLASS(pool, DistinctHitCollector, topK, pool,
                          allocatorPtr, attrExpr, comp, dp, sortFlag);
}

END_HA3_NAMESPACE(search);

