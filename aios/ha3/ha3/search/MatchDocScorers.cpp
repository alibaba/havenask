#include <ha3/search/MatchDocScorers.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/cava/ScorerProvider.h>
#include <suez/turing/expression/plugin/ScorerWrapper.h>

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
using namespace autil::mem_pool;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchDocScorers);

MatchDocScorers::MatchDocScorers(uint32_t reRankSize,
                                 common::Ha3MatchDocAllocator *allocator,
                                 Pool *pool)
    : _scoreRef(NULL)
    , _scoreAttrExpr(NULL)
    , _allocator(allocator)
    , _reRankSize(reRankSize)
    , _hasAssginedScoreValue(false)
    , _pool(pool)
{
}

MatchDocScorers::~MatchDocScorers() { 
    for (size_t i = 0; i < _scorerWrappers.size(); ++i) {
        POOL_DELETE_CLASS(_scorerWrappers[i]);
    }
    for (size_t i = 0; i < _cavaScorerProviders.size(); ++i) {
        POOL_DELETE_CLASS(_cavaScorerProviders[i]);
    }
    for (size_t i = 0; i < _scoringProviders.size(); ++i) {
        POOL_DELETE_CLASS(_scoringProviders[i]);
    }
    POOL_DELETE_CLASS(_scoreAttrExpr);
}

bool MatchDocScorers::init(const std::vector<Scorer*> &scorerVec,
                           const common::Request* request,
                           const config::IndexInfoHelper *indexInfoHelper,
                           SearchCommonResource &resource,
                           SearchPartitionResource &partitionResource,
                           SearchProcessorResource &processorResource)
{
    _scoreRef = _allocator->declareWithConstructFlagDefaultGroup<score_t>(
            SCORE_REF, false, SL_CACHE);
    if (_scoreRef == NULL) {
        return false;
    }
    for (auto scorer : scorerVec) {
        ScorerWrapper* scorerWrapper = POOL_NEW_CLASS(_pool,
                ScorerWrapper, scorer, _scoreRef);
        _scorerWrappers.push_back(scorerWrapper);
        auto scoringProvider = MatchDocSearcher::createScoringProvider(request,
                indexInfoHelper, resource,
                partitionResource, processorResource);
        _scoringProviders.push_back(scoringProvider);
        _cavaScorerProviders.push_back(POOL_NEW_CLASS(resource.pool,
                        ha3::ScorerProvider, scoringProvider, resource.cavaJitModules));
    }
    return true;
}

bool MatchDocScorers::beginRequest() {
    for (size_t i = 0; i < _scorerWrappers.size(); ++i) {        
        bool ret = _scorerWrappers[i]->beginRequest(_scoringProviders[i]);
        if (!ret) {
            return false;
        }
    }
    return true;
}

void MatchDocScorers::endRequest() {
    for (size_t i = 0; i < _scorerWrappers.size(); ++i) {        
        _scorerWrappers[i]->endRequest();
    }
}

void MatchDocScorers::setTotalMatchDocs(uint32_t v) {
    for (auto provider : _scoringProviders) {
        provider->setTotalMatchDocs(v);
    }
}

void MatchDocScorers::setAggregateResultsPtr(const common::AggregateResultsPtr &aggResultsPtr)
{
    for (auto provider : _scoringProviders) {
        provider->setAggregateResultsPtr(aggResultsPtr);
    }
}

uint32_t MatchDocScorers::scoreMatchDocs(PoolVector<matchdoc::MatchDoc> &matchDocVect)
{
    uint32_t totalDeletedCount = 0;
    for (size_t phase = 0; phase < _scorerWrappers.size(); ++phase) {
        uint32_t deletedCount = doScore(phase, matchDocVect);
        totalDeletedCount += deletedCount;
        _hasAssginedScoreValue = true;
        if (matchDocVect.empty()) {
            break;
        }
    }
    return totalDeletedCount;
}

uint32_t MatchDocScorers::doScore(uint32_t phase, 
                                  PoolVector<matchdoc::MatchDoc> &matchDocVect)
{
    uint32_t matchDocSize = matchDocVect.size();       
    size_t scoreDocCount = std::min(matchDocSize, _reRankSize);
    _scorerWrappers[phase]->batchScore(matchDocVect.data(), scoreDocCount);
    if (!_hasAssginedScoreValue) {
        for (size_t i = scoreDocCount; i < matchDocSize; i++) {
            matchdoc::MatchDoc &matchDoc = matchDocVect[i];
            _scoreRef->set(matchDoc, (score_t)0);
        }
    }
    _scorerWrappers[phase]->setAttributeEvaluated();
    // delete matchdoc
    uint32_t newCursor = 0;
    for (uint32_t i = 0; i < matchDocSize; ++i) {
        auto matchDoc = matchDocVect[i];
        if (unlikely(matchDoc.isDeleted())) {
            _allocator->deallocate(matchDoc);
            continue;
        }
        if (newCursor != i) {
            matchDocVect[newCursor] = matchDoc;
        }
        ++newCursor;
    }
    
    matchDocVect.resize(newCursor);
    return (matchDocSize - newCursor);
}

AttributeExpression *MatchDocScorers::getScoreAttributeExpression() {
    assert(_scoreRef);
    if (_scoreAttrExpr) {
        return _scoreAttrExpr;
    }
    _scoreAttrExpr= POOL_NEW_CLASS(_pool, AttributeExpressionTyped<score_t>);
    _scoreAttrExpr->setReference(_scoreRef);
    _scoreAttrExpr->setEvaluated();
    return _scoreAttrExpr;
}

END_HA3_NAMESPACE(search);

