#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/RankResource.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/search/OptimizerChainManager.h>
#include <ha3/search/MatchDocSearchStrategy.h>
#include <ha3/search/LayerMetasCreator.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/cava/ScorerProvider.h>
#include <ha3/rank/HitCollectorBase.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <ha3/search/LayerMetaUtil.h>
#include <suez/turing/expression/common.h>

using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchDocSearcher);

MatchDocSearcher::MatchDocSearcher(
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchRuntimeResource &runtimeResource,
            const rank::RankProfileManager *rankProfileMgr,
            const OptimizerChainManager *optimizerChainManager,
            const SorterManager *sorterManager,
            const config::AggSamplerConfigInfo& aggSamplerConfigInfo,
            const config::ClusterConfigInfo &clusterConfigInfo,
            uint32_t partCount,
            SearcherCache *searcherCache,
            const rank::RankProfile *rankProfile,
            const LayerMetasPtr &layerMetas)
        : _resource(resource)
        , _partitionResource(partitionResource)

        , _indexInfoHelper(POOL_NEW_CLASS(_resource.pool, config::LegacyIndexInfoHelper,
                        _resource.tableInfo->getIndexInfos()))
        , _rankProfileMgr(rankProfileMgr)
        , _optimizerChainManager(optimizerChainManager)
        , _sorterManager(sorterManager)
        , _searcherCacheManager(searcherCache,
                                _partitionResource.indexPartitionReaderWrapper,
                                _resource.pool,
                                _resource.tableInfo->hasSubSchema(),
                                resource,
                                partitionResource)
        , _processorResource(runtimeResource)
        , _seekAndRankProcessor(_resource, _partitionResource, _processorResource, aggSamplerConfigInfo)
        , _rerankProcessor(_resource, _partitionResource, _processorResource)
        , _extraRankProcessor(_resource, _partitionResource, _processorResource)
        , _clusterConfigInfo(clusterConfigInfo)
        , _partCount(partCount)
        , _layerMetas(layerMetas)
{
    _processorResource.rankProfile = rankProfile;
}

MatchDocSearcher::~MatchDocSearcher() {
    POOL_DELETE_CLASS(_processorResource.scoringProvider);
    POOL_DELETE_CLASS(_processorResource.cavaScorerProvider);
    POOL_DELETE_CLASS(_indexInfoHelper);
}

common::ResultPtr MatchDocSearcher::search(const common::Request *request) {
    assert(request->getConfigClause()->getPhaseNumber() == SEARCH_PHASE_ONE);

    if (!init(request)) {
        return constructErrorResult();
    }

    InnerSearchResult innerResult(_resource.pool);
    std::shared_ptr<MatchDocSearchStrategy> searchStrategy(getSearchStrategy(request));
    SeekAndRankResult result1 = _seekAndRankProcessor.process(request, searchStrategy.get(), innerResult);
    _rerankProcessor.process(request, result1.ranked, result1.rankComp, innerResult);
    searchStrategy->afterRerank(innerResult);
    _extraRankProcessor.process(request, innerResult, searchStrategy->getExtraRankCount());
    searchStrategy->afterExtraRank(innerResult);

    MatchDocSearchStrategy::releaseRedundantMatchDocs(innerResult.matchDocVec,
            searchStrategy->getResultCount(),
            _resource.matchDocAllocator);
    doLazyEvaluate(innerResult.matchDocVec);
    common::Result *result = constructResult(innerResult);
    result = searchStrategy->reconstructResult(result);
    return common::ResultPtr(result);
}

bool MatchDocSearcher::seek(const common::Request *request,
                            SearcherCacheInfo *searchCacheInfo,
                            InnerSearchResult &innerResult)
{
    assert(request->getConfigClause()->getPhaseNumber() == SEARCH_PHASE_ONE);
    if (!init(request)) {
        return false;
    }
    auto &docCountLimits = _processorResource.docCountLimits;
    std::shared_ptr<MatchDocSearchStrategy> searchStrategy(new MatchDocSearchStrategy(docCountLimits));
    SeekAndRankResult seekResult = _seekAndRankProcessor.processWithCache(
            request, searchCacheInfo, innerResult);
    _rerankProcessor.process(request, seekResult.ranked, seekResult.rankComp, innerResult);
    if (searchCacheInfo) {
        searchCacheInfo->fillAfterRerank(innerResult.actualMatchDocs);
    }
    innerResult.matchDocAllocatorPtr = _resource.matchDocAllocator;
    innerResult.extraRankCount = searchStrategy->getExtraRankCount();
    // rewrite extra rank if cache miss
    if (searchCacheInfo && !searchCacheInfo->isHit) {
        auto cacheManager = searchCacheInfo->cacheManager;
        if (cacheManager) {
            auto cacheStrategy = cacheManager->getCacheStrategy();
            auto &extraRankCount = innerResult.extraRankCount;
            extraRankCount = std::max(extraRankCount,
                    cacheStrategy->getCacheDocNum(request,
                            docCountLimits.requiredTopK));
            extraRankCount = std::min(extraRankCount, (uint32_t)innerResult.matchDocVec.size());
        }
    }
    return true;
}

bool MatchDocSearcher::initAuxJoin(HashJoinInfo *hashJoinInfo) {
    const std::string &joinAttributeName = hashJoinInfo->getJoinFieldName();
    AttributeExpression *expr =
            _partitionResource.attributeExpressionCreator->createAtomicExpr(
                    joinAttributeName);
    if (!expr || expr->isMultiValue() ||
        !expr->allocate(_resource.matchDocAllocator.get())) {
        HA3_LOG(WARN, "Create aux join attribute expression failed! "
                      "attributeName[%s]",
                hashJoinInfo->getJoinFieldName().c_str());
        _resource.errorResult->addError(ERROR_CREATE_ATTRIBUTE_EXPRESSION,
                                        joinAttributeName);
        return false;
    }

    hashJoinInfo->setJoinAttrExpr(expr);

    std::string auxDocIdRefName =
            BUILD_IN_JOIN_DOCID_REF_PREIX + joinAttributeName;
    auto auxDocIdRef =
            _resource.matchDocAllocator->declare<DocIdWrapper>(auxDocIdRefName);
    if (!auxDocIdRef) {
        HA3_LOG(WARN, "Create aux join docid attr failed! ");
        _resource.errorResult->addError(
                ERROR_CREATE_ATTRIBUTE_EXPRESSION, auxDocIdRefName);
        return false;
    }

    hashJoinInfo->setAuxDocidRef(auxDocIdRef);

    return true;
}

bool MatchDocSearcher::seekAndJoin(const common::Request *request,
                                   SearcherCacheInfo *searchCacheInfo,
                                   HashJoinInfo *hashJoinInfo,
                                   InnerSearchResult &innerResult)
{
    assert(request->getConfigClause()->getPhaseNumber() == SEARCH_PHASE_ONE);
    if (!init(request)) {
        return false;
    }
    if (!initAuxJoin(hashJoinInfo)) {
	return false;
    }
    auto &docCountLimits = _processorResource.docCountLimits;
    std::shared_ptr<MatchDocSearchStrategy> searchStrategy(new MatchDocSearchStrategy(docCountLimits));
    SeekAndRankResult seekResult = _seekAndRankProcessor.processWithCacheAndJoinInfo(
                    request, searchCacheInfo, hashJoinInfo, innerResult);
    _rerankProcessor.process(request, seekResult.ranked, seekResult.rankComp, innerResult);
    if (searchCacheInfo) {
        searchCacheInfo->fillAfterRerank(innerResult.actualMatchDocs);
    }
    innerResult.matchDocAllocatorPtr = _resource.matchDocAllocator;
    innerResult.extraRankCount = searchStrategy->getExtraRankCount();
    // rewrite extra rank if cache miss
    if (searchCacheInfo && !searchCacheInfo->isHit) {
        auto cacheManager = searchCacheInfo->cacheManager;
        if (cacheManager) {
            auto cacheStrategy = cacheManager->getCacheStrategy();
            auto &extraRankCount = innerResult.extraRankCount;
            extraRankCount = std::max(extraRankCount,
                    cacheStrategy->getCacheDocNum(request,
                            docCountLimits.requiredTopK));
            extraRankCount = std::min(extraRankCount, (uint32_t)innerResult.matchDocVec.size());
        }
    }
    return true;
}

bool MatchDocSearcher::init(const common::Request *request) {
    if (!initProcessorResource(request) || !doInit(request)) {
        return false;
    }

    if (!_seekAndRankProcessor.init(request) || !_rerankProcessor.init(request)) {
        return false;
    }

    if (!_extraRankProcessor.init(request)) { // only for modify topk/require matchdata
        return false;
    }

    // attribute init after plugin, accept var declared in plugins
    if (!doCreateAttributeExpressionVec(request->getAttributeClause())) {
        return false;
    }

    // create join filter have to go after createAttributeExpression (to create docid convert)
    _seekAndRankProcessor.initJoinFilter(request);
    return true;
}

bool MatchDocSearcher::initProcessorResource(const common::Request *request) {
    if (!_processorResource.rankProfile &&
        !_rankProfileMgr->getRankProfile(request, _processorResource.rankProfile,
                    _resource.errorResult))
    {
        HA3_LOG(WARN, "Get RankProfile Failed");
        return false;
    }
    _processorResource.sorterManager = _sorterManager;
    _processorResource.scoringProvider = createScoringProvider(request,
            _indexInfoHelper, _resource, _partitionResource, _processorResource);
    _processorResource.cavaScorerProvider = POOL_NEW_CLASS(_resource.pool,
            ha3::ScorerProvider, _processorResource.scoringProvider, _resource.cavaJitModules);
    if (NULL == _layerMetas) {
        _layerMetas = LayerMetaUtil::createLayerMetas(
                request->getQueryLayerClause(),
                request->getRankClause(),
                _partitionResource.indexPartitionReaderWrapper.get(),
                request->getPool());
    }
    _processorResource.layerMetas = _layerMetas.get();
    return true;
}

bool MatchDocSearcher::doInit(const common::Request *request) {

    _partitionResource.indexPartitionReaderWrapper->setTopK(
            getMatchDocMaxCount(_processorResource.docCountLimits.runtimeTopK));
    _partitionResource.indexPartitionReaderWrapper->setSessionPool(
            _resource.pool);

    if (!doOptimize(request,
                    _partitionResource.indexPartitionReaderWrapper.get())) {
        return false;
    }

    if (!initExtraDocIdentifier(request->getConfigClause())) {
        return false;
    }
    return true;
}

uint32_t MatchDocSearcher::getMatchDocMaxCount(int runtimeTopK) const {
    bool isNthCollector = true;//
    uint32_t matchDocMaxCount = runtimeTopK;

    if (isNthCollector) {
        //NthCollector uses two time of runtimeTopK to buffer matchdocs
        matchDocMaxCount *= 2;
    }
    return matchDocMaxCount + rank::HitCollectorBase::BATCH_EVALUATE_SCORE_SIZE + 1;
}

MatchDocSearchStrategy *MatchDocSearcher::getSearchStrategy(const common::Request *request) const {
    if (_searcherCacheManager.useCache(request)) {
        return _searcherCacheManager.getSearchStrategy(
                _sorterManager, request, _processorResource.docCountLimits);
    } else {
        return new MatchDocSearchStrategy(_processorResource.docCountLimits);
    }
}

bool MatchDocSearcher::doOptimize(const common::Request *request,
                IndexPartitionReaderWrapper *readerWrapper)
{
    if (NULL == request->getOptimizerClause()) {
        return true;
    }

    OptimizerChainPtr chain = _optimizerChainManager->createOptimizerChain(request);
    if (!chain) {
        HA3_LOG(WARN, "Create optimize chain failed.");
        _resource.errorResult->addError(ERROR_CREATE_OPTIMIZER_CHAIN);
        return false;
    }

    OptimizeParam param;
    param.request = request;
    param.readerWrapper = readerWrapper;
    param.sessionMetricsCollector = _resource.sessionMetricsCollector;

    if (!chain->optimize(&param)) {
        HA3_LOG(WARN, "Fail to do optimize.");
        _resource.errorResult->addError(ERROR_OPTIMIZER);
        return false;
    }
    REQUEST_TRACE_WITH_TRACER(_resource.tracer, TRACE3, "after optimize, request is \n %s",
                              request->toString().c_str());
    return true;
}

rank::ScoringProvider *MatchDocSearcher::createScoringProvider(
        const common::Request *request,
        const config::IndexInfoHelper *indexInfoHelper,
        SearchCommonResource &resource,
        SearchPartitionResource &partitionResource,
        SearchProcessorResource &processorResource)
{
    RankResource rankResource;
    rankResource.pool = resource.pool;
    rankResource.cavaAllocator = resource.cavaAllocator;
    rankResource.request = request;
    rankResource.matchDataManager = &partitionResource.matchDataManager;

    int32_t totalDocCount = partitionResource.getTotalDocCount();
    rankResource.globalMatchData.setDocCount(totalDocCount);
    rankResource.indexInfoHelper = indexInfoHelper;
    rankResource.requestTracer = resource.tracer;
    rankResource.boostTable = processorResource.rankProfile->getFieldBoostTable();;
    if (partitionResource.indexPartitionReaderWrapper) {
        rankResource.indexReaderPtr =
            partitionResource.indexPartitionReaderWrapper->getReader()->GetIndexReader();
    }
    rankResource.attrExprCreator = partitionResource.attributeExpressionCreator.get();
    rankResource.dataProvider = &resource.dataProvider;
    rankResource.matchDocAllocator = resource.matchDocAllocator;
    rankResource.fieldInfos = resource.tableInfo->getFieldInfos();
    rankResource.queryTerminator = resource.timeoutTerminator;
    rankResource.kvpairs = request ? &request->getConfigClause()->getKVPairs() : NULL;
    rankResource.partitionReaderSnapshot = partitionResource.partitionReaderSnapshot.get();

    return POOL_NEW_CLASS(resource.pool, rank::ScoringProvider, rankResource);
}

bool MatchDocSearcher::doCreateAttributeExpressionVec(const common::AttributeClause *attributeClause) {
    if (!attributeClause) {
        return true;
    }

    const std::set<std::string> &attributeNames = attributeClause->getAttributeNames();
    _attributeExpressionVec.reserve(attributeNames.size());
    for (std::set<std::string>::const_iterator it = attributeNames.begin();
         it != attributeNames.end(); ++it)
    {
        const std::string &attributeName = *it;

        AttributeExpression *expr
            = _partitionResource.attributeExpressionCreator->createAtomicExpr(attributeName);

        if (!expr || !expr->allocate(_resource.matchDocAllocator.get())) {
            HA3_LOG(WARN, "Create attribute expression failed! "
                    "attributeName[%s]", attributeName.c_str());
            _resource.errorResult->addError(ERROR_CREATE_ATTRIBUTE_EXPRESSION,
                    attributeName);
            return false;
        }
        auto refer = expr->getReferenceBase();
        refer->setSerializeLevel(SL_ATTRIBUTE);
        matchdoc::ValueType valueType = refer->getValueType();
        valueType._ha3ReservedFlag = 1;
        refer->setValueType(valueType);
        _attributeExpressionVec.push_back(expr);
    }

    return true;
}

bool MatchDocSearcher::initExtraDocIdentifier(const common::ConfigClause *configClause) {
    uint8_t phaseOneInfoMask = configClause->getPhaseOneBasicInfoMask();
    std::string exprName;
    std::string refName;
    if (!getPKExprInfo(phaseOneInfoMask, exprName, refName)) {
        return false;
    }
    bool isPKNecessary = configClause->getFetchSummaryType() != BY_DOCID;
    AttributeExpression *pkExpr = _partitionResource.attributeExpressionCreator->createAtomicExprWithoutPool(exprName);
    if (!isPKNecessary && !pkExpr) {
        phaseOneInfoMask &= ~(common::pob_primarykey64_flag | common::pob_primarykey128_flag);
        _resource.matchDocAllocator->initPhaseOneInfoReferences(phaseOneInfoMask);
        return true;
    }
    if (!pkExpr) {
        _resource.errorResult->addError(ERROR_CREATE_PK_EXPR, exprName);
        HA3_LOG(WARN, "create pkExpr [%s] failed.", exprName.c_str());
        return false;
    }
    _resource.matchDocAllocator->initPhaseOneInfoReferences(phaseOneInfoMask);
    pkExpr->setOriginalString(refName);
    if (!pkExpr->allocate(_resource.matchDocAllocator.get())) {
        HA3_LOG(WARN, "allocate refer [%s] failed.", refName.c_str());
        return false;
    }

    auto refer = pkExpr->getReferenceBase();
    refer->setSerializeLevel(SL_CACHE);

    _attributeExpressionVec.push_back(pkExpr);
    return true;
}

bool MatchDocSearcher::getPKExprInfo(uint8_t phaseOneInfoMask,
                                     std::string &exprName, std::string &refName) const
{
    if (PHASE_ONE_HAS_RAWPK(phaseOneInfoMask)) {
        const IndexInfo *indexInfo = _resource.tableInfo->getPrimaryKeyIndexInfo();
        if (!indexInfo) {
            HA3_LOG(WARN, "get pk info failed, no primary key index found.");
            return false;
        }
        exprName = indexInfo->fieldName;
        refName = RAW_PRIMARYKEY_REF;
        return true;
    }
    exprName = PRIMARYKEY_REF;
    refName = PRIMARYKEY_REF;
    return true;
}

void MatchDocSearcher::fillAttributePhaseOne(common::MatchDocs *matchDocs) const {
    std::vector<matchdoc::MatchDoc> &matchDocVec = matchDocs->getMatchDocsVect();
    ExpressionEvaluator<std::vector<AttributeExpression *> > evaluator(
            _attributeExpressionVec,
            _resource.matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&matchDocVec[0], matchDocVec.size());
}

void MatchDocSearcher::doLazyEvaluate(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect) const {
    const ExpressionSet &exprSet = _processorResource.comparatorCreator->getLazyEvaluateExpressions();
    ExpressionEvaluator<ExpressionSet> evaluator(exprSet,
            _resource.matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&matchDocVect[0], matchDocVect.size());
}

common::MatchDocs *MatchDocSearcher::createMatchDocs(InnerSearchResult &innerResult) const {
    common::MatchDocs *matchDocs = new common::MatchDocs();
    matchDocs->setTotalMatchDocs(innerResult.totalMatchDocs);
    matchDocs->setActualMatchDocs(innerResult.actualMatchDocs);
    matchDocs->setMatchDocAllocator(_resource.matchDocAllocator);
    matchDocs->resize(innerResult.matchDocVec.size());
    HA3_LOG(TRACE3, "matchDocVect.size()=[%zd]", innerResult.matchDocVec.size());
    for (uint32_t i = 0; i < innerResult.matchDocVec.size(); i++) {
        matchDocs->insertMatchDoc(i, innerResult.matchDocVec[i]);
    }
    fillAttributePhaseOne(matchDocs);
    _resource.matchDocAllocator->dropField(common::SIMPLE_MATCH_DATA_REF);
    _resource.matchDocAllocator->dropField(common::MATCH_DATA_REF);
    return matchDocs;
}


common::ResultPtr MatchDocSearcher::constructErrorResult() const  {
    common::ResultPtr result = common::ResultPtr(new common::Result(new common::MatchDocs()));
    result->addErrorResult(_resource.errorResult);
    return result;
}

void MatchDocSearcher::fillGlobalInformations(common::Result *result) const {
    const common::AttributeItemMapPtr &v =
        _resource.dataProvider.getNeedSerializeGlobalVariables();
    if (v && !v->empty()) {
        result->addGlobalVariableMap(v);
    }
}

common::Result* MatchDocSearcher::constructResult(InnerSearchResult& innerResult) const {
    _resource.sessionMetricsCollector->estimatedMatchCountTrigger(innerResult.totalMatchDocs);
    common::MatchDocs *matchDocs = createMatchDocs(innerResult);
    common::Result *result = new common::Result(matchDocs);
    result->fillAggregateResults(innerResult.aggResultsPtr);
    result->setUseTruncateOptimizer(_resource.sessionMetricsCollector->useTruncateOptimizer());
    fillGlobalInformations(result);
    result->addErrorResult(_resource.errorResult);
    return result;
}

END_HA3_NAMESPACE(search);
