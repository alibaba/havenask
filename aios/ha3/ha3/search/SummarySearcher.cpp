#include <ha3/search/SummarySearcher.h>
#include <indexlib/util/metrics_center.h>
#include <indexlib/indexlib.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <suez/turing/common/KvTracerMacro.h>
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(func_expression);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SummarySearcher);

using HA3_NS(common)::Result;
using HA3_NS(common)::ResultPtr;

template <typename PKType>
PKType toPrimaryKeyType(primarykey_t pk) {
    return pk;
}

template <>
uint64_t toPrimaryKeyType<uint64_t>(primarykey_t pk) {
    return pk.value[1];
}


SummarySearcher::SummarySearcher(SearchCommonResource& resource,
                                 IndexPartitionWrapperPtr indexPartittionWrapper,
                                 const summary::SummaryProfileManagerPtr& summaryProfileManager,
                                 const config::HitSummarySchemaPtr &hitSummarySchema)
    : _resource(resource)
    , _indexPartittionWrapper(indexPartittionWrapper)
    , _summaryProfileManager(summaryProfileManager)
    , _hitSummarySchema(hitSummarySchema)
    , _traceMode(Tracer::TM_VECTOR)
    , _traceLevel(ISEARCH_TRACE_NONE)
    , _summarySearchType(SUMMARY_SEARCH_NORMAL)
    , _attributeExpressionCreator(NULL)
{
}

SummarySearcher::~SummarySearcher() {
}

const SummaryProfile *SummarySearcher::getSummaryProfile(
        const Request *request,
        SummaryProfileManagerPtr summaryProfileManager)
{
    DocIdClause *docIdClause = request->getDocIdClause();
    /* TODO fetch from Query String*/
    string summaryProfileName = docIdClause->getSummaryProfileName();
    const SummaryProfile *summaryProfile =
        summaryProfileManager->getSummaryProfile(summaryProfileName);

    if (!summaryProfile) {
        string errorMsg = "fail to get summary profile profile name:"
                          + summaryProfileName;
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _resource.errorResult->addError(ERROR_NO_SUMMARYPROFILE, errorMsg);
    }

    return summaryProfile;

}

bool SummarySearcher::declareAttributeFieldsToSummary(
        SummaryExtractorProvider *summaryExtractorProvider)
{
    const vector<string> &requiredAttributeFields =
        _summaryProfileManager->getRequiredAttributeFields();
    for (size_t i = 0; i < requiredAttributeFields.size(); i++) {
        const string &attributeName = requiredAttributeFields[i];
        if (!summaryExtractorProvider->fillAttributeToSummary(attributeName)) {
            HA3_LOG(WARN, "fillAttributeToSummary for %s failed",
                    attributeName.c_str());
            return false;
        }
    }
    return true;
}

bool SummarySearcher::convertPK2DocId(GlobalIdVector &gids,
                                      const IndexPartitionReaderWrapperPtr& indexPartReaderWrapper,
                                      bool ignoreDelete)
{
    IndexReaderPtr primaryKeyReaderPtr =
        indexPartReaderWrapper->getReader()->GetPrimaryKeyReader();
    if (!primaryKeyReaderPtr) {
        _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                "can not find primary key index");
        return false;
    }

    if (primaryKeyReaderPtr->GetIndexType() == it_primarykey64) {
        return convertPK2DocId<uint64_t>(gids, primaryKeyReaderPtr, ignoreDelete);
    } else {
        return convertPK2DocId<uint128_t>(gids, primaryKeyReaderPtr, ignoreDelete);
    }
}

template <typename PKType>
bool SummarySearcher::convertPK2DocId(GlobalIdVector &gids,
                                      const IndexReaderPtr &primaryKeyReaderPtr,
                                      bool ignoreDelete)
{
    PrimaryKeyIndexReaderTyped<PKType> *primaryKeyIndexReader =
        dynamic_cast<PrimaryKeyIndexReaderTyped<PKType>*>(primaryKeyReaderPtr.get());
    if (!primaryKeyIndexReader) {
        HA3_LOG(WARN, "can not get uint128 primaryKeyIndexReader");
        _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                "can not get uint128 primaryKeyIndexReader");
        return false;
    }

    for (GlobalIdVector::iterator it = gids.begin(); it != gids.end(); ++it)
    {
        PKType pk = toPrimaryKeyType<PKType>(it->getPrimaryKey());
        docid_t lastDocId = INVALID_DOCID;
        docid_t docId = primaryKeyIndexReader->Lookup(pk, lastDocId);
        if (docId == INVALID_DOCID && ignoreDelete) {
            docId = lastDocId;
        }
        it->setDocId(docId);
    }
    return true;
}

void SummarySearcher::genSummaryGroupIdVec(ConfigClause *configClause)
{
    const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr = _indexPartittionWrapper->getSummarySchema();
    const string &fetchSummaryGroup = configClause->getFetchSummaryGroup();
    StringTokenizer st(fetchSummaryGroup, CLAUSE_SUB_MULTIVALUE_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() > 0) {
        for (StringTokenizer::Iterator vIt = st.begin();
             vIt != st.end(); vIt++)
        {
            summarygroupid_t groupId = summarySchemaPtr->GetSummaryGroupId(*vIt);
            if (INVALID_SUMMARYGROUPID == groupId) {
                HA3_LOG(WARN, "wrong summary group name "
                        "%s, use default group", fetchSummaryGroup.c_str());
                _summaryGroupIdVec.clear();
                break;
            }
            _summaryGroupIdVec.push_back(groupId);
        }
    }
}

HA3_NS(common)::ResultPtr SummarySearcher::search(const Request *request,
                                  const proto::Range &hashIdRange,
                                  FullIndexVersion fullIndexVersion)
{
    _summarySearchType = SUMMARY_SEARCH_NORMAL;
    KVTRACE_MODULE_SCOPE_WITH_TRACER(
            _resource.sessionMetricsCollector->getTracer(), "summary");
    ConfigClause *configClause = request->getConfigClause();
    assert(configClause);

    if (configClause->getTraceType() == "kvtracer") {
        _traceMode = Tracer::TM_TREE;
    }
    _traceLevel = Tracer::convertLevel(configClause->getTrace());

    genSummaryGroupIdVec(configClause);

    HA3_NS(common)::ResultPtr result(new HA3_NS(common)::Result(new Hits()));
    if (configClause->getFetchSummaryType() == BY_DOCID) {
        fetchSummaryByDocId(result.get(), request, hashIdRange, fullIndexVersion);
    } else {
        fetchSummaryByPk(result.get(), request, hashIdRange);
    }

    result->addErrorResult(_resource.errorResult);
    return result;
}

bool SummarySearcher::createIndexPartReaderWrapperForExtra(const string& tableName)
{
    IndexPartitionReaderPtr mainIndexPartReader =
        _partitionReaderSnapshot->GetIndexPartitionReader(tableName);
    if (!mainIndexPartReader) {
        HA3_LOG(ERROR, "get index partition reader [%s] failed", tableName.c_str());
        return false;
    }
    _indexPartitionReaderWrapper.reset(new IndexPartitionReaderWrapper(
                    NULL, NULL, {mainIndexPartReader}));
    return true;
}

HA3_NS(common)::ResultPtr SummarySearcher::extraSearch(const Request *request,
                                       HA3_NS(common)::ResultPtr inputResult,
                                       const string& tableName)
{
    _summarySearchType = SUMMARY_SEARCH_EXTRA;
    KVTRACE_MODULE_SCOPE_WITH_TRACER(
            _resource.sessionMetricsCollector->getTracer(), "summary");

    ConfigClause *configClause = request->getConfigClause();
    assert(configClause);

    if (configClause->getTraceType() == "kvtracer") {
        _traceMode = Tracer::TM_TREE;
    }
    _traceLevel = Tracer::convertLevel(configClause->getTrace());

    genSummaryGroupIdVec(configClause);
    Hits *inputHits = inputResult->getHits();
    if (!inputHits) {
        return inputResult;
    }

    if (configClause->getFetchSummaryType() == BY_DOCID) {
        HA3_LOG(ERROR, "extra search not support fetch summary by docid");
        inputResult->addErrorResult(ErrorResult(ERROR_GENERAL,
                                                "extra search not support fetch summary by docid"));
        return inputResult;
    }

    if (!createIndexPartReaderWrapperForExtra(tableName)) {
        inputResult->addErrorResult(ErrorResult(ERROR_GENERAL,
                                                "extra search create index partition reader wrapper failed."));
        return inputResult;
    }

    return doExtraSearch(request, inputResult);
}


common::ResultPtr SummarySearcher::doExtraSearch(const Request *request,
                                                 const HA3_NS(common)::ResultPtr &inputResult)
{
    GlobalIdVector gids;
    vector<uint32_t> poses;
    auto inputHits = inputResult->getHits();
    uint32_t size = inputHits->size();
    for (uint32_t i = 0; i < size; ++i) {
        const HitPtr& hit = inputHits->getHit(i);
        if (!hit->hasSummary()) {
            gids.push_back(GlobalIdentifier(0, 0, 0, 0, hit->getPrimaryKey()));
            poses.push_back(i);
        }
    }

    bool ignoreDelete = request->getConfigClause()->ignoreDelete() != ConfigClause::CB_FALSE;
    convertPK2DocId(gids, _indexPartitionReaderWrapper, ignoreDelete);
    HA3_NS(common)::ResultPtr tmpResult(new HA3_NS(common)::Result(new Hits()));
    doSearch(gids, request, tmpResult.get());

    SUMMARY_TRACE(_resource.tracer, INFO, "extra fetch summary by pk");
    tmpResult->addErrorResult(_resource.errorResult);

    // MergeResult
    Hits* tmpHits = tmpResult->getHits();
    assert(poses.size() == tmpHits->size());
    uint32_t returnCount = 0;
    for (uint32_t i = 0; i < poses.size(); ++i) {
        const HitPtr& hit = inputHits->getHit(poses[i]);
        const HitPtr& tmpHit = tmpHits->getHit(i);
        if (tmpHit->hasSummary()) {
            tmpHit->setDocId(hit->getDocId());
            tmpHit->setHashId(hit->getHashId());
            tmpHit->setClusterId(hit->getClusterId());
            hit->stealSummary(*tmpHit);
            ++returnCount;
        }
    }
    _resource.sessionMetricsCollector->extraReturnCountTrigger(returnCount);

    if (tmpResult->hasError()) {
        inputResult->addErrorResult(tmpResult->getMultiErrorResult());
    }
    return inputResult;
}



void SummarySearcher::fetchSummaryByDocId(
        HA3_NS(common)::Result *result,
        const Request *request,
        const proto::Range &hashIdRange,
        FullIndexVersion fullIndexVersion)
{
    GlobalIdVectorMap globalIdVectorMap;
    DocIdClause *docIdClause = request->getDocIdClause();
    docIdClause->getGlobalIdVectorMap(hashIdRange,
            fullIndexVersion, globalIdVectorMap);
    HA3_LOG(DEBUG, "IncVersionToGlobalIdVectorMap.size() = %zd",
            globalIdVectorMap.size());
    for (GlobalIdVectorMap::const_iterator it = globalIdVectorMap.begin();
         it != globalIdVectorMap.end(); ++it)
    {
        if (!createIndexPartReaderWrapper(it->first)) {
            continue;
        }
        HA3_NS(common)::ResultPtr newResult(new HA3_NS(common)::Result(new Hits()));
        if (doSearch(it->second, request, newResult.get())) {
            result->mergeByAppend(newResult, request->getConfigClause()->isDoDedup());
        }
    }
    SUMMARY_TRACE(_resource.tracer, INFO, "fetch summary by docid");
}

void SummarySearcher::fetchSummaryByPk(
        HA3_NS(common)::Result *result,
        const Request *request,
        const proto::Range &hashIdRange)
{
    if (!createIndexPartReaderWrapper(INVALID_VERSION)) {
        return ;
    }
    GlobalIdVector gids;
    DocIdClause *docIdClause = request->getDocIdClause();
    docIdClause->getGlobalIdVector(hashIdRange, gids);
    GlobalIdVector originalGids = gids;
    bool ignoreDelete = request->getConfigClause()->ignoreDelete() != ConfigClause::CB_FALSE;
    if (convertPK2DocId(gids, _indexPartitionReaderWrapper, ignoreDelete) &&
        doSearch(gids, request, result))
    {
        resetGids(result, originalGids);
    }
    SUMMARY_TRACE(_resource.tracer, INFO, "fetch summary by pk");
}

bool SummarySearcher::createIndexPartReaderWrapper(FullIndexVersion fullIndexVersion) {
    if (!_indexPartitionReaderWrapper) {
        HA3_LOG(WARN, "Failed to get IndexPartitionReaderPtr of "
                "version[%d] for phase two", fullIndexVersion);
        _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                "no valid index partition reader.");
        return false;
    }
    return true;
}

// not do summary trace before doSearch
bool SummarySearcher::doSearch(const GlobalIdVector &gids,
                               const Request *request,
                               HA3_NS(common)::Result* result)
{
    const SummaryProfile *summaryProfile = getSummaryProfile(request, _summaryProfileManager);
    if (!summaryProfile) {
        return false;
    }

    SearchPartitionResource partitionResource(_resource, request,
            _indexPartitionReaderWrapper, _partitionReaderSnapshot);

    DocIdClause *docIdClause = request->getDocIdClause();
    SummaryQueryInfo queryInfo(docIdClause->getQueryString(),
                               docIdClause->getTermVector());

    SummaryExtractorProvider summaryExtractorProvider(&queryInfo,
            &summaryProfile->getFieldSummaryConfig(),
            request,
            // for test
            (_attributeExpressionCreator
             ? _attributeExpressionCreator :
             partitionResource.attributeExpressionCreator.get()),
            _hitSummarySchema.get(), // will modify schema
            _resource);

    if (!declareAttributeFieldsToSummary(&summaryExtractorProvider)) {
        string errMsg = "failed to declare attribute fields to summary";
        HA3_LOG(WARN, "%s", errMsg.c_str());
        _resource.errorResult->addError(ERROR_SUMMARY_DECLARE_ATTRIBUTE, errMsg);
        return false;
    }

    unique_ptr<SummaryExtractorChain> summaryExtractorChain(summaryProfile->createSummaryExtractorChain());

    if (!summaryExtractorChain->beginRequest(&summaryExtractorProvider)) {
        string errMsg = "fail to beginRequest of summaryExtractor";
        HA3_LOG(WARN, "%s", errMsg.c_str());
        _resource.errorResult->addError(ERROR_SUMMARY_EXTRACTOR_BEGIN_REQUEST, errMsg);
        return false;
    }

    bool allowLackOfSummary = request->getConfigClause()->allowLackOfSummary();
    int64_t signature = request->getDocIdClause()->getSignature();

    doSearch(gids, allowLackOfSummary, signature, request->getPool(), result,
             summaryExtractorChain.get(),
             &summaryExtractorProvider);

    summaryExtractorChain->endRequest();
    return true;
}

void SummarySearcher::resetGids(HA3_NS(common)::Result *result, const GlobalIdVector &originalGids) {
    Hits *hits = result->getHits();
    assert((size_t)hits->size() == originalGids.size());

    for (uint32_t i = 0; i < hits->size(); ++i) {
        const HitPtr &hitPtr = hits->getHit(i);
        hitPtr->setGlobalIdentifier(originalGids[i]);
    }
}

void SummarySearcher::doSearch(const GlobalIdVector &gids,
                               bool allowLackOfSummary,
                               int64_t signature,
                               autil::mem_pool::Pool *pool,
                               HA3_NS(common)::Result* result,
                               SummaryExtractorChain *summaryExtractorChain,
                               SummaryExtractorProvider *summaryExtractorProvider)
{
    Hits *hits = result->getHits();
    HitSummarySchema *hitSummarySchema =
        summaryExtractorProvider->getHitSummarySchema();
    for (GlobalIdVector::const_iterator it = gids.begin();
         it != gids.end(); ++it)
    {
        const GlobalIdentifier &gid = *it;
        HitPtr hitPtr(new Hit(gid.getDocId()));

        Tracer *tracer = NULL;
        if (_resource.tracer != NULL) {
            tracer = _resource.tracer->cloneWithoutTraceInfo();
            hitPtr->setTracerWithoutCreate(tracer);
        }
        hitPtr->setGlobalIdentifier(gid);
        hitPtr->setSummaryHit(new SummaryHit(hitSummarySchema, pool, tracer));
        hits->addHit(hitPtr);
    }
    fillHits(hits, summaryExtractorChain, summaryExtractorProvider, allowLackOfSummary);
    hits->setSummaryFilled();
    hits->summarySchemaToSignature();
    // do not return hitsummaryschema to qrs if schema signature is the same.
    if (signature != hitSummarySchema->getSignature()) {
        hits->addHitSummarySchema(hitSummarySchema->clone());
    }
}

void SummarySearcher::fillHits(Hits *hits,
                               SummaryExtractorChain *summaryExtractorChain,
                               SummaryExtractorProvider *summaryExtractorProvider,
                               bool allowLackOfSummary)

{
    _resource.sessionMetricsCollector->fetchSummaryStartTrigger(_summarySearchType);
    fetchSummaryDocuments(hits, summaryExtractorProvider, allowLackOfSummary);
    _resource.sessionMetricsCollector->fetchSummaryEndTrigger(_summarySearchType);
    _resource.sessionMetricsCollector->extractSummaryStartTrigger(_summarySearchType);

    size_t totalFetchSummarySize = 0;
    for(uint32_t i = 0; i < hits->size(); ++i)
    {
        const HitPtr &hitPtr = hits->getHit(i);
        SummaryHit *summaryHit = hitPtr->getSummaryHit();
        if (summaryHit) {
            size_t summaryFieldCount = summaryHit->getFieldCount();
            for (size_t j = 0; j < summaryFieldCount; ++j) {
                const ConstString* field = summaryHit->getFieldValue(j);
                totalFetchSummarySize += field ? field->size() : 0;
            }
            summaryExtractorChain->extractSummary(*summaryHit);
        }
    }

    _resource.sessionMetricsCollector->totalFetchSummarySizeTrigger(
        totalFetchSummarySize, _summarySearchType);
    _resource.sessionMetricsCollector->extractSummaryEndTrigger( _summarySearchType);
}

void SummarySearcher::fetchSummaryDocuments(
        Hits *hits,
        SummaryExtractorProvider *summaryExtractorProvider,
        bool allowLackOfSummary)
{
    SummaryFetcher summaryFetcher(
            summaryExtractorProvider->getFilledAttributes(),
            _resource.matchDocAllocator.get(),
            _indexPartitionReaderWrapper->getReader()->GetSummaryReader());
    ErrorCode errorCode = ERROR_NONE;
    string errorMsg;
    try {
        doFetchSummaryDocument(summaryFetcher, hits, allowLackOfSummary);
    } catch (const ExceptionBase &e) {
        HA3_LOG(ERROR, "Fetch summary documents exception: %s", e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_FETCH_SUMMARY_FILEIO_ERROR;
        }
    } catch (const std::exception& e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
    }

    if (errorCode != ERROR_NONE) {
        HA3_LOG(ERROR, "Fetch summary documents failed,"
                "Exception: [%s]", errorMsg.c_str());
        _resource.errorResult->addError(errorCode, errorMsg);
    }
}

void SummarySearcher::doFetchSummaryDocument(
        SummaryFetcher &summaryFetcher, Hits *hits, bool allowLackOfSummary)
{
    uint32_t hitSize = hits->size();
    HA3_LOG(DEBUG, "hitSize=%d", hitSize);
    uint32_t i = 0;
    for (; i < hitSize; i++) {
        if (_resource.timeoutTerminator && _resource.timeoutTerminator->checkTimeout()) {
            HA3_LOG(WARN, "fetch summary timeout, required summary size[%d],"
                    " actual fetch summary size[%d]", hitSize, i);
            _resource.errorResult->addError(ERROR_FETCH_SUMMARY_TIMEOUT);
            break;
        }
        Hit *hit = hits->getHit(i).get();
        docid_t docId = hit->getDocId();
        if (docId < 0) {
            hit->setSummaryHit(NULL);
            if (!allowLackOfSummary) {
                HA3_LOG(WARN, "%s", "invalid docid, fetch summary failed!");
            }
            continue;
        }
        if (!summaryFetcher.fetchSummary(hit, _summaryGroupIdVec)) {
            hit->setSummaryHit(NULL);
            stringstream ss;
            if (!allowLackOfSummary) {
                ss << "fail to get document gid[" <<
                    (hit->getGlobalIdentifier().toString()) << "].";
                HA3_LOG(WARN, "%s", ss.str().c_str());
            }
            _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY, ss.str());
            continue;
        }
    }

    for (; i < hitSize; i++) {
        Hit *hit = hits->getHit(i).get();
        hit->setSummaryHit(NULL);
    }
}

END_HA3_NAMESPACE(search);
