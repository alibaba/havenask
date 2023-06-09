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
#include "ha3/search/SummarySearcher.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/StringTokenizer.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/PBResultFormatter.h"
#include "ha3/common/Request.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "ha3/common/SummaryHit.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SummaryFetcher.h"
#include "ha3/summary/SummaryExtractorChain.h"
#include "ha3/summary/SummaryExtractorProvider.h"
#include "ha3/summary/SummaryProfile.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/util/KeyValueMap.h"
#ifndef AIOS_OPEN_SOURCE
#include "kvtracer/KVTracer.h"
#endif
#include "suez/turing/common/KvTracerMacro.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace proto {
class Range;
} // namespace proto
} // namespace isearch

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::mem_pool;

using namespace indexlibv2::index;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::common;
using namespace indexlib::util;

using namespace isearch::util;
using namespace isearch::common;
using namespace isearch::config;
using namespace isearch::summary;
using namespace isearch::monitor;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SummarySearcher);

using isearch::common::Result;
using isearch::common::ResultPtr;

SummarySearcher::SummarySearcher(SearchCommonResource &resource,
                                 const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
                                 indexlib::partition::PartitionReaderSnapshot *snapshot,
                                 const summary::SummaryProfileManagerPtr &summaryProfileManager,
                                 const config::HitSummarySchemaPtr &hitSummarySchema)
    : _resource(resource)
    , _indexPartitionReaderWrapper(indexPartitionReaderWrapper)
    , _partitionReaderSnapshot(snapshot)
    , _summaryProfileManager(summaryProfileManager)
    , _hitSummarySchema(hitSummarySchema)
    , _traceMode(Tracer::TM_VECTOR)
    , _traceLevel(ISEARCH_TRACE_NONE)
    , _summarySearchType(SUMMARY_SEARCH_NORMAL)
    , _attributeExpressionCreator(NULL) {}

SummarySearcher::~SummarySearcher() {}

const SummaryProfile *
SummarySearcher::getSummaryProfile(const Request *request,
                                   SummaryProfileManagerPtr summaryProfileManager) {
    DocIdClause *docIdClause = request->getDocIdClause();
    /* TODO fetch from Query String*/
    string summaryProfileName = docIdClause->getSummaryProfileName();
    const SummaryProfile *summaryProfile
        = summaryProfileManager->getSummaryProfile(summaryProfileName);

    if (!summaryProfile) {
        string errorMsg = "fail to get summary profile profile name:" + summaryProfileName;
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        _resource.errorResult->addError(ERROR_NO_SUMMARYPROFILE, errorMsg);
    }

    return summaryProfile;
}

bool SummarySearcher::declareAttributeFieldsToSummary(
    SummaryExtractorProvider *summaryExtractorProvider) {
    const vector<string> &requiredAttributeFields
        = _summaryProfileManager->getRequiredAttributeFields();
    for (size_t i = 0; i < requiredAttributeFields.size(); i++) {
        const string &attributeName = requiredAttributeFields[i];
        if (!summaryExtractorProvider->fillAttributeToSummary(attributeName)) {
            AUTIL_LOG(WARN, "fillAttributeToSummary for %s failed", attributeName.c_str());
            return false;
        }
    }
    return true;
}

bool SummarySearcher::convertPK2DocId(GlobalIdVector &gids,
                                      const IndexPartitionReaderWrapperPtr &indexPartReaderWrapper,
                                      bool ignoreDelete) {
    std::shared_ptr<InvertedIndexReader> primaryKeyReaderPtr = indexPartReaderWrapper->getPrimaryKeyReader();
    if (!primaryKeyReaderPtr) {
        _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                                        "can not find primary key index");
        return false;
    }

    for (GlobalIdVector::iterator it = gids.begin(); it != gids.end(); ++it) {
        auto pk = it->getPrimaryKey();
        docid_t docId = INVALID_DOCID;
        if (!indexPartReaderWrapper->pk2DocId(pk, ignoreDelete, docId)) {
            AUTIL_LOG(ERROR, "pk convert to docid failed");
            _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                                            "pk convert to docid failed");
            return false;
        }
        it->setDocId(docId);
    }
    return true;
}

void SummarySearcher::genSummaryGroupIdVec(ConfigClause *configClause) {
    auto schema = _indexPartitionReaderWrapper->getSchema();
    auto indexConfigs = schema->GetIndexConfigs(indexlibv2::index::SUMMARY_INDEX_TYPE_STR);
    if (indexConfigs.empty()) {
        AUTIL_LOG(WARN, "summary schema is empty.");
        return;
    }
    assert(indexConfigs.size() == 1u);

    const string &fetchSummaryGroup = configClause->getFetchSummaryGroup();
    StringTokenizer st(fetchSummaryGroup,
                       CLAUSE_SUB_MULTIVALUE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    const auto &summaryIndexConfig
        = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(indexConfigs[0]);
    assert(summaryIndexConfig);

    if (st.getNumTokens() > 0) {
        for (StringTokenizer::Iterator vIt = st.begin(); vIt != st.end(); vIt++) {
            summarygroupid_t groupId = summaryIndexConfig->GetSummaryGroupId(*vIt);
            if (INVALID_SUMMARYGROUPID == groupId) {
                AUTIL_LOG(WARN,
                          "wrong summary group name "
                          "%s, use default group",
                          fetchSummaryGroup.c_str());
                _summaryGroupIdVec.clear();
                break;
            }
            _summaryGroupIdVec.push_back(groupId);
        }
    }
}

isearch::common::ResultPtr SummarySearcher::search(const Request *request,
                                                   const proto::Range &hashIdRange,
                                                   FullIndexVersion fullIndexVersion) {
    _summarySearchType = SUMMARY_SEARCH_NORMAL;
    KVTRACE_MODULE_SCOPE_WITH_TRACER(_resource.sessionMetricsCollector->getTracer(), "summary");
    ConfigClause *configClause = request->getConfigClause();
    assert(configClause);

    if (configClause->getTraceType() == "kvtracer") {
        _traceMode = Tracer::TM_TREE;
    }
    _traceLevel = Tracer::convertLevel(configClause->getTrace());

    genSummaryGroupIdVec(configClause);

    isearch::common::ResultPtr result(new isearch::common::Result(new Hits()));
    if (configClause->getFetchSummaryType() == BY_DOCID) {
        fetchSummaryByDocId(result.get(), request, hashIdRange, fullIndexVersion);
    } else {
        fetchSummaryByPk(result.get(), request, hashIdRange);
    }

    result->addErrorResult(_resource.errorResult);
    return result;
}

bool SummarySearcher::createIndexPartReaderWrapperForExtra(const string &tableName) {
    if (_partitionReaderSnapshot == nullptr) {
        AUTIL_LOG(ERROR, "table [%s], partition reader snapshot is empty.", tableName.c_str());
        return false;
    }
    auto tabletReader = _partitionReaderSnapshot->GetTabletReader(tableName);
    if (tabletReader) {
        _indexPartitionReaderWrapper.reset(new IndexPartitionReaderWrapper(tabletReader));
        return true;
    }
    IndexPartitionReaderPtr mainIndexPartReader
        = _partitionReaderSnapshot->GetIndexPartitionReader(tableName);
    if (!mainIndexPartReader) {
        AUTIL_LOG(ERROR, "get index partition reader [%s] failed", tableName.c_str());
        return false;
    }
    vector<IndexPartitionReaderPtr> *indexReaderVec
        = new vector<IndexPartitionReaderPtr>({mainIndexPartReader});
    _indexPartitionReaderWrapper.reset(
        new IndexPartitionReaderWrapper(nullptr, nullptr, indexReaderVec, true));
    return true;
}

isearch::common::ResultPtr SummarySearcher::extraSearch(const Request *request,
                                                        isearch::common::ResultPtr inputResult,
                                                        const string &tableName) {
    _summarySearchType = SUMMARY_SEARCH_EXTRA;
    KVTRACE_MODULE_SCOPE_WITH_TRACER(_resource.sessionMetricsCollector->getTracer(), "summary");

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
        AUTIL_LOG(ERROR, "extra search not support fetch summary by docid");
        inputResult->addErrorResult(
            ErrorResult(ERROR_GENERAL, "extra search not support fetch summary by docid"));
        return inputResult;
    }

    if (!createIndexPartReaderWrapperForExtra(tableName)) {
        inputResult->addErrorResult(ErrorResult(
            ERROR_GENERAL, "extra search create index partition reader wrapper failed."));
        return inputResult;
    }

    return doExtraSearch(request, inputResult);
}

common::ResultPtr SummarySearcher::doExtraSearch(const Request *request,
                                                 const isearch::common::ResultPtr &inputResult) {
    GlobalIdVector gids;
    vector<uint32_t> poses;
    auto inputHits = inputResult->getHits();
    uint32_t size = inputHits->size();
    for (uint32_t i = 0; i < size; ++i) {
        const HitPtr &hit = inputHits->getHit(i);
        if (!hit->hasSummary()) {
            gids.push_back(GlobalIdentifier(0, 0, 0, 0, hit->getPrimaryKey()));
            poses.push_back(i);
        }
    }

    bool ignoreDelete = request->getConfigClause()->ignoreDelete() != ConfigClause::CB_FALSE;
    convertPK2DocId(gids, _indexPartitionReaderWrapper, ignoreDelete);
    isearch::common::ResultPtr tmpResult(new isearch::common::Result(new Hits()));
    doSearch(gids, request, tmpResult.get());

    SUMMARY_TRACE(_resource.tracer, INFO, "extra fetch summary by pk");
    tmpResult->addErrorResult(_resource.errorResult);

    // MergeResult
    Hits *tmpHits = tmpResult->getHits();
    assert(poses.size() == tmpHits->size());
    uint32_t returnCount = 0;
    for (uint32_t i = 0; i < poses.size(); ++i) {
        const HitPtr &hit = inputHits->getHit(poses[i]);
        const HitPtr &tmpHit = tmpHits->getHit(i);
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

void SummarySearcher::fetchSummaryByDocId(isearch::common::Result *result,
                                          const Request *request,
                                          const proto::Range &hashIdRange,
                                          FullIndexVersion fullIndexVersion) {
    GlobalIdVectorMap globalIdVectorMap;
    DocIdClause *docIdClause = request->getDocIdClause();
    docIdClause->getGlobalIdVectorMap(hashIdRange, fullIndexVersion, globalIdVectorMap);
    AUTIL_LOG(DEBUG, "IncVersionToGlobalIdVectorMap.size() = %zd", globalIdVectorMap.size());
    for (GlobalIdVectorMap::const_iterator it = globalIdVectorMap.begin();
         it != globalIdVectorMap.end();
         ++it) {
        if (!createIndexPartReaderWrapper(it->first)) {
            continue;
        }
        isearch::common::ResultPtr newResult(new isearch::common::Result(new Hits()));
        if (doSearch(it->second, request, newResult.get())) {
            result->mergeByAppend(newResult, request->getConfigClause()->isDoDedup());
        }
    }
    SUMMARY_TRACE(_resource.tracer, INFO, "fetch summary by docid");
}

void SummarySearcher::fetchSummaryByPk(isearch::common::Result *result,
                                       const Request *request,
                                       const proto::Range &hashIdRange) {
    if (!createIndexPartReaderWrapper(INVALID_VERSION)) {
        return;
    }
    GlobalIdVector gids;
    DocIdClause *docIdClause = request->getDocIdClause();
    docIdClause->getGlobalIdVector(hashIdRange, gids);
    GlobalIdVector originalGids = gids;
    bool ignoreDelete = request->getConfigClause()->ignoreDelete() != ConfigClause::CB_FALSE;
    if (convertPK2DocId(gids, _indexPartitionReaderWrapper, ignoreDelete)
        && doSearch(gids, request, result)) {
        resetGids(result, originalGids);
    }
    SUMMARY_TRACE(_resource.tracer, INFO, "fetch summary by pk");
}

bool SummarySearcher::createIndexPartReaderWrapper(FullIndexVersion fullIndexVersion) {
    if (!_indexPartitionReaderWrapper) {
        AUTIL_LOG(WARN,
                  "Failed to get IndexPartitionReaderPtr of "
                  "version[%d] for phase two",
                  fullIndexVersion);
        _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY,
                                        "no valid index partition reader.");
        return false;
    }
    return true;
}

// not do summary trace before doSearch
bool SummarySearcher::doSearch(const GlobalIdVector &gids,
                               const Request *request,
                               isearch::common::Result *result) {
    const SummaryProfile *summaryProfile = getSummaryProfile(request, _summaryProfileManager);
    if (!summaryProfile) {
        return false;
    }

    SearchPartitionResource partitionResource(
        _resource, request, _indexPartitionReaderWrapper, _partitionReaderSnapshot);

    DocIdClause *docIdClause = request->getDocIdClause();
    SummaryQueryInfo queryInfo(docIdClause->getQueryString(), docIdClause->getTermVector());

    SummaryExtractorProvider summaryExtractorProvider(
        &queryInfo,
        &summaryProfile->getFieldSummaryConfig(),
        request,
        // for test
        (_attributeExpressionCreator ? _attributeExpressionCreator
                                     : partitionResource.attributeExpressionCreator.get()),
        _hitSummarySchema.get(), // will modify schema
        _resource);

    if (!declareAttributeFieldsToSummary(&summaryExtractorProvider)) {
        string errMsg = "failed to declare attribute fields to summary";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        _resource.errorResult->addError(ERROR_SUMMARY_DECLARE_ATTRIBUTE, errMsg);
        return false;
    }

    unique_ptr<SummaryExtractorChain> summaryExtractorChain(
        summaryProfile->createSummaryExtractorChain());

    if (!summaryExtractorChain->beginRequest(&summaryExtractorProvider)) {
        string errMsg = "fail to beginRequest of summaryExtractor";
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        _resource.errorResult->addError(ERROR_SUMMARY_EXTRACTOR_BEGIN_REQUEST, errMsg);
        return false;
    }

    bool allowLackOfSummary = request->getConfigClause()->allowLackOfSummary();
    int64_t signature = request->getDocIdClause()->getSignature();

    doSearch(gids,
             allowLackOfSummary,
             signature,
             request->getPool(),
             result,
             summaryExtractorChain.get(),
             &summaryExtractorProvider);

    summaryExtractorChain->endRequest();
    return true;
}

void SummarySearcher::resetGids(isearch::common::Result *result,
                                const GlobalIdVector &originalGids) {
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
                               isearch::common::Result *result,
                               SummaryExtractorChain *summaryExtractorChain,
                               SummaryExtractorProvider *summaryExtractorProvider) {
    Hits *hits = result->getHits();
    HitSummarySchema *hitSummarySchema = summaryExtractorProvider->getHitSummarySchema();
    for (GlobalIdVector::const_iterator it = gids.begin(); it != gids.end(); ++it) {
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
    fillHits(hits, pool, summaryExtractorChain, summaryExtractorProvider, allowLackOfSummary);
    hits->setSummaryFilled();
    hits->summarySchemaToSignature();
    // do not return hitsummaryschema to qrs if schema signature is the same.
    if (signature != hitSummarySchema->getSignature()) {
        hits->addHitSummarySchema(hitSummarySchema->clone());
    }
}

void SummarySearcher::fillHits(Hits *hits,
                               autil::mem_pool::Pool *pool,
                               SummaryExtractorChain *summaryExtractorChain,
                               SummaryExtractorProvider *summaryExtractorProvider,
                               bool allowLackOfSummary)

{
    _resource.sessionMetricsCollector->fetchSummaryStartTrigger(_summarySearchType);
    fetchSummaryDocuments(hits, pool, summaryExtractorProvider, allowLackOfSummary);
    _resource.sessionMetricsCollector->fetchSummaryEndTrigger(_summarySearchType);
    _resource.sessionMetricsCollector->extractSummaryStartTrigger(_summarySearchType);

    size_t totalFetchSummarySize = 0;
    for (uint32_t i = 0; i < hits->size(); ++i) {
        const HitPtr &hitPtr = hits->getHit(i);
        SummaryHit *summaryHit = hitPtr->getSummaryHit();
        if (summaryHit) {
            size_t summaryFieldCount = summaryHit->getFieldCount();
            for (size_t j = 0; j < summaryFieldCount; ++j) {
                const StringView *field = summaryHit->getFieldValue(j);
                totalFetchSummarySize += field ? field->size() : 0;
            }
            summaryExtractorChain->extractSummary(*summaryHit);
        }
    }

    _resource.sessionMetricsCollector->totalFetchSummarySizeTrigger(totalFetchSummarySize,
                                                                    _summarySearchType);
    _resource.sessionMetricsCollector->extractSummaryEndTrigger(_summarySearchType);
}

void SummarySearcher::fetchSummaryDocuments(Hits *hits,
                                            autil::mem_pool::Pool *pool,
                                            SummaryExtractorProvider *summaryExtractorProvider,
                                            bool allowLackOfSummary) {
    SummaryFetcher summaryFetcher(summaryExtractorProvider->getFilledAttributes(),
                                  _resource.matchDocAllocator.get(),
                                  _indexPartitionReaderWrapper->getSummaryReader());
    ErrorCode errorCode = ERROR_NONE;
    string errorMsg;
    try {
        doFetchSummaryDocument(summaryFetcher, hits, pool, allowLackOfSummary);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "Fetch summary documents exception: %s", e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_FETCH_SUMMARY_FILEIO_ERROR;
        }
    } catch (const std::exception &e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_FETCH_SUMMARY;
    }

    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(ERROR,
                  "Fetch summary documents failed,"
                  "Exception: [%s]",
                  errorMsg.c_str());
        _resource.errorResult->addError(errorCode, errorMsg);
    }
}

void SummarySearcher::doFetchSummaryDocument(SummaryFetcher &summaryFetcher,
                                             Hits *hits,
                                             autil::mem_pool::Pool *pool,
                                             bool allowLackOfSummary) {
    assert(pool);
    uint32_t hitSize = hits->size();
    AUTIL_LOG(DEBUG, "hitSize=%d", hitSize);
    indexlib::index::ErrorCodeVec summaryResult;
    if (_resource.timeoutTerminator) {
        if (_resource.timeoutTerminator->getLeftTime() < 0) {
            summaryResult.assign(hitSize, indexlib::index::ErrorCode::Timeout);
        } else {
            autil::TimeoutTerminator terminatorForSummary(
                _resource.timeoutTerminator->getTimeout(),
                _resource.timeoutTerminator->getStartTime());
            terminatorForSummary.init(1, 1);
            summaryResult = summaryFetcher.batchFetchSummary(
                hits, pool, &terminatorForSummary, _summaryGroupIdVec);
        }
    } else {
        summaryResult = summaryFetcher.batchFetchSummary(hits, pool, nullptr, _summaryGroupIdVec);
    }

    bool hasTimeout = false;
    int32_t filledSize = 0;
    for (uint32_t i = 0; i < hitSize; ++i) {
        if (summaryResult[i] == indexlib::index::ErrorCode::OK) {
            filledSize++;
            continue;
        }
        Hit *hit = hits->getHit(i).get();
        hit->setSummaryHit(NULL);
        if (summaryResult[i] == indexlib::index::ErrorCode::Timeout) {
            hasTimeout = true;
        } else {
            docid_t docId = hit->getDocId();
            if (docId < 0) {
                if (!allowLackOfSummary) {
                    AUTIL_LOG(WARN, "%s", "invalid docid, fetch summary failed!");
                }
            } else {
                stringstream ss;
                if (!allowLackOfSummary) {
                    ss << "fail to get document gid[" << (hit->getGlobalIdentifier().toString())
                       << "].";
                    AUTIL_LOG(WARN, "%s", ss.str().c_str());
                }
                _resource.errorResult->addError(ERROR_SEARCH_FETCH_SUMMARY, ss.str());
            }
        }
    }
    if (hasTimeout) {
        AUTIL_LOG(WARN,
                  "fetch summary timeout, required summary size[%d],"
                  " actual fetch summary size[%d]",
                  hitSize,
                  filledSize);
        _resource.errorResult->addError(ERROR_FETCH_SUMMARY_TIMEOUT);
    }
}

} // namespace search
} // namespace isearch
