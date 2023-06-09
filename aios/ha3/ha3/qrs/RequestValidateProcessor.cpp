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
#include "ha3/qrs/RequestValidateProcessor.h"

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/LevelClause.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/QueryTokenizer.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/qrs/RequestValidator.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"

using namespace std;
using namespace suez::turing;
using namespace isearch::config;
using namespace isearch::common;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, RequestValidateProcessor);

RequestValidateProcessor::RequestValidateProcessor(
        const ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
        const ClusterConfigMapPtr &clusterConfigMapPtr,
        RequestValidatorPtr &requestValidatorPtr,
        QueryTokenizer &queryTokenizer)
    : _clusterConfigMapPtr(clusterConfigMapPtr)
    , _clusterTableInfoMapPtr(clusterTableInfoMapPtr)
    , _queryTokenizer(queryTokenizer)
    , _requestValidator(*requestValidatorPtr)
{
    _requestValidator.setTracer(getTracer());
}

RequestValidateProcessor::~RequestValidateProcessor() {
}

string RequestValidateProcessor::getBizName(const string &name)
{
    const vector<string> &names = autil::StringUtil::split(name, ZONE_BIZ_NAME_SPLITTER);
    assert(names.size() > 0);
    if (names.size() == 1) {
        return DEFAULT_BIZ_NAME;
    }
    return names[1];
}

void RequestValidateProcessor::process(RequestPtr &requestPtr,
                                       ResultPtr &resultPtr)
{
    assert(resultPtr != NULL);
    if (!validateRequest(requestPtr, resultPtr)) {
        _metricsCollectorPtr->increaseSyntaxErrorQps();
        return;
    }
    QrsProcessor::process(requestPtr, resultPtr);
}

bool RequestValidateProcessor::validateRequest(RequestPtr &requestPtr,
        ResultPtr &resultPtr)
{
    MultiErrorResultPtr errorResultPtr = resultPtr->getMultiErrorResult();
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (NULL == configClause) {
        errorResultPtr->addError(ERROR_NO_CONFIG_CLAUSE);
        AUTIL_LOG(WARN, "%s", haErrorCodeToString(ERROR_NO_CONFIG_CLAUSE).c_str());
        return false;
    }
    //validate pb matchdocs format
    if (!validatePbMatchDocFormat(configClause, errorResultPtr)) {
        return false;
    }

    vector<string> clusterNameVec = std::move(configClause->getClusterNameVector());
    if (clusterNameVec.empty()) {
        errorResultPtr->addError(ERROR_CLUSTER_NAME_NOT_EXIST, "empty cluster");
        return false;
    }
    //validate multi cluster
    if (!validateMultiCluster(clusterNameVec, requestPtr->getLevelClause(),
                              errorResultPtr))
    {
        return false;
    }

    //validate fetch summary cluster
    if (!validateFetchSummaryCluster(configClause, errorResultPtr)) {
        return false;
    }
    //set table name into Request
    const string &clusterName = configClause->getClusterName();
    ClusterTableInfoMap::const_iterator iter = _clusterTableInfoMapPtr->find(clusterName);
    if (iter == _clusterTableInfoMapPtr->end()) {
        string msg = string("cluster name: ") + clusterName;
        errorResultPtr->addError(ERROR_CLUSTER_NAME_NOT_EXIST, msg);
        AUTIL_LOG(WARN, "%s, clusterName:%s",
                haErrorCodeToString(ERROR_CLUSTER_NAME_NOT_EXIST).c_str(),
                clusterName.c_str());
        return false;
    }

    const TableInfoPtr &mainTableInfoPtr = iter->second;
    if (!tokenizeQuery(requestPtr, errorResultPtr, mainTableInfoPtr, clusterName)) {
        return false;
    }
    //validate request
    if(!_requestValidator.validate(requestPtr)) {
        const string &errorMsg = _requestValidator.getErrorMsg();
        errorResultPtr->addError(_requestValidator.getErrorCode(), errorMsg);
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

bool RequestValidateProcessor::tokenizeQuery(const RequestPtr& requestPtr,
        const common::MultiErrorResultPtr &errorResultPtr,
        const suez::turing::TableInfoPtr &mainTableInfoPtr,
        const std::string &clusterName)
{
    //tokenize query
    REQUEST_TRACE(DEBUG, "begin tokenize QueryClause");
    QueryClause *queryClause = requestPtr->getQueryClause();
    if (queryClause == NULL || queryClause->getQueryCount() == 0) {
        errorResultPtr->addError(ERROR_NO_QUERY_CLAUSE);
        AUTIL_LOG(WARN, "%s", haErrorCodeToString(ERROR_NO_QUERY_CLAUSE).c_str());
        return false;
    }
    const ConfigClause *configClause = requestPtr->getConfigClause();
    QueryOperator defaultOP = configClause->getDefaultOP();
    if (OP_UNKNOWN == defaultOP) {
        const QueryInfo &queryInfo = (*_clusterConfigMapPtr)[clusterName].getQueryInfo();
        defaultOP = queryInfo.getDefaultOperator();
    }
    _queryTokenizer.setConfigInfo(configClause->getNoTokenizeIndexes(),
                                  configClause->getAnalyzerName(),
                                  configClause->getIndexAnalyzerMap());
    const IndexInfos *indexInfos = mainTableInfoPtr->getIndexInfos();
    uint32_t queryCount = queryClause->getQueryCount();
    for (uint32_t i = 0; i < queryCount; ++i) {
        auto rootQuery = queryClause->getRootQuery(i);
        Query *cleanQuery = nullptr;
        bool success = tokenizeAndCleanStopWord(rootQuery, defaultOP, indexInfos,
                errorResultPtr, &cleanQuery);
        if (!success) {
            REQUEST_TRACE(ERROR, "tokenizeAndCleanStopWord failed: %s",
                          rootQuery->toString().c_str());
            return false;
        }
        queryClause->setRootQuery(cleanQuery, i);
        if (cleanQuery) {
            REQUEST_TRACE(DEBUG, "no stop word query:%s", cleanQuery->toString().c_str());
        }
    }
    REQUEST_TRACE(DEBUG, "end tokenize QueryClause");

    //tokenize aux query
    REQUEST_TRACE(DEBUG, "begin tokenize AuxQueryClause");
    AuxQueryClause *auxQueryClause = requestPtr->getAuxQueryClause();
    if (auxQueryClause != NULL) {
        TableInfoPtr auxTableInfoPtr;
        if (!getAuxTableInfo(clusterName, errorResultPtr, auxTableInfoPtr)) {
            REQUEST_TRACE(ERROR, "getAuxTableInfo failed");
            return false;
        }
        assert(auxTableInfoPtr);
        const IndexInfos *auxIndexInfos = auxTableInfoPtr->getIndexInfos();
        uint32_t auxQueryCount = auxQueryClause->getQueryCount();
        for (uint32_t i = 0; i < auxQueryCount; ++i) {
            auto rootQuery = auxQueryClause->getRootQuery(i);
            Query *cleanQuery = nullptr;
            bool success = tokenizeAndCleanStopWord(rootQuery, OP_AND, auxIndexInfos,
                    errorResultPtr, &cleanQuery);
            if (!success) {
                REQUEST_TRACE(ERROR, "aux tokenizeAndCleanStopWord failed: %s",
                        rootQuery->toString().c_str());
                return false;
            }
            auxQueryClause->setRootQuery(cleanQuery, i);
            if (cleanQuery) {
                REQUEST_TRACE(DEBUG, "aux no stop word query:%s",
                        cleanQuery->toString().c_str());
            }
        }
    }
    REQUEST_TRACE(DEBUG, "end tokenize AuxQueryClause");
    return true;
}

bool RequestValidateProcessor::validateMultiCluster(
        vector<string> &clusterNameVec, const LevelClause *levelClause,
        MultiErrorResultPtr &errorResultPtr)
{
    if (levelClause && levelClause->maySearchSecondLevel()) {
        const ClusterLists& clusterLists = levelClause->getLevelClusters();
        for (size_t i = 0; i < clusterLists.size(); i++) {
            for (size_t j = 0; j < clusterLists[i].size(); j++) {
                if (find(clusterNameVec.begin(), clusterNameVec.end(),
                                clusterLists[i][j]) == clusterNameVec.end())
                {
                    clusterNameVec.emplace_back(clusterLists[i][j]);
                }
            }
        }
    }

    const string &clusterName = clusterNameVec[0];
    TableInfoPtr tableInfoPtr1;
    if (!getTableInfo(clusterName, errorResultPtr, tableInfoPtr1)) {
        return false;
    }

    for (size_t i = 1; i < clusterNameVec.size(); ++i) {
        TableInfoPtr tableInfoPtr2;
        if (!getTableInfo(clusterNameVec[i], errorResultPtr, tableInfoPtr2)) {
            return false;
        }
        if (!schemaCompatible(tableInfoPtr1, tableInfoPtr2)) {
            errorResultPtr->addError(ERROR_MULTI_CLUSTERNAME_WITH_DIFFERENT_TABLE);
            AUTIL_LOG(WARN, "schema not compatible between cluster[%s] and cluster[%s]",
                    clusterName.c_str(), clusterNameVec[i].c_str());
            return false;
        }
    }
    return true;
}

bool RequestValidateProcessor::validateFetchSummaryCluster(
        ConfigClause *configClause, MultiErrorResultPtr &errorResultPtr)
{
    uint32_t fetchSummaryType = configClause->getFetchSummaryType();
    const string &cluster = configClause->getClusterName();
    const string &fetchSummaryCluster = configClause->getFetchSummaryClusterName(cluster);
    bool sameCluster = cluster == fetchSummaryCluster;

    TableInfoPtr summaryTableInfoPtr;
    ClusterConfigInfo clusterConfig;

    if (!getTableInfo(fetchSummaryCluster, errorResultPtr, summaryTableInfoPtr)
        || !getClusterConfigInfo(fetchSummaryCluster, clusterConfig))
    {
        string errorString = "Cluster: [" + fetchSummaryCluster + "] " +
                             haErrorCodeToString(ERROR_FETCH_SUMMARY_CLUSTER_NOT_EXIST);
        errorResultPtr->addError(ERROR_FETCH_SUMMARY_CLUSTER_NOT_EXIST, errorString);
        AUTIL_LOG(WARN, "%s", errorString.c_str());
        return false;
    }

    if (!sameCluster && fetchSummaryType != BY_RAWPK) {
        errorResultPtr->addError(ERROR_ONLY_SUPPORT_FETCH_SUMMARY_BY_RAWPK);
        AUTIL_LOG(WARN, "%s", haErrorCodeToString(ERROR_ONLY_SUPPORT_FETCH_SUMMARY_BY_RAWPK).c_str());
        return false;
    }

    const IndexInfo* summaryPrimaryKeyIndex =
        summaryTableInfoPtr->getPrimaryKeyIndexInfo();

    if (fetchSummaryType != BY_DOCID && !summaryPrimaryKeyIndex) {
        string errorString = "Cluster: [" + fetchSummaryCluster + "] does not have"
                             " primary key.";
        errorResultPtr->addError(ERROR_SUMMARY_CLUSTER_PK_NOT_MATCH, errorString);
        AUTIL_LOG(WARN, "%s", errorString.c_str());
        return false;
    }

    // if (fetchSummaryType == BY_DOCID) {
    //     string errorString = "Cluster [" + fetchSummaryCluster + "] " +
    //                          "is realtime cluster. Can't fetch summary by docid";
    //     errorResultPtr->addError(ERROR_ONLY_SUPPORT_FETCH_SUMMARY_BY_PK);
    //     AUTIL_LOG(WARN, "%s", errorString.c_str());
    //     return false;
    // }

    if (!sameCluster) {
        TableInfoPtr tableInfoPtr;
        if (!getTableInfo(cluster, errorResultPtr, tableInfoPtr)) {
            string errorString = "Cluster: [" + cluster + "] " +
                                 haErrorCodeToString(ERROR_CLUSTER_NAME_NOT_EXIST);
            errorResultPtr->addError(ERROR_CLUSTER_NAME_NOT_EXIST, errorString);
            AUTIL_LOG(WARN, "%s", errorString.c_str());
            return false;
        }

        const IndexInfo* primaryKeyIndex = tableInfoPtr->getPrimaryKeyIndexInfo();
        assert(fetchSummaryType == BY_RAWPK);
        if (!primaryKeyIndex) {
            string errorString = "Cluster: [" + cluster + "] does not have"
                                 " primary key.";
            errorResultPtr->addError(ERROR_SUMMARY_CLUSTER_PK_NOT_MATCH, errorString);
            AUTIL_LOG(WARN, "%s", errorString.c_str());
            return false;
        }

        if (summaryPrimaryKeyIndex &&
            summaryPrimaryKeyIndex->fieldName != primaryKeyIndex->fieldName)
        {
            string errorString = "Summary cluster [" + fetchSummaryCluster + "] pk field ["
                                 + summaryPrimaryKeyIndex->fieldName + "]"
                                 "does not match cluster [" + cluster + "] pk field ["
                                 + primaryKeyIndex->fieldName + "]";
            errorResultPtr->addError(ERROR_SUMMARY_CLUSTER_PK_NOT_MATCH, errorString);
            AUTIL_LOG(WARN, "%s", errorString.c_str());
        }
    }

    fillPhaseOneInfoMask(configClause, summaryPrimaryKeyIndex);

    return true;
}

void RequestValidateProcessor::fillPhaseOneInfoMask(ConfigClause *configClause,
        const IndexInfo* pkIndexInfo)
{
    // pkIndexInfo maybe null
    uint32_t fetchSummaryType = configClause->getFetchSummaryType();
    assert(pkIndexInfo || fetchSummaryType == BY_DOCID);
    bool needPrimaryKey64 = fetchSummaryType == BY_PK && pkIndexInfo
                            && pkIndexInfo->getIndexType() == it_primarykey64;
    bool needPrimaryKey128 = fetchSummaryType == BY_PK && pkIndexInfo
                             && pkIndexInfo->getIndexType() == it_primarykey128;
    bool needIndexVersion = fetchSummaryType == BY_DOCID;
    bool needFullVersion = fetchSummaryType == BY_DOCID || configClause->isDoDedup();
    bool needRawPk = fetchSummaryType == BY_RAWPK;
    const string& clusterName = configClause->getClusterName();
    const string& fetchSummaryClusterName =
        configClause->getFetchSummaryClusterName(clusterName);
    bool needIp = clusterName == fetchSummaryClusterName;
    uint8_t phaseOneBasicInfoMask = 0;
    if (needRawPk) {
        phaseOneBasicInfoMask |= pob_rawpk_flag;
    }
    if (needPrimaryKey64) {
        phaseOneBasicInfoMask |= pob_primarykey64_flag;
    }
    if (needPrimaryKey128) {
        phaseOneBasicInfoMask |= pob_primarykey128_flag;
    }
    if (needIndexVersion) {
        phaseOneBasicInfoMask |= pob_indexversion_flag;
    }
    if (needFullVersion) {
        phaseOneBasicInfoMask |= pob_fullversion_flag;
    }
    if (needIp) {
        phaseOneBasicInfoMask |= pob_ip_flag;
    }
    configClause->setPhaseOneBasicInfoMask(phaseOneBasicInfoMask);
}

bool RequestValidateProcessor::validatePbMatchDocFormat(
        ConfigClause *configClause, MultiErrorResultPtr &errorResultPtr)
{
    if (!configClause->hasPBMatchDocFormat()) {
        return true;
    }
    if (!configClause->isNoSummary()) {
        errorResultPtr->addError(ERROR_PB_MATCHDOCS_FORMAT_IN_PHASE2,
                "ConfigClause: pb matchdocs format can only be used in phase1.");
        AUTIL_LOG(WARN, "cannot format pb matchdocs in phase2.");
        return false;
    }
    if (RESULT_FORMAT_PROTOBUF != configClause->getResultFormatSetting()) {
        errorResultPtr->addError(ERROR_PB_MATCHDOCS_FORMAT_WITH_XML,
                "ConfigClause: pb matchdocs format can only be used in pb format.");
        AUTIL_LOG(WARN, "cannot format pb matchdocs in phase2.");
        return false;
    }
    return true;
}

bool RequestValidateProcessor::getTableInfo(const string& clusterName,
        MultiErrorResultPtr &errorResultPtr, TableInfoPtr &tableInfoPtr) {
    ClusterTableInfoMap::const_iterator iter = _clusterTableInfoMapPtr->find(clusterName);
    if (iter == _clusterTableInfoMapPtr->end()) {
        string msg = string("cluster name: ") + clusterName;
        errorResultPtr->addError(ERROR_CLUSTER_NAME_NOT_EXIST, msg);
        AUTIL_LOG(WARN, "%s cluster name:%s",
                haErrorCodeToString(ERROR_CLUSTER_NAME_NOT_EXIST).c_str(),
                clusterName.c_str());
        return false;
    }
    tableInfoPtr = iter->second;
    return true;
}

bool RequestValidateProcessor::schemaCompatible(const TableInfoPtr &tableInfoPtr1,
        const TableInfoPtr &tableInfoPtr2)
{
    if (tableInfoPtr1 == tableInfoPtr2) {
        return true;
    }
    if (!indexInfoCompatible(tableInfoPtr1->getIndexInfos(),
                             tableInfoPtr2->getIndexInfos())) {
        return false;
    }
    if (!attributeInfoCompatible(tableInfoPtr1->getAttributeInfos(),
                                 tableInfoPtr2->getAttributeInfos())) {
        return false;
    }
    return true;
}

bool RequestValidateProcessor::indexInfoCompatible(const IndexInfos* indexInfo1,
        const IndexInfos* indexInfo2)
{
    if (indexInfo1 == NULL && indexInfo2 == NULL) {
        return true;
    }
    if (indexInfo1 == NULL || indexInfo2 == NULL) {
        return false;
    }
    for (IndexInfos::ConstIterator it = indexInfo1->begin();
         it != indexInfo1->end(); ++it)
    {
        const IndexInfo *info1 = *it;
        const string &indexName = info1->getIndexName();
        const IndexInfo *info2 = indexInfo2->getIndexInfo(indexName.c_str());
        if (info2 == NULL) {
            continue;
        }
        if (info1->getIndexType() != info2->getIndexType()) {
            AUTIL_LOG(WARN, "index info not compatible, different index types."
                    "index name:%s", indexName.c_str());
            return false;
        }
        if (info1->getAnalyzerName() != info2->getAnalyzerName()) {
            AUTIL_LOG(WARN, "index info not compatible, different analyzers."
                    "index name:%s", indexName.c_str());
            return false;
        }
    }
    return true;
}

bool RequestValidateProcessor::attributeInfoCompatible(
        const AttributeInfos* attrInfo1, const AttributeInfos* attrInfo2)
{
    if (attrInfo1 == NULL && attrInfo2 == NULL) {
        return true;
    }
    if (attrInfo1 == NULL || attrInfo2 == NULL) {
        return false;
    }
    const AttributeInfos::AttrVector attrs = attrInfo1->getAttrbuteInfoVector();
    for (AttributeInfos::AttrVector::const_iterator it = attrs.begin();
         it != attrs.end(); ++it)
    {
        const AttributeInfo *info1 = *it;
        const string &attrName = info1->getAttrName();
        const AttributeInfo *info2 = attrInfo2->getAttributeInfo(attrName.c_str());
        if (info2 == NULL) {
            continue;
        }
        if (info1->getMultiValueFlag() != info2->getMultiValueFlag()) {
            AUTIL_LOG(WARN, "attribute info not compatible, different multivalue flags."
                    "attribute name:%s", attrName.c_str());
            return false;
        }
        if (info1->getFieldType() != info2->getFieldType()) {
            AUTIL_LOG(WARN, "index info not compatible, different field types."
                    "attribute name:%s", attrName.c_str());
            return false;
        }
    }
    return true;
}

QrsProcessorPtr RequestValidateProcessor::clone() {
    QrsProcessorPtr ptr(new RequestValidateProcessor(*this));
    return ptr;
}

void RequestValidateProcessor::setClusterTableInfoMapPtr(
        const ClusterTableInfoMapPtr &clusterTableInfoMapPtr)
{
    _clusterTableInfoMapPtr = clusterTableInfoMapPtr;
}

void RequestValidateProcessor::setClusterConfigInfoMap(
        const config::ClusterConfigMapPtr &clusterConfigMapPtr)
{
    _clusterConfigMapPtr = clusterConfigMapPtr;
}

bool RequestValidateProcessor::getClusterConfigInfo(
        const string &clusterName, ClusterConfigInfo& clusterConfig) const
{
    ClusterConfigMap::const_iterator it = _clusterConfigMapPtr->find(clusterName);
    if (it == _clusterConfigMapPtr->end()) {
        return false;
    }
    clusterConfig = it->second;
    return true;
}

bool RequestValidateProcessor::tokenizeAndCleanStopWord(
        Query *rootQuery,
        QueryOperator defaultOP,
        const IndexInfos *indexInfos,
        const MultiErrorResultPtr &errorResultPtr,
        Query **ppCleanQuery)
{
    if (!rootQuery) {
        errorResultPtr->addError(ERROR_QUERY_CLAUSE);
        AUTIL_LOG(WARN, "%s", haErrorCodeToString(ERROR_QUERY_CLAUSE).c_str());
        return false;
    }
    REQUEST_TRACE(DEBUG, "before tokenize query:%s",
                  rootQuery->toString().c_str());
    if (!_queryTokenizer.tokenizeQuery(rootQuery,
                    indexInfos, defaultOP, true))
    {
        ErrorResult errorResult = _queryTokenizer.getErrorResult();
        errorResultPtr->addError(errorResult.getErrorCode(),
                errorResult.getErrorMsg());
        AUTIL_LOG(WARN, "%s", haErrorCodeToString(errorResult.getErrorCode()).c_str());
        return false;
    }
    *ppCleanQuery = _queryTokenizer.stealQuery();
    if (*ppCleanQuery) {
        REQUEST_TRACE(DEBUG, "tokenized query:%s",
                      (*ppCleanQuery)->toString().c_str());
    }
    return true;
}

bool RequestValidateProcessor::getAuxTableInfo(
        const std::string &clusterName,
        const MultiErrorResultPtr &errorResultPtr,
        TableInfoPtr &auxTableInfoPtr)
{
     assert(errorResultPtr);
     ClusterConfigMap::const_iterator configIter =
        _clusterConfigMapPtr->find(clusterName);
    if (configIter == _clusterConfigMapPtr->end()) {
        errorResultPtr->addError(
                ERROR_CLUSTER_NAME_NOT_EXIST,
                "can not find cluster[" + clusterName + "]'s config name");
        return false;
    }

    const ClusterConfigInfo &clusterConfigInfo = configIter->second;
    string scanJoinCluster =
        clusterConfigInfo.getJoinConfig().getScanJoinCluster();
    if (!scanJoinCluster.empty()) {
        scanJoinCluster = scanJoinCluster + ZONE_BIZ_NAME_SPLITTER +
                          getBizName(clusterName);
        ClusterTableInfoMap::const_iterator auxIter =
            _clusterTableInfoMapPtr->find(scanJoinCluster);
        if (auxIter == _clusterTableInfoMapPtr->end()) {
            errorResultPtr->addError(
                    ERROR_CLUSTER_NAME_NOT_EXIST,
                    "can't find cluster[" + clusterName + "]'s scanJoinCluster:["
                    + scanJoinCluster +  "]");
            return false;
        }
        auxTableInfoPtr = auxIter->second;
        return true;
    }
    errorResultPtr->addError(
            ERROR_CLUSTER_NAME_NOT_EXIST,
            "cluster[" + clusterName + "]'s scanJoinCluster is empty");
    return false;
}

} // namespace qrs
} // namespace isearch
