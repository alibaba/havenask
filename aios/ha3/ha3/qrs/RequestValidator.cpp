#include <ha3/qrs/RequestValidator.h>
#include <ha3/qrs/QueryValidator.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <autil/StringUtil.h>
#include <ha3/qrs/ConfigClauseValidator.h>
#include <ha3/common/Hit.h>
#include <iostream>
#include <ha3/qrs/LayerClauseValidator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, RequestValidator);

bool RequestValidator::_syntaxExprValidateTable[SYTAX_EXPR_LOCATION_COUNT][vt_type_count][2];
bool RequestValidator::_subDocValidateTable[3][SYTAX_EXPR_LOCATION_COUNT][2];

static bool isSyntaxExprValidateTableInited = RequestValidator::initSyntaxExprValidateTable();
static bool isSubDocValidateTableInited = RequestValidator::initSubDocValidateTable();

static string sLocationString[SYTAX_EXPR_LOCATION_COUNT]  = {
    "SORT_SYNTAX_LOCATION",
    "DISTINCT_SYNTAX_LOCATION",
    "DISTINCT_FILTER_SYNTAX_LOCATION",
    "FILTER_SYNTAX_LOCATION",
    "AGGKEY_SYNTAX_LOCATION",
    "AGGFUN_SYNTAX_LOCATION",
    "AGGFILTER_SYNTAX_LOCATION",
    "RANGE_AGGKEY_SYNTAX_LOCATION",
    "SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION",
    "SEARCHER_CACHE_FILTER_SYNTAX_LOCATION",
    "RANK_SORT_SYNTAX_LOCATION",
    "AUX_FILTER_SYNTAX_LOCATION",
};

RequestValidator::RequestValidator(const ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
                                   uint32_t retHitsLimit,
                                   const ClusterConfigMapPtr &clusterConfigMapPtr,
                                   const ClusterFuncMapPtr &clusterFuncMapPtr,
                                   const CavaPluginManagerMapPtr &clusterCavaPluginManagerMapPtr,
                                   const ClusterSorterNamesPtr &clusterSorterNamesPtr)
    : _clusterTableInfoMapPtr(clusterTableInfoMapPtr)
    , _indexInfos(NULL)
    , _attrInfos(NULL)
    , _retHitsLimit(retHitsLimit)
    , _clusterConfigMapPtr(clusterConfigMapPtr)
    , _clusterFuncMapPtr(clusterFuncMapPtr)
    , _clusterCavaPluginManagerMapPtr(clusterCavaPluginManagerMapPtr)
    , _clusterSorterNamesPtr(clusterSorterNamesPtr)
    , _syntaxExprValidator(NULL)
    , _auxSyntaxExprValidator(nullptr)
{
    reset();
}

RequestValidator::~RequestValidator() {
    (void) isSyntaxExprValidateTableInited;
    (void) isSubDocValidateTableInited;
    DELETE_AND_SET_NULL(_syntaxExprValidator);
    DELETE_AND_SET_NULL(_auxSyntaxExprValidator);
}

string RequestValidator::getBizName(const string &name)
{
    vector<string> names =
            autil::StringUtil::split(name, ZONE_BIZ_NAME_SPLITTER);
    assert(names.size() > 0);
    if (names.size() == 1) {
        return DEFAULT_BIZ_NAME;
    }
    return names[1];
}

bool RequestValidator::validate(const RequestPtr& requestPtr) {
    reset();
    const ConfigClause *configClause = requestPtr->getConfigClause();
    if (!validateConfigClause(configClause)) {
        return false;
    }

    vector<string> clusterNames = configClause->getClusterNameVector();
    _subDocDisplayType = configClause->getSubDocDisplayType();
    const LevelClause *levelClause = requestPtr->getLevelClause();
    if (levelClause && levelClause->maySearchSecondLevel()) {
        const ClusterLists& clusterLists = levelClause->getLevelClusters();
        for (size_t i = 0; i < clusterLists.size(); i++) {
            for (size_t j = 0; j < clusterLists[i].size(); j++) {
                if (find(clusterNames.begin(), clusterNames.end(),
                                clusterLists[i][j]) == clusterNames.end())
                {
                    clusterNames.push_back(clusterLists[i][j]);
                }
            }
        }
    }

    for (vector<string>::const_iterator it = clusterNames.begin();
         it != clusterNames.end(); ++it)
    {
        const string &clusterName = *it;
        ClusterTableInfoMap::const_iterator iter = _clusterTableInfoMapPtr->find(clusterName);
        if (iter == _clusterTableInfoMapPtr->end()) {
            _errorCode = ERROR_CLUSTER_NAME_NOT_EXIST;
            _errorMsg =  "can not find cluster[" + clusterName
                         + "]'s table name";
            return false;
        }
        const TableInfoPtr &tableInfoPtr = iter->second;
        _indexInfos = tableInfoPtr->getIndexInfos();
        _attrInfos = tableInfoPtr->getAttributeInfos();
        //TODO reject query if thers is no sub schema but sub_doc isn't no.
        //bool hasSubSchema = tableInfoPtr->hasSubSchema();
        // if(!hasSubSchema && _subDocDisplayType != SUB_DOC_DISPLAY_NO) {
        //     _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
        //     _errorMsg = "no sub doc schema is c.";
        //     return false;
        // }
        if (requestPtr->getAuxQueryClause() ||
            requestPtr->getAuxFilterClause()) {
            ClusterConfigMap::const_iterator configIter =
                _clusterConfigMapPtr->find(clusterName);
            if (configIter == _clusterConfigMapPtr->end()) {
                _errorCode = ERROR_CLUSTER_NAME_NOT_EXIST;
                _errorMsg =
                    "can not find cluster[" + clusterName + "]'s config name";
                return false;
            }

            DELETE_AND_SET_NULL(_auxSyntaxExprValidator);
            TableInfoPtr auxTableInfoPtr;
            _auxIndexInfos = nullptr;
            _auxAttrInfos = nullptr;
            const ClusterConfigInfo &auxClusterConfigInfo = configIter->second;
            string scanJoinCluster =
                auxClusterConfigInfo.getJoinConfig().getScanJoinCluster();
            if (!scanJoinCluster.empty()) {
                scanJoinCluster = scanJoinCluster + ZONE_BIZ_NAME_SPLITTER +
                                  getBizName(clusterName);
                ClusterTableInfoMap::const_iterator auxIter =
                    _clusterTableInfoMapPtr->find(scanJoinCluster);
                if (auxIter == _clusterTableInfoMapPtr->end()) {
                    _errorCode = ERROR_CLUSTER_NAME_NOT_EXIST;
                    _errorMsg = "cluster[" + clusterName + "]'s scanJoinCluster is empty";
                    return false;
                }
                auxTableInfoPtr = auxIter->second;
                _auxIndexInfos = auxTableInfoPtr->getIndexInfos();
                _auxAttrInfos = auxTableInfoPtr->getAttributeInfos();

                _auxSyntaxExprValidator = new SyntaxExprValidator(
                    _auxAttrInfos, VirtualAttributes(), false);
                addFunctionInfo(clusterNames, _auxSyntaxExprValidator);
            } else {
                _errorCode = ERROR_NO_SCAN_JOIN_CLUSTER_NAME;
                _errorMsg = "can not find scan join cluster for[" +
                            scanJoinCluster + "]'s table name";
                return false;
	    }
        }

        vector<VirtualAttribute*> virtualAttributes;
        DELETE_AND_SET_NULL(_syntaxExprValidator);
        _syntaxExprValidator = new SyntaxExprValidator(_attrInfos,
                virtualAttributes, _subDocDisplayType != SUB_DOC_DISPLAY_NO);
        addFunctionInfo(clusterNames, _syntaxExprValidator);

        const QueryClause *queryClause = requestPtr->getQueryClause();
        if (!validateQueryClause(queryClause)) {
            return false;
        }
        const VirtualAttributeClause *virtualAttributeClause =
            requestPtr->getVirtualAttributeClause();
        //validateVirtualAttributeClause will change virtualAttributes
        if (!validateVirtualAttributeClause(virtualAttributeClause,
                        virtualAttributes))
        {
            return false;
        }

        const FilterClause *filterClause = requestPtr->getFilterClause();
        if (!validateFilterClause(filterClause)) {
            return false;
        }

        const SortClause *sortClause = requestPtr->getSortClause();
        if (!validateSortClause(sortClause)) {
            return false;
        }

        const RankSortClause *rankSortClause = requestPtr->getRankSortClause();
        if (!validateRankSortClause(rankSortClause)) {
            return false;
        }

        const DistinctClause *distinctClause = requestPtr->getDistinctClause();
        if (!validateDistinctClause(distinctClause)) {
            return false;
        }

        if (rankSortClause && distinctClause) {
            size_t rankSortDescCount = rankSortClause->getRankSortDescCount();
            if (rankSortDescCount > 1) {
                const DistinctDescription* firstDistinctDesc =
                    distinctClause->getDistinctDescription((uint32_t)0);
                if (NULL != firstDistinctDesc) {
                    _errorMsg = "multi rank Sort round conflict with first round distinct.";
                    _errorCode =  ERROR_RANK_SORT_CLAUSE_AND_DISTINCT_CLAUSE_CONFLICT;
                    HA3_LOG(WARN, "%s", _errorMsg.c_str());
                    return false;
                }
            }
        }

        const AggregateClause *aggregateClause = requestPtr->getAggregateClause();
        if (!validateAggregateClause(aggregateClause)) {
            return false;
        }

        const AttributeClause *attributeClause = requestPtr->getAttributeClause();
        if (!validateAttributeClause(attributeClause, virtualAttributes, _subDocDisplayType)) {
            return false;
        }

        const SearcherCacheClause *searcherCacheClause = requestPtr->getSearcherCacheClause();
        if (!validateSearcherCacheClause(searcherCacheClause)) {
            return false;
        }

        virtualAttributes.clear();
    }

    const PKFilterClause *pkFilterClause = requestPtr->getPKFilterClause();
    if (!validatePKFilterClause(pkFilterClause)) {
        return false;
    }

    const HealthCheckClause *healthCheckClause =
        requestPtr->getHealthCheckClause();
    if (!validateHealthCheckClause(healthCheckClause)) {
        return false;
    }

    const FetchSummaryClause *fetchSummaryClause =
        requestPtr->getFetchSummaryClause();
    if (!validateFetchSummaryClause(fetchSummaryClause, configClause)) {
        return false;
    }
    // may do compatible update
    QueryLayerClause *queryLayerClause = requestPtr->getQueryLayerClause();
    if (!validateQueryLayerClause(queryLayerClause)) {
        return false;
    }

    const FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
    if (!validateFinalSortClause(finalSortClause, configClause)) {
        return false;
    }

    if (!validateAuxQueryClause(requestPtr->getAuxQueryClause())) {
	return false;
    }

    if (!validateAuxFilterClause(requestPtr->getAuxFilterClause())) {
	return false;
    }

    return true;
}

void RequestValidator::addFunctionInfo(std::vector<std::string> &clusterNames,
                                       SyntaxExprValidator *syntaxExprValidator)
{
    for (auto name : clusterNames) {
        if (_clusterFuncMapPtr && !_clusterFuncMapPtr->empty()) {
            auto it = _clusterFuncMapPtr->find(name);
            if (it != _clusterFuncMapPtr->end()) {
                syntaxExprValidator->addFuncInfoMap(&it->second);
            }
        }
    }
    for (auto name : clusterNames) {
        if (_clusterCavaPluginManagerMapPtr && !_clusterCavaPluginManagerMapPtr->empty()) {
            auto it = _clusterCavaPluginManagerMapPtr->find(name);
            if (it != _clusterCavaPluginManagerMapPtr->end()) {
                syntaxExprValidator->addCavaPluginManager(it->second);
            }
        }
    }
}

bool RequestValidator::validateConfigClause(const ConfigClause* configClause)
{
    ConfigClauseValidator configValidator(_retHitsLimit);
    if (!configValidator.validate(configClause))
    {
        _errorCode = configValidator.getErrorCode();
        _errorMsg = haErrorCodeToString(_errorCode);
        return false;
    }
    return true;
}

bool RequestValidator::validateHealthCheckClause(
        const common::HealthCheckClause* healthCheckClause) {
    if (!healthCheckClause) {
        return true;
    }
    if (healthCheckClause->getCheckTimes() < 0) {
        _errorCode = ERROR_HEALTHCHECK_CHECK_TIMES;
        _errorMsg = haErrorCodeToString(_errorCode);
        return false;
    }
    return true;
}

bool RequestValidator::validateQueryClause(const QueryClause* queryClause) {
    if (queryClause == NULL || queryClause->getQueryCount() == 0) {
        _errorCode = ERROR_NO_QUERY_CLAUSE;
        _errorMsg = haErrorCodeToString(_errorCode);
        return false;
    }
    HA3_LOG(DEBUG, "query string[%s]", queryClause->getOriginalString().c_str());
    uint32_t queryCount = queryClause->getQueryCount();
    for (uint32_t i = 0; i < queryCount; ++i) {
        Query* query = queryClause->getRootQuery(i);
        if (!query) {
            _errorCode = ERROR_NO_QUERY_CLAUSE;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }
        HA3_LOG(DEBUG, "query name[%s]", query->getQueryName().c_str());
        QueryValidator queryValidator(_indexInfos);
        query->accept(&queryValidator);
        _errorCode = queryValidator.getErrorCode();
        if (_errorCode != ERROR_NONE) {
            _errorMsg = queryValidator.getErrorMsg();
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateVirtualAttributeClause(
        const VirtualAttributeClause* virtualAttributeClause,
        vector<VirtualAttribute*> &virtualAttributes)
{
    if (NULL == virtualAttributeClause) {
        return true;
    }

    const vector<VirtualAttribute*> &origVirtualAttributes =
        virtualAttributeClause->getVirtualAttributes();
    vector<VirtualAttribute*>::const_iterator it = origVirtualAttributes.begin();
    for (; it != origVirtualAttributes.end(); ++it) {
        if (!validateVirtualAttributeName(virtualAttributes,
                        (*it)->getVirtualAttributeName()))
        {
            _errorCode = ERROR_VIRTUALATTRIBUTE_CLAUSE;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }

        SyntaxExpr* syntaxExpr = (*it)->getVirtualAttributeSyntaxExpr();
        bool isValidate = _syntaxExprValidator->validate(syntaxExpr);
        ExprResultType resultType = vt_unknown;
        if (syntaxExpr) {
            resultType = syntaxExpr->getExprResultType();
        }

        if (isValidate) {
            if (vt_unknown != resultType) {
                virtualAttributes.push_back(*it);
                _syntaxExprValidator->addVirtualAttribute(*it);
            } else {
                _errorCode = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
                _errorMsg = "validate virtual attribute["
                            + (*it)->getVirtualAttributeName() + "] failed";
                return false;
            }
        } else {
            _errorCode = _syntaxExprValidator->getErrorCode();
            _errorMsg = "validate virtual attribute["
                        + (*it)->getVirtualAttributeName() + "] failed, " +
                        _syntaxExprValidator->getErrorMsg();
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateVirtualAttributeName(
        const vector<VirtualAttribute*> &virtualAttributes, const string &name)
{
    if ((NULL != _attrInfos && NULL != _attrInfos->getAttributeInfo(name.c_str())))
    {
        HA3_LOG(WARN, "virtual attribute name [%s] conflict with attribute.",
                name.c_str());
        return false;
    }

    for (vector<VirtualAttribute*>::const_iterator it = virtualAttributes.begin();
         it != virtualAttributes.end(); ++it)
    {
        if (name == (*it)->getVirtualAttributeName()) {
            HA3_LOG(WARN, "virtual attribute name [%s] conflict with other "
                    "virtual attribute.", name.c_str());
            return false;
        }
    }

    return true;
}

bool RequestValidator::validateFilterClause(const FilterClause* filterClause,
        SyntaxExprLocation location)
{
    if (filterClause == NULL) {
        return true;
    }
    const SyntaxExpr *syntaxExpr = filterClause->getRootSyntaxExpr();
    return validateSyntaxExpr(syntaxExpr, location);
}

bool RequestValidator::validatePKFilterClause(const PKFilterClause* pkFilterClause) {
    if(pkFilterClause == NULL){
        return true;
    }
    string pkString = pkFilterClause->getOriginalString();
    if(pkString.empty()){
        _errorCode = ERROR_PKFILTER_CLAUSE;
        _errorMsg = "The content of pkfilter is empty!";
        return false;
    }
    return true;
}

bool RequestValidator::validateSortClause(const SortClause* sortClause)
{
    if (sortClause == NULL) {
        return true;
    }

    const vector<SortDescription*>& sortDes = sortClause->getSortDescriptions();
    bool hasRank = false;
    set<string> exprStrSet;
    for (vector<SortDescription*>::const_iterator it = sortDes.begin();
         it != sortDes.end(); it++)
    {
        if ((*it)->isRankExpression()) {
            if (hasRank) {
                _errorCode = ERROR_SORT_SAME_EXPRESSION;
                _errorMsg = "two rank expression";
                return false;
            }
            hasRank = true;
            continue;
        }

        const SyntaxExpr *syntaxExpr = (*it)->getRootSyntaxExpr();
        if (!validateSyntaxExpr(syntaxExpr, SORT_SYNTAX_LOCATION)) {
            return false;
        }
        const string &exprStr = syntaxExpr->getExprString();
        if (exprStrSet.find(exprStr) != exprStrSet.end()) {
            _errorCode = ERROR_SORT_SAME_EXPRESSION;
            _errorMsg = "two expression [" + exprStr + "]";
            return false;
        }
        exprStrSet.insert(exprStr);
    }
    return true;
}

bool RequestValidator::validateFinalSortClause(
        const FinalSortClause *finalSortClause,
        const ConfigClause *configClause)
{
    if (finalSortClause == NULL
        || finalSortClause->useDefaultSort()) {
        return true;
    }
    const string &sortName = finalSortClause->getSortName();
    const vector<string> &clusterNameVec = configClause->getClusterNameVector();
    for (size_t i = 0; i < clusterNameVec.size(); i++) {
        string clusterName = clusterNameVec[i];
        if ((*_clusterSorterNamesPtr)[clusterName].find(sortName)
            == (*_clusterSorterNamesPtr)[clusterName].end())
        {
            _errorCode = ERROR_FINAL_SORT_INVALID_SORT_NAME;
            _errorMsg = "sortname [" + sortName + "] is undefined in cluster [" + clusterName + "]";
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateRankSortClause(const RankSortClause* rankSortClause)
{
    if (rankSortClause == NULL) {
        return true;
    }

    for (size_t i = 0; i < rankSortClause->getRankSortDescCount(); ++i) {
        const vector<SortDescription*>& sortDes = rankSortClause->getRankSortDesc(i)->getSortDescriptions();
        bool hasRank = false;
        set<string> exprStrSet;
        for (vector<SortDescription*>::const_iterator it = sortDes.begin();
             it != sortDes.end(); it++)
        {
            if ((*it)->isRankExpression()) {
                if (hasRank) {
                    _errorCode = ERROR_SORT_SAME_EXPRESSION;
                    _errorMsg = "two rank expression";
                    return false;
                }
                hasRank = true;
                continue;
            }

            const SyntaxExpr *syntaxExpr = (*it)->getRootSyntaxExpr();
            if (!validateSyntaxExpr(syntaxExpr, RANK_SORT_SYNTAX_LOCATION)) {
                return false;
            }
            const string &exprStr = syntaxExpr->getExprString();
            if (exprStrSet.find(exprStr) != exprStrSet.end()) {
                _errorCode = ERROR_SORT_SAME_EXPRESSION;
                _errorMsg = "two expression [" + exprStr + "]";
                return false;
            }
            exprStrSet.insert(exprStr);
        }
    }
    return true;
}

bool RequestValidator::validateFetchSummaryClause(
        const FetchSummaryClause *fetchSummaryClause,
        const ConfigClause *configClause)
{
    if (fetchSummaryClause == NULL) {
        if (configClause->getResultFormatSetting() != RESULT_FORMAT_FB_SUMMARY) {
            return true;
        }
        _errorCode = ERROR_UNSUPPORT_RESULT_FORMAT;
        _errorMsg = "Only support type fb_summary for phase two";
        return false;
    }
    if (fetchSummaryClause->getClusterCount() == 0) {
        _errorCode = ERROR_FETCH_SUMMARY_CLAUSE;
        _errorMsg = "Error fetch summary clause, empty cluster name";
        return false;
    }
    const vector<string> &clusterNames = fetchSummaryClause->getClusterNames();
    const vector<GlobalIdentifier> &gids = fetchSummaryClause->getGids();
    const vector<string> &rawPks = fetchSummaryClause->getRawPks();
    uint32_t fetchSummaryType = configClause->getFetchSummaryType();

    if (fetchSummaryType == BY_DOCID || fetchSummaryType == BY_PK) {
        if (clusterNames.size() != gids.size()) {
            _errorCode = ERROR_FETCH_SUMMARY_CLAUSE;
            _errorMsg = "parse fetch summary failed";
            return false;
        }
    } else {
        if (clusterNames.size() != rawPks.size()) {
            _errorCode = ERROR_FETCH_SUMMARY_CLAUSE;
            _errorMsg = "parse fetch summary failed";
            return false;
        }
    }

    for (size_t i = 0; i < clusterNames.size(); ++i) {
        const string &clusterName = clusterNames[i];
        ClusterConfigMap::const_iterator it =
            _clusterConfigMapPtr->find(clusterName);
        if (it == _clusterConfigMapPtr->end()) {
            _errorCode = ERROR_CLUSTER_NAME_NOT_EXIST;
            _errorMsg = "invalid fetch summary clause,  cluster: "
                        + clusterName + " not exist";
            return false;
        }
        if (fetchSummaryType == BY_DOCID) {
            const GlobalIdentifier &gid = gids[i];
            if (gid.getDocId() < 0 || gid.getIndexVersion() < 0) {
                _errorCode = ERROR_FETCH_SUMMARY_GID_FORMAT;
                _errorMsg = "error gid, docid and index version must greater"
                            " than 0";
                return false;
            }
        } else {
            ClusterTableInfoMap::const_iterator iter =
                _clusterTableInfoMapPtr->find(clusterName);
            assert(iter != _clusterTableInfoMapPtr->end());
            const TableInfoPtr &tableInfoPtr = iter->second;
            const string &tableName = tableInfoPtr->getTableName();
            const IndexInfo *pkInfo = tableInfoPtr->getPrimaryKeyIndexInfo();
            if (pkInfo == NULL) {
                _errorCode = ERROR_FETCH_SUMMARY_CLAUSE;
                _errorMsg = "invalid fetch summary clause,  table: "
                            + tableName + " does not have primary key index";
                return false;
            }
            if (fetchSummaryType == BY_RAWPK) {
                const vector<string> &hashFields = it->second.getHashMode()._hashFields;
                const string &pkField = pkInfo->fieldName;
                if (hashFields.empty() || hashFields[0] != pkField) {
                    _errorCode = ERROR_FETCH_SUMMARY_CLAUSE;
                    _errorMsg = "invalid fetch summary clause,  cluster: "
                                + clusterName + " should have same primary key"
                                " field and hash field";
                    return false;
                }
            }
        }
    }
    return true;
}

bool RequestValidator::validateAttributeClause(
        const AttributeClause* attributeClause,
        const vector<VirtualAttribute*> &virtualAttributes,
        SubDocDisplayType subDocDisplayType)
{
    if (attributeClause == NULL) {
        return true;
    }

    const set<string> &attrNames = attributeClause->getAttributeNames();
    for (set<string>::const_iterator it = attrNames.begin();
         it != attrNames.end(); ++it)
    {
        const string &attrName = *it;
        const AttributeInfo *attrInfo = NULL;
        if ((NULL == _attrInfos)
            || (NULL == (attrInfo = _attrInfos->getAttributeInfo(attrName))))
        {
            vector<VirtualAttribute*>::const_iterator it = virtualAttributes.begin();
            for (;it != virtualAttributes.end(); ++it)
            {
                if (attrName == (*it)->getVirtualAttributeName()) {
                    break;
                }
            }
            if (virtualAttributes.end() == it)
            {
                _errorCode = ERROR_ATTRIBUTE_NOT_EXIST;
                ostringstream oss;
                oss << "attribute [" << attrName << "] not exist";
                _errorMsg = oss.str();
                return false;
            }
        } else if (attrInfo != NULL && attrInfo->getSubDocAttributeFlag()
                   &&subDocDisplayType != SUB_DOC_DISPLAY_FLAT)
        {
                _errorCode = ERROR_ATTRIBUTE_EXIST_IN_SUB_DOC;
                ostringstream oss;
                oss << "attribute [" << attrName << "] is exist in sub doc";
                _errorMsg = oss.str();
                return false;
        }
    }
    return true;
}

bool RequestValidator::validateQueryLayerClause(
        QueryLayerClause* layerClause)
{
    LayerClauseValidator validator(_attrInfos);
    if (!validator.validate(layerClause)) {
        _errorCode = validator.getErrorCode();
        _errorMsg = validator.getErrorMsg();
        return false;
    }
    return true;
}

bool RequestValidator::validateDistinctClause(const DistinctClause* distinctClause)
{
    if (distinctClause == NULL) {
        return true;
    }

    uint32_t noneDistDescCount = 0;
    const vector<DistinctDescription*> &distDescs =
        distinctClause->getDistinctDescriptions();
    for (vector<DistinctDescription*>::const_iterator it = distDescs.begin();
         it != distDescs.end(); ++it)
    {
        DistinctDescription *distDesc = *it;
        if (NULL == distDesc) {
            noneDistDescCount ++;
            continue;
        }

        SyntaxExpr* syntaxExpr = distDesc->getRootSyntaxExpr();
        if (!validateSyntaxExpr(syntaxExpr, DISTINCT_SYNTAX_LOCATION))
        {
            return false;
        }

        if (distDesc->getDistinctTimes() <= 0) {
            _errorCode = ERROR_DISTINCT_TIMES;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }

        if (distDesc->getDistinctCount() <= 0) {
            _errorCode = ERROR_DISTINCT_COUNT;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }

        if (distDesc->getMaxItemCount() < 0) {
            _errorCode = ERROR_DISTINCT_MAX_ITEM_COUNT;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }

        FilterClause *filterClause = distDesc->getFilterClause();
        if (filterClause) {
            if (!validateFilterClause(filterClause, DISTINCT_FILTER_SYNTAX_LOCATION)) {
                return false;
            }
        }
    }

    uint32_t distDescCount = distDescs.size();
    if (distDescCount == 0 ||
        distDescCount > DistinctClause::MAX_DISTINCT_DESC_COUNT)
    {
        _errorCode = ERROR_DISTINCT_DESC_COUNT;
        _errorMsg = "wrong distinct description count, "
                    "it must be >0 and <=2.";
        HA3_LOG(WARN, "%s original string: %s", _errorMsg.c_str(),
                distinctClause->getOriginalString().c_str());
        return false;
    }

    if (distDescCount - noneDistDescCount < 1) {
        _errorCode = ERROR_DISTINCT_DESC_COUNT;
        _errorMsg = "no effective distinct description , "
                    "it must be one at least.";
        HA3_LOG(WARN, "%s original string: %s", _errorMsg.c_str(),
                distinctClause->getOriginalString().c_str());
        return false;
    }

    return true;
}


bool RequestValidator::validateAggregateClause(
        const AggregateClause* aggregateClause)
{
    if (aggregateClause == NULL) {
        return true;
    }

    const AggregateDescriptions& aggDesVec = aggregateClause->getAggDescriptions();

    for (AggregateDescriptions::const_iterator it = aggDesVec.begin();
         it != aggDesVec.end(); ++it)
    {
        //validate group key expression
        const SyntaxExpr *groupKeyExpr = (*it)->getGroupKeyExpr();
        SyntaxExprLocation location = AGGKEY_SYNTAX_LOCATION;
        if ((*it)->isRangeAggregate() ) {
            location = RANGE_AGGKEY_SYNTAX_LOCATION;
        }
        if(!groupKeyExpr || !validateSyntaxExpr(groupKeyExpr, location)) {
            HA3_LOG(WARN, "AggregateClause: groupKeyExpr validate error");
            return false;
        }
        //validate aggregate function
        const vector<AggFunDescription*> &aggFunDesVec
            = (*it)->getAggFunDescriptions();
        if (aggFunDesVec.size() == 0){
            HA3_LOG(WARN, "aggFunDesVec.size() == 0");
            return false;
        }

        bool allowSubExpr = groupKeyExpr->isSubExpression();
        for (size_t i = 0; i < aggFunDesVec.size(); i++) {
            if (!validateAggFunDescription(aggFunDesVec[i], allowSubExpr)) {
                HA3_LOG(WARN, "validate the %lu aggregate Function error", i);
                return false;
            }
        }

        FilterClause *filterClause = (*it)->getFilterClause();
        if (filterClause) {
            if (!validateFilterClause(filterClause, AGGFILTER_SYNTAX_LOCATION)) {
                return false;
            } else if (!allowSubExpr && filterClause->getRootSyntaxExpr()->isSubExpression()) {
                _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
                _errorMsg = "group_key is not sub while agg_filter is sub";
                return false;
            }
        }
        //validate aggregate ranges
        const vector<string> &ranges = (*it)->getRange();
        ExprResultType exprResultType = groupKeyExpr->getExprResultType();
        if (!validateAggregateRange(exprResultType, ranges)) {
            HA3_LOG(WARN, "validate aggregate range error");
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateAggregateRange(ExprResultType resultType,
        const vector<string> &ranges)
{
    HA3_LOG(TRACE3, "ranges.size: %ld", ranges.size());
    if (ranges.size() < 2) {
        return true;
    }

    bool bAscendOrder = true;

#define VALIDATE_AGG_RANGE_HELPER(vt_type)                              \
    case vt_type:                                                       \
        {                                                               \
            typedef VariableTypeTraits<vt_type, false>::AttrItemType T; \
            bAscendOrder = validateAscendOrder<T>(ranges);              \
        }                                                               \
    break                                                               \

    switch(resultType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(VALIDATE_AGG_RANGE_HELPER);
    default:
        _errorCode = ERROR_AGG_RANGE_TYPE;
        ostringstream oss;
        oss << "agg range type invalid, --ExprResultType:["
            << resultType << "]";
        _errorMsg = oss.str();
        return false;
    }
#undef VALIDATE_AGG_RANGE_HELPER
    return bAscendOrder;
}

bool RequestValidator::validateAggFunDescription(
        const AggFunDescription* aggFunDes, bool allowSubExpr)
{
    const string &funName = aggFunDes->getFunctionName();
    if (funName != AGGREGATE_FUNCTION_COUNT) {
        const SyntaxExpr* syntaxExpr = aggFunDes->getSyntaxExpr();
        if (!validateSyntaxExpr(syntaxExpr, AGGFUN_SYNTAX_LOCATION)) {
            HA3_LOG(WARN, "aggregate function expression validate error");
            return false;
        }
        if (!allowSubExpr && syntaxExpr->isSubExpression()) {
            _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
            _errorMsg = "agg fun should not be sub when group key is not sub";
            return false;
        }
        HA3_LOG(TRACE3, "funName:%s, ExprResultType: %d",
            funName.c_str(), syntaxExpr->getExprResultType());
    }

    return validateAggFunName(funName);
}

bool RequestValidator::validateAggFunName(const string &aggFunName) {
    bool isValidate = (aggFunName == AGGREGATE_FUNCTION_COUNT) ||
                      (aggFunName == AGGREGATE_FUNCTION_MAX) ||
                      (aggFunName == AGGREGATE_FUNCTION_MIN) ||
                      (aggFunName == AGGREGATE_FUNCTION_SUM) ||
                      (aggFunName == AGGREGATE_FUNCTION_DISTINCT_COUNT);

    if (!isValidate) {
        HA3_LOG(WARN, "not supported aggregate function name: [%s]",
            aggFunName.c_str());
        _errorCode = ERROR_AGG_FUN_NAME;
        _errorMsg = string(":not supported aggregate function name[") +
                    aggFunName + string("]");
    }

    return isValidate;
}

bool RequestValidator::validateSyntaxExpr(const SyntaxExpr *syntaxExpr,
        SyntaxExprLocation location)
{
    if (syntaxExpr == NULL) {
        HA3_LOG(WARN, "the parameter of syntaxExpr is NULL.");
        return false;
    }

    _syntaxExprValidator->validate(syntaxExpr);
    _errorCode = _syntaxExprValidator->getErrorCode();
    if (_errorCode != ERROR_NONE) {
        _errorMsg = _syntaxExprValidator->getErrorMsg();
        return false;
    }

    auto vtType = syntaxExpr->getExprResultType();
    bool isMulti = syntaxExpr->isMultiValue();
    if (! _syntaxExprValidateTable[location][vtType][ isMulti ? 1 : 0] ) {
        _errorCode = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
        _errorMsg = "syntax location: " + sLocationString[location];
        return false;
    }
    bool isSubExpr = syntaxExpr->isSubExpression();
    if(!_subDocValidateTable[_subDocDisplayType][location][isSubExpr? 1 : 0]) {
        _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
        _errorMsg = "syntax location: "  + sLocationString[location];
        return false;
    }

    return true;
}

bool RequestValidator::validateAuxSyntaxExpr(const SyntaxExpr *syntaxExpr,
        SyntaxExprLocation location)
{
    if (syntaxExpr == NULL) {
        HA3_LOG(WARN, "the parameter of syntaxExpr is NULL.");
        return false;
    }

    _auxSyntaxExprValidator->validate(syntaxExpr);
    _errorCode = _auxSyntaxExprValidator->getErrorCode();
    if (_errorCode != ERROR_NONE) {
        _errorMsg = _auxSyntaxExprValidator->getErrorMsg();
        return false;
    }

    auto vtType = syntaxExpr->getExprResultType();
    bool isMulti = syntaxExpr->isMultiValue();
    if (! _syntaxExprValidateTable[location][vtType][ isMulti ? 1 : 0] ) {
        _errorCode = ERROR_SYNTAX_EXPRESSION_TYPE_NOT_SUPPORT;
        _errorMsg = "syntax location: " + sLocationString[location];
        return false;
    }
    bool isSubExpr = syntaxExpr->isSubExpression();
    if(isSubExpr) {
        _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
        _errorMsg = "syntax location: "  + sLocationString[location];
        return false;
    }

    return true;
}

bool RequestValidator::validateSearcherCacheClause(
        const SearcherCacheClause *searcherCacheClause)
{
    if (searcherCacheClause == NULL) {
        return true;
    }
    SyntaxExpr *expireTimeExpr = searcherCacheClause->getExpireExpr();
    if (expireTimeExpr &&
        !validateSyntaxExpr(expireTimeExpr, SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION))
    {
        return false;
    }

    FilterClause *filterClause = searcherCacheClause->getFilterClause();
    if (filterClause && !validateFilterClause(filterClause, SEARCHER_CACHE_FILTER_SYNTAX_LOCATION)) {
        return false;
    }

    const set<string> &refreshAttributes = searcherCacheClause->getRefreshAttributes();
    for (set<string>::const_iterator it = refreshAttributes.begin();
         it != refreshAttributes.end();  it++)
    {
        if (_attrInfos->getAttributeInfo(*it) == NULL) {
            _errorCode = ERROR_INVALID_REFRESH_ATTRIBUTE;
            _errorMsg = string("error searcher cache, "
                    "not exist refresh attribute ") + (*it);
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateAuxQueryClause(
        const AuxQueryClause *auxQueryClause) {
    if (!auxQueryClause) {
	return true;
    }
    HA3_LOG(DEBUG, "aux query string[%s]",
            auxQueryClause->getOriginalString().c_str());
    uint32_t queryCount = auxQueryClause->getQueryCount();
    for (uint32_t i = 0; i < queryCount; ++i) {
        Query* auxQuery = auxQueryClause->getRootQuery(i);
        if (!auxQuery) {
            _errorCode = ERROR_NO_AUX_QUERY_CLAUSE;
            _errorMsg = haErrorCodeToString(_errorCode);
            return false;
        }
        HA3_LOG(DEBUG, "aux query name[%s]", auxQuery->getQueryName().c_str());
        QueryValidator queryValidator(_auxIndexInfos);
        auxQuery->accept(&queryValidator);
        _errorCode = queryValidator.getErrorCode();
        if (_errorCode != ERROR_NONE) {
            _errorMsg = queryValidator.getErrorMsg();
            return false;
        }
    }
    return true;
}

bool RequestValidator::validateAuxFilterClause(
        const AuxFilterClause *auxFilterClause, SyntaxExprLocation location) {
    if (auxFilterClause == NULL) {
        return true;
    }
    const SyntaxExpr *syntaxExpr = auxFilterClause->getRootSyntaxExpr();
    return validateAuxSyntaxExpr(syntaxExpr, location);
}

void RequestValidator::reset() {
    _errorCode = ERROR_NONE;
    _errorMsg = "";
    _subDocDisplayType = SUB_DOC_DISPLAY_NO;
}

void RequestValidator::setSubDocSupport(SubDocDisplayType subDocType,
                                        bool isSubExpr, bool support)
{
    uint32_t idx = isSubExpr ? 1 : 0;
    _subDocValidateTable[subDocType][SORT_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][DISTINCT_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][DISTINCT_FILTER_SYNTAX_LOCATION][idx] = support;

    _subDocValidateTable[subDocType][FILTER_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][AGGKEY_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][AGGFUN_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][AGGFILTER_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][RANGE_AGGKEY_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][SEARCHER_CACHE_FILTER_SYNTAX_LOCATION][idx] = support;
    _subDocValidateTable[subDocType][RANK_SORT_SYNTAX_LOCATION][idx] = support;
}

bool RequestValidator::initSubDocValidateTable() {
    memset(_subDocValidateTable, 0, sizeof(_subDocValidateTable));
    setSubDocSupport(SUB_DOC_DISPLAY_NO, false, true);
    setSubDocSupport(SUB_DOC_DISPLAY_NO, true, false);
    setSubDocSupport(SUB_DOC_DISPLAY_FLAT, false, true);
    setSubDocSupport(SUB_DOC_DISPLAY_FLAT, true, true);

    setSubDocSupport(SUB_DOC_DISPLAY_GROUP, false, true);
    setSubDocSupport(SUB_DOC_DISPLAY_GROUP, true, false);
    _subDocValidateTable[SUB_DOC_DISPLAY_GROUP][FILTER_SYNTAX_LOCATION][1] = true;
    _subDocValidateTable[SUB_DOC_DISPLAY_GROUP][AGGKEY_SYNTAX_LOCATION][1] = true;
    _subDocValidateTable[SUB_DOC_DISPLAY_GROUP][RANGE_AGGKEY_SYNTAX_LOCATION][1] = true;
    _subDocValidateTable[SUB_DOC_DISPLAY_GROUP][AGGFILTER_SYNTAX_LOCATION][1] = true;
    _subDocValidateTable[SUB_DOC_DISPLAY_GROUP][AGGFUN_SYNTAX_LOCATION][1] = true;

    return true;
}

void RequestValidator::setNumericTypeSupport(SyntaxExprLocation location,
        bool isMulti, bool support)
{
    int multiPos = isMulti ? 1 : 0;
    _syntaxExprValidateTable[location][vt_int8][multiPos] = support;
    _syntaxExprValidateTable[location][vt_int16][multiPos] = support;
    _syntaxExprValidateTable[location][vt_int32][multiPos] = support;
    _syntaxExprValidateTable[location][vt_int64][multiPos] = support;

    _syntaxExprValidateTable[location][vt_uint8][multiPos] = support;
    _syntaxExprValidateTable[location][vt_uint16][multiPos] = support;
    _syntaxExprValidateTable[location][vt_uint32][multiPos] = support;
    _syntaxExprValidateTable[location][vt_uint64][multiPos] = support;

    _syntaxExprValidateTable[location][vt_float][multiPos] = support;
    _syntaxExprValidateTable[location][vt_double][multiPos] = support;
}

bool RequestValidator::initSyntaxExprValidateTable() {
    memset(_syntaxExprValidateTable, 0, sizeof(_syntaxExprValidateTable));
    //sort syntax:
    setNumericTypeSupport(SORT_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[SORT_SYNTAX_LOCATION][vt_string][0] = true;

    //distinct syntax:
    setNumericTypeSupport(DISTINCT_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_float][0] = false;
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_double][0] = false;
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_string][0] = true;
    setNumericTypeSupport(DISTINCT_SYNTAX_LOCATION, true);
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_float][1] = false;
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_double][1] = false;
    _syntaxExprValidateTable[DISTINCT_SYNTAX_LOCATION][vt_string][1] = true;

    //filter syntax:
    _syntaxExprValidateTable[FILTER_SYNTAX_LOCATION][vt_bool][0] = true;
    _syntaxExprValidateTable[AGGFILTER_SYNTAX_LOCATION][vt_bool][0] = true;
    _syntaxExprValidateTable[DISTINCT_FILTER_SYNTAX_LOCATION][vt_bool][0] = true;
    _syntaxExprValidateTable[SEARCHER_CACHE_FILTER_SYNTAX_LOCATION][vt_bool][0] = true;

    //aggreate key syntax:
    setNumericTypeSupport(AGGKEY_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[AGGKEY_SYNTAX_LOCATION][vt_string][0] = true;

    setNumericTypeSupport(AGGKEY_SYNTAX_LOCATION, true);
    _syntaxExprValidateTable[AGGKEY_SYNTAX_LOCATION][vt_string][1] = true;

    //aggreate fun syntax
    setNumericTypeSupport(AGGFUN_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[AGGFUN_SYNTAX_LOCATION][vt_string][0] = true;

    setNumericTypeSupport(AGGFUN_SYNTAX_LOCATION, true);
    _syntaxExprValidateTable[AGGFUN_SYNTAX_LOCATION][vt_string][1] = true;


    //range aggreate key syntax:
    setNumericTypeSupport(RANGE_AGGKEY_SYNTAX_LOCATION, false);
    setNumericTypeSupport(RANGE_AGGKEY_SYNTAX_LOCATION, true);

    //searcher cache expire time expression
    setNumericTypeSupport(SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION, false, false);
    _syntaxExprValidateTable[SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION][vt_uint32][0] = true;

    //rank sort syntax:
    setNumericTypeSupport(RANK_SORT_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[RANK_SORT_SYNTAX_LOCATION][vt_string][0] = true;

    //aux filter syntax:
    setNumericTypeSupport(AUX_FILTER_SYNTAX_LOCATION, false);
    _syntaxExprValidateTable[AUX_FILTER_SYNTAX_LOCATION][vt_bool][0] = true;

    return true;
}

bool RequestValidator::isRangeKeyWord(const std::string &keyName) {
    if (keyName == LAYERKEY_DOCID
        || keyName == LAYERKEY_OTHER
        || keyName == LAYERKEY_SEGMENTID)
    {
        return true;
    }
    return false;
}

END_HA3_NAMESPACE(qrs);
