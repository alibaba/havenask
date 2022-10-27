#ifndef ISEARCH_REQUESTVALIDATOR_H
#define ISEARCH_REQUESTVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/Request.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <suez/turing/expression/util/AttributeInfos.h>
#include <autil/StringUtil.h>
#include <ha3/config/QrsConfig.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/syntax/SyntaxExprValidator.h>
#include <suez/turing/expression/plugin/SorterManager.h>

BEGIN_HA3_NAMESPACE(qrs);

enum SyntaxExprLocation {
    SORT_SYNTAX_LOCATION,
    DISTINCT_SYNTAX_LOCATION,
    DISTINCT_FILTER_SYNTAX_LOCATION,
    FILTER_SYNTAX_LOCATION,
    AGGKEY_SYNTAX_LOCATION,
    AGGFUN_SYNTAX_LOCATION,
    AGGFILTER_SYNTAX_LOCATION,
    RANGE_AGGKEY_SYNTAX_LOCATION,
    SEARCHER_CACHE_EXPIRE_TIME_SYNTAX_LOCATION,
    SEARCHER_CACHE_FILTER_SYNTAX_LOCATION,
    RANK_SORT_SYNTAX_LOCATION,
    AUX_FILTER_SYNTAX_LOCATION,
    SYTAX_EXPR_LOCATION_COUNT,
};

class RequestValidator
{
public:
    RequestValidator(const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
                     uint32_t retHitsLimit,
                     const config::ClusterConfigMapPtr &clusterConfigMapPtr,
                     const suez::turing::ClusterFuncMapPtr &clusterFuncMapPtr,
                     const suez::turing::CavaPluginManagerMapPtr &clusterCavaPluginManagerMapPtr,
                     const suez::turing::ClusterSorterNamesPtr &clusterSorterNamesPtr);

    ~RequestValidator();
public:
    bool validate(const common::RequestPtr &request);

    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }
    static bool initSyntaxExprValidateTable();
    static bool initSubDocValidateTable();
private:
    bool validateConfigClause(const common::ConfigClause *configClause);
    bool validateQueryClause(const common::QueryClause *queryClause);
    bool validateFilterClause(const common::FilterClause *filterClause,
                              SyntaxExprLocation location = FILTER_SYNTAX_LOCATION);
    bool validatePKFilterClause(const common::PKFilterClause *PKFilterClause);
    bool validateSortClause(const common::SortClause *sortClause);
    bool validateFinalSortClause(const common::FinalSortClause *finalSortClause,
                                 const common::ConfigClause *configClause);
    bool validateRankSortClause(const common::RankSortClause *rankSortClause);
    bool validateDistinctClause(const common::DistinctClause *distinctClaus);
    bool validateAggregateClause(const common::AggregateClause *aggregateClaus);
    bool validateHealthCheckClause(const common::HealthCheckClause *healthCheckClause);
    bool validateQueryLayerClause(common::QueryLayerClause *layerClause);

    bool validateAttributeClause(
            const common::AttributeClause *attributeClause,
            const std::vector<suez::turing::VirtualAttribute *> &virtualAttributes,
            SubDocDisplayType subDocDisplayType);
    bool validateVirtualAttributeClause(
            const common::VirtualAttributeClause *virtualAttributeClause,
            std::vector<suez::turing::VirtualAttribute *> &virtualAttributes);
    bool validateVirtualAttributeName(
            const std::vector<suez::turing::VirtualAttribute *> &virtualAttributes,
            const std::string &name);

    bool validateSearcherCacheClause(
            const common::SearcherCacheClause *searcherCacheClause);

    bool validateSyntaxExpr(const suez::turing::SyntaxExpr *syntaxExpr,
                            SyntaxExprLocation locatio);
    bool validateAuxSyntaxExpr(const suez::turing::SyntaxExpr *syntaxExpr,
            SyntaxExprLocation locatio);
    bool validateAggFunDescription(const common::AggFunDescription *aggFunDes,
                                   bool allowSubExpr);
    bool validateAggFunName(const std::string &aggFunName);
    bool validateAggregateRange(suez::turing::ExprResultType resultType,
                                const std::vector<std::string> &ranges);
    bool validateFetchSummaryClause(
            const common::FetchSummaryClause *fetchSummaryClause,
            const common::ConfigClause *configClause);
    template <typename T>
    bool validateAscendOrder(const std::vector<std::string> &ranges);
    bool validateAuxQueryClause(const common::AuxQueryClause *auxQueryClause);
    bool validateAuxFilterClause(
            const common::AuxFilterClause *auxFilterClause,
            SyntaxExprLocation location = AUX_FILTER_SYNTAX_LOCATION);

    void reset();
    void addFunctionInfo(std::vector<std::string> &clusterNames,
                         suez::turing::SyntaxExprValidator *syntaxExprValidator);

private:
    static void setNumericTypeSupport(SyntaxExprLocation location, bool isMulti,
            bool support = true);
    static void setSubDocSupport(SubDocDisplayType subDocType,
                                 bool isSubExpr, bool support = true);
    static bool isRangeKeyWord(const std::string &keyName);
    static std::string getBizName(const std::string &name);

private:
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    const suez::turing::IndexInfos *_indexInfos;
    const suez::turing::AttributeInfos *_attrInfos;
    const suez::turing::IndexInfos *_auxIndexInfos;
    const suez::turing::AttributeInfos *_auxAttrInfos;
    uint32_t _retHitsLimit;
    const config::ClusterConfigMapPtr _clusterConfigMapPtr;
    const suez::turing::ClusterFuncMapPtr _clusterFuncMapPtr;
    suez::turing::CavaPluginManagerMapPtr _clusterCavaPluginManagerMapPtr;
    const suez::turing::ClusterSorterNamesPtr _clusterSorterNamesPtr;

    ErrorCode _errorCode;
    std::string _errorMsg;
    SubDocDisplayType _subDocDisplayType;
    suez::turing::SyntaxExprValidator *_syntaxExprValidator;
    suez::turing::SyntaxExprValidator *_auxSyntaxExprValidator;
    static bool _syntaxExprValidateTable[SYTAX_EXPR_LOCATION_COUNT][vt_type_count][2];
    static bool _subDocValidateTable[3][SYTAX_EXPR_LOCATION_COUNT][2];
private:
    HA3_LOG_DECLARE();
    friend class RequestValidatorTest;
};

template <typename T>
bool RequestValidator::validateAscendOrder(const std::vector<std::string> &ranges) {
    std::vector<T> typeRanges;
    autil::StringUtil::fromString(ranges, typeRanges);

    assert(typeRanges.size() > 1);

    typename std::vector<T>::iterator it = typeRanges.begin();
    typename std::vector<T>::iterator it2 = it + 1;
    for ( ; it2 != typeRanges.end(); it++, it2++)
    {
        if ((*it2) <= (*it)) {
            _errorCode = ERROR_AGG_RANGE_ORDER;
            _errorMsg = "";
            HA3_LOG(WARN, "%s", haErrorCodeToString(_errorCode).c_str());
            return false;
        }
    }
    return true;
}

HA3_TYPEDEF_PTR(RequestValidator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_REQUESTVALIDATOR_H
