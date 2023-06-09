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
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace common {
class AggFunDescription;
class AggregateClause;
class AttributeClause;
class ConfigClause;
class DistinctClause;
class FetchSummaryClause;
class FinalSortClause;
class HealthCheckClause;
class OptimizerClause;
class PKFilterClause;
class QueryLayerClause;
class RankSortClause;
class SearcherCacheClause;
class SortClause;
class VirtualAttributeClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeInfos;
class IndexInfos;
class SyntaxExprValidator;
class VirtualAttribute;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace qrs {

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
    bool validate(const common::RequestPtr& requestPtr,
                  const std::vector<std::string> &clusterNames);
    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }
    static bool initSyntaxExprValidateTable();
    static bool initSubDocValidateTable();
    void setTracer(common::Tracer *tracer) {
        _tracer = tracer;
    }
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
    bool validateOptimizerClause(const common::OptimizerClause *optimizerClause);
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
    bool validateBasciAuxQueryInfo(
            const common::AuxQueryClause *auxQueryClause,
            const common::AuxFilterClause *auxFilterClause,
            const std::vector<std::string> &clusterNames,
            const std::vector<suez::turing::VirtualAttribute *> &virtualAttributes);
    void reset();
    void addFunctionInfo(const std::vector<std::string> &clusterNames,
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
    common::Tracer *_tracer;
    static bool _syntaxExprValidateTable[SYTAX_EXPR_LOCATION_COUNT][vt_type_count][2];
    static bool _subDocValidateTable[3][SYTAX_EXPR_LOCATION_COUNT][2];
private:
    AUTIL_LOG_DECLARE();
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
            AUTIL_LOG(WARN, "%s", haErrorCodeToString(_errorCode).c_str());
            return false;
        }
    }
    return true;
}

typedef std::shared_ptr<RequestValidator> RequestValidatorPtr;

} // namespace qrs
} // namespace isearch

