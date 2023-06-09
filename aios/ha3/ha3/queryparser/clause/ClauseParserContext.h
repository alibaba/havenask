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

#include <stddef.h>
#include <string>

#include "ha3/common/AggFunDescription.h" // IWYU pragma: keep
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h" // IWYU pragma: keep
#include "ha3/common/LayerDescription.h" // IWYU pragma: keep
#include "ha3/common/QueryLayerClause.h" // IWYU pragma: keep
#include "ha3/common/RankSortClause.h" // IWYU pragma: keep
#include "ha3/common/RankSortDescription.h" // IWYU pragma: keep
#include "ha3/common/SearcherCacheClause.h" // IWYU pragma: keep
#include "ha3/common/SortDescription.h" // IWYU pragma: keep
#include "ha3/common/VirtualAttributeClause.h" // IWYU pragma: keep
#include "ha3/queryparser/AggregateParser.h"
#include "ha3/queryparser/DistinctParser.h"
#include "ha3/queryparser/LayerParser.h"
#include "ha3/queryparser/SearcherCacheParser.h"
#include "ha3/queryparser/SyntaxExprParser.h"
#include "ha3/queryparser/VirtualAttributeParser.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez


namespace isearch {
namespace queryparser {

class ClauseParserContext
{
public:
    enum Status {
        OK = 0,
        INITIAL_STATUS,
        BAD_INDEXNAME,
        BAD_FIELDBIT,
        EVALUATE_FAILED,
        SYNTAX_ERROR,
    };
private:
    static const char *STATUS_MSGS[];
public:
    ClauseParserContext()
        :
        _distParser(&_errorResult)
        , _virtualAttributeParser(&_errorResult)
        , _searcherCacheParser(&_errorResult)
        , _distDes(NULL)
        , _aggDes(NULL)
        , _virtualAttributeClause(NULL)
        , _queryLayerClause(NULL)
        , _searcherCacheClause(NULL)
        , _rankSortClause(NULL)
        , _syntaxExpr(NULL)
    {
    }
    ~ClauseParserContext();
public:
    bool parseDistClause(const char *clauseStr);
    bool parseAggClause(const char *clauseStr);
    bool parseVirtualAttributeClause(const char *clauseStr);
    bool parseSyntaxExpr(const char *clauseStr);
    bool parseLayerClause(const char *clauseStr);
    bool parseSearcherCacheClause(const char *clauseStr);
    bool parseRankSortClause(const char *clauseStr);
public:
    std::string getNormalizedExprStr(const std::string &exprStr);
public:
    Status getStatus() const {return _status;}
    void setStatus(Status status) {_status = status;}

    const std::string getStatusMsg() const;
    void setStatusMsg(const std::string &statusMsg) {
        _statusMsg = statusMsg;
    }

    void setDistDescription(common::DistinctDescription *distDescription);
    common::DistinctDescription* stealDistDescription();

    void setVirtualAttributeClause(common::VirtualAttributeClause *virtualAttributeClause);
    common::VirtualAttributeClause* stealVirtualAttributeClause();

    void setAggDescription(common::AggregateDescription *aggDescription);
    common::AggregateDescription* stealAggDescription();

    void setSyntaxExpr(suez::turing::SyntaxExpr *syntaxExpr);
    suez::turing::SyntaxExpr* stealSyntaxExpr();

    void setLayerClause(common::QueryLayerClause *queryLayerClause);
    common::QueryLayerClause* stealLayerClause();

    void setSearcherCacheClause(common::SearcherCacheClause* searcherCacheClause);
    common::SearcherCacheClause* stealSearcherCacheClause();

    void setRankSortClause(common::RankSortClause* rankSortClause);
    common::RankSortClause* stealRankSortClause();

    const common::ErrorResult &getErrorResult() const {
        return _errorResult;
    }

    DistinctParser& getDistinctParser() { return _distParser; }
    VirtualAttributeParser& getVirtualAttributeParser() { return _virtualAttributeParser; }
    AggregateParser& getAggregateParser() { return _aggParser; }
    suez::turing::SyntaxExprParser& getSyntaxExprParser() { return _syntaxParser; }
    LayerParser& getLayerParser() { return _layerParser; }
    SearcherCacheParser& getSearcherCacheParser() { return _searcherCacheParser; }
private:
    void doParse(const char *clauseStr);
private:
    common::ErrorResult _errorResult;
private:
    DistinctParser _distParser;
    VirtualAttributeParser _virtualAttributeParser;
    AggregateParser _aggParser;
    LayerParser _layerParser;
    SearcherCacheParser _searcherCacheParser;
    suez::turing::SyntaxExprParser _syntaxParser;
    common::DistinctDescription *_distDes;
    common::AggregateDescription *_aggDes;
    common::VirtualAttributeClause *_virtualAttributeClause;
    common::QueryLayerClause *_queryLayerClause;
    common::SearcherCacheClause *_searcherCacheClause;
    common::RankSortClause *_rankSortClause;
    suez::turing::SyntaxExpr *_syntaxExpr;
    Status _status;
    std::string _statusMsg;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
