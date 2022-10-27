#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/queryparser/ClauseScanner.h>
#include <ha3/queryparser/DistinctParser.h>
#include <ha3/queryparser/AggregateParser.h>
#include <ha3/queryparser/SyntaxExprParser.h>
#include <typeinfo>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, ClauseParserContext);

const char* ClauseParserContext::STATUS_MSGS[] = {
    "OK.",
    "Initial status.",
    "Bad index name.",
    "Bad fieldbit name.",
    "Evaluate failed.",
};

ClauseParserContext::~ClauseParserContext() {
    if (_distDes) {
        delete _distDes;
        _distDes = NULL;
    }
    if (_aggDes) {
        delete _aggDes;
        _aggDes = NULL;
    }
    if (_virtualAttributeClause) {
        delete _virtualAttributeClause;
        _virtualAttributeClause = NULL;
    }
    if (_queryLayerClause) {
        delete _queryLayerClause;
        _queryLayerClause = NULL;
    }
    if (_syntaxExpr) {
        delete _syntaxExpr;
        _syntaxExpr = NULL;
    }    
    if (_searcherCacheClause) {
        delete _searcherCacheClause;
        _searcherCacheClause = NULL;
    }    
    if (_rankSortClause) {
        delete _rankSortClause;
        _rankSortClause = NULL;
    }

}

void ClauseParserContext::doParse(const char *clauseStr) {
    istringstream iss(clauseStr);
    ClauseScanner scanner(&iss, &cout);
    isearch_bison::ClauseBisonParser parser(scanner, *this);
    if (parser.parse() != 0) {
        HA3_LOG(DEBUG, "%s:|%s|", getStatusMsg().c_str(), clauseStr);
    }
}

#define PARSE_AND_CHECK_RESULT(clauseStr, result, errorCode, errorString) \
    {                                                                   \
        doParse(clauseStr);                                             \
        if (result) {                                                   \
            result->setOriginalString(clauseStr);                       \
        }                                                               \
        if (( _status != ClauseParserContext::OK                         \
             && !_errorResult.hasError()) || !result)                   \
        {                                                               \
            _errorResult.resetError(errorCode,                          \
                    string(errorString));                               \
        }                                                               \
        return !_errorResult.hasError();                                \
    }

bool ClauseParserContext::parseAggClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _aggDes, 
                           ERROR_AGGREGATE_CLAUSE,
                           string("parse aggregate clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseDistClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _distDes, 
                           ERROR_DISTINCT_CLAUSE,
                           string("parse distinct clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseVirtualAttributeClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _virtualAttributeClause, 
                           ERROR_VIRTUALATTRIBUTE_CLAUSE,
                           string("parse virtualAttribute clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseLayerClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _queryLayerClause,
                           ERROR_LAYER_CLAUSE, // TODO
                           string("parse layer clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseSearcherCacheClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _searcherCacheClause,
                           ERROR_CACHE_CLAUSE,
                           string("parse searcher cache clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseRankSortClause(const char *clauseStr) {
    PARSE_AND_CHECK_RESULT(clauseStr, _rankSortClause,
                           ERROR_RANK_SORT_CLAUSE,
                           string("parse rank sort clause failed:") + 
                           getStatusMsg());
}

bool ClauseParserContext::parseSyntaxExpr(const char *clauseStr) {
    doParse(clauseStr);
    if ( (_status != ClauseParserContext::OK 
         && !_errorResult.hasError() ) || !_syntaxExpr) 
    {
        _errorResult.resetError(ERROR_PARSE_SYNTAX,
                                string("parse syntax expression clause failed:")
                                + getStatusMsg());
    }
    return !_errorResult.hasError();
}

string ClauseParserContext::getNormalizedExprStr(const string &exprStr) {
    if (!parseSyntaxExpr(exprStr.c_str())) {
        return "";
    }
    SyntaxExpr *syntaxExpr = stealSyntaxExpr();
    unique_ptr<SyntaxExpr> syntaxExpr_ptr(syntaxExpr);
    if (syntaxExpr) {
        return syntaxExpr->getExprString();
    }
    return "";
}

const std::string ClauseParserContext::getStatusMsg() const {
    if (_status < ClauseParserContext::SYNTAX_ERROR) {
        return STATUS_MSGS[_status];
    }
    return _statusMsg;
}

#define SET_STEAL_MEMBER_HELPER(action, type, member)                   \
    void ClauseParserContext::set##action(type *var) {                  \
        if (member) {                                                   \
            delete member;                                              \
        }                                                               \
        member = var;                                                   \
    }                                                                   \
                                                                        \
    type* ClauseParserContext::steal##action() {                        \
        type *tmp = member;                                             \
        member = NULL;                                                  \
        return tmp;                                                     \
    }
SET_STEAL_MEMBER_HELPER(DistDescription, DistinctDescription, _distDes);
SET_STEAL_MEMBER_HELPER(VirtualAttributeClause, VirtualAttributeClause, _virtualAttributeClause);
SET_STEAL_MEMBER_HELPER(AggDescription, AggregateDescription, _aggDes);
SET_STEAL_MEMBER_HELPER(SyntaxExpr, SyntaxExpr, _syntaxExpr);
SET_STEAL_MEMBER_HELPER(LayerClause, QueryLayerClause, _queryLayerClause);
SET_STEAL_MEMBER_HELPER(SearcherCacheClause, SearcherCacheClause, 
                        _searcherCacheClause);
SET_STEAL_MEMBER_HELPER(RankSortClause, RankSortClause, 
                        _rankSortClause);

#undef SET_STEAL_MEMBER_HELPER

END_HA3_NAMESPACE(queryparser);
