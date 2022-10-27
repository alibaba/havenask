#ifndef ISEARCH_CLAUSEPARSERCONTEXT_H_
#define ISEARCH_CLAUSEPARSERCONTEXT_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <assert.h>
#include <ha3/common/DistinctClause.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/common/AggregateDescription.h>
#include <ha3/queryparser/DistinctParser.h>
#include <ha3/queryparser/VirtualAttributeParser.h>
#include <ha3/queryparser/AggregateParser.h>
#include <ha3/queryparser/SyntaxExprParser.h>
#include <ha3/queryparser/LayerParser.h>
#include <ha3/queryparser/SearcherCacheParser.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/common/SearcherCacheClause.h>
#include <ha3/common/RankSortClause.h>


BEGIN_HA3_NAMESPACE(queryparser);

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
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif
