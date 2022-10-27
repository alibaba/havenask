#ifndef ISEARCH_PARSERCONTEXT_H_
#define ISEARCH_PARSERCONTEXT_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <assert.h>
#include <ha3/queryparser/QueryExpr.h>
BEGIN_HA3_NAMESPACE(queryparser);

class QueryParser;
class ParserContext
{
public:
    enum Status {
        OK = 0,
        INITIAL_STATUS,
        EVALUATE_FAILED,
        SYNTAX_ERROR,
    };
private:
    static const char *STATUS_MSGS[];
public:
    ParserContext(QueryParser &queryParser);
    ~ParserContext();
public:
    Status getStatus() const {return _status;}
    void setStatus(Status status) {_status = status;}

    const std::string getStatusMsg() const;
    void setStatusMsg(const std::string &statusMsg) {
        _statusMsg = statusMsg;
    }

    void addQuery(common::Query *query);
    const std::vector<common::Query*> &getQuerys() {return _rootQuerys;}
    std::vector<common::Query*> stealQuerys();

    void addQueryExpr(QueryExpr *queryExpr);
    std::vector<QueryExpr*> stealQueryExprs();
    const std::vector<QueryExpr*> &getQueryExprs() {return _rootQueryExprs;}

    QueryParser& getParser() {return _queryParser;}
private:
    QueryParser &_queryParser;
    std::vector<common::Query*> _rootQuerys;
    std::vector<QueryExpr*> _rootQueryExprs;
    Status _status;
    std::string _statusMsg;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif
