#include <ha3/queryparser/ParserContext.h>

using namespace std;

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, ParserContext);

const char* ParserContext::STATUS_MSGS[] = {
    "OK.",
    "Initial status.",
    "Bad index name.",
    "Bad fieldbit name.",
    "Evaluate failed.",
};

ParserContext::ParserContext(QueryParser &queryParser) 
  : _queryParser(queryParser) 
{
    _status = ParserContext::INITIAL_STATUS;
}

ParserContext::~ParserContext() {
    for (size_t i = 0; i < _rootQuerys.size(); ++i) {
        DELETE_AND_SET_NULL(_rootQuerys[i]);
    }
    _rootQuerys.clear();
    for (size_t i = 0; i < _rootQueryExprs.size(); ++i) {
        DELETE_AND_SET_NULL(_rootQueryExprs[i]);
    }
    _rootQueryExprs.clear();
}

const std::string ParserContext::getStatusMsg() const {
    if (_status < ParserContext::SYNTAX_ERROR) {
        return STATUS_MSGS[_status];
    }
    return _statusMsg;
}

void ParserContext::addQuery(common::Query *query) {
    _rootQuerys.push_back(query);
}

vector<common::Query*> ParserContext::stealQuerys() {
    std::vector<common::Query*> rootQuerys;
    rootQuerys.swap(_rootQuerys);
    return rootQuerys;
}

void ParserContext::addQueryExpr(QueryExpr *queryExpr) {
    _rootQueryExprs.push_back(queryExpr);
}

vector<QueryExpr*> ParserContext::stealQueryExprs() {
    vector<QueryExpr*> rootQueryExprs;
    rootQueryExprs.swap(_rootQueryExprs);
    return rootQueryExprs;
}

END_HA3_NAMESPACE(queryparser);
