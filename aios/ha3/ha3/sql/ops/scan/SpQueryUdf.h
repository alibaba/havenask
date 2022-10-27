#ifndef ISEARCH_SPQUERYUDF_H
#define ISEARCH_SPQUERYUDF_H

#include <ha3/queryparser/ParserContext.h>
#include <ha3/queryparser/QueryParser.h>

BEGIN_HA3_NAMESPACE(sql);

class SPQueryUdf {
public:
    explicit SPQueryUdf(const char *defaultIndex);
    ~SPQueryUdf();
public:
    queryparser::ParserContext *parse(const char *queryText);
private:
    queryparser::QueryParser _queryParser;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SPQUERYUDF_H