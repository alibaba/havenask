#ifndef ISEARCH_QUERYCONSTRUCTOR_H
#define ISEARCH_QUERYCONSTRUCTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class QueryConstructor
{
public:
    QueryConstructor();
    ~QueryConstructor();
public:
    static void prepare(Query *query, const char *indexName,
                 const char *str1 = NULL, const char *str2 = NULL, const char *str3 = NULL);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYCONSTRUCTOR_H
