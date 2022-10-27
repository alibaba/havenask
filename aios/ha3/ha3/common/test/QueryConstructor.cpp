#include <ha3/common/test/QueryConstructor.h>
#include <ha3/common/TermQuery.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, QueryConstructor);

QueryConstructor::QueryConstructor() { 
}

QueryConstructor::~QueryConstructor() { 
}

void QueryConstructor::prepare(Query *query, const char *indexName,
                               const char *str1, const char *str2, const char *str3)
{
    RequiredFields requiredFields;
    if (str1 != NULL) {
        Term term1(str1, indexName, requiredFields);
        QueryPtr termQueryPtr1(new TermQuery(term1, ""));
        query->addQuery(termQueryPtr1);
    }

    if (str2 != NULL) {
        Term term2(str2, indexName, requiredFields);
        QueryPtr termQueryPtr2(new TermQuery(term2, ""));
        query->addQuery(termQueryPtr2);
    }

    if (str3 != NULL){
        Term term3(str3, indexName, requiredFields);
        QueryPtr termQueryPtr3(new TermQuery(term3, ""));
        query->addQuery(termQueryPtr3);
    }
}

END_HA3_NAMESPACE(common);

