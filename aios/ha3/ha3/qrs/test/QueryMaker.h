#ifndef ISEARCH_QUERYMAKER_H
#define ISEARCH_QUERYMAKER_H

#include <set>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <ha3/common/TermQuery.h>
#include <autil/StringTokenizer.h>

BEGIN_HA3_NAMESPACE(qrs);

class QueryMaker
{
public:
    QueryMaker();
    ~QueryMaker();
public:
    template <typename QueryType>
    static QueryType* createBinaryQuery(const char *words, std::set<std::string>* stopWords = NULL);

    template <typename QueryType>
    static QueryType* packageBinaryQuery(common::QueryPtr theFirstChildQueryPtr, 
            const char *word);

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryMaker);

template <typename QueryType>
inline QueryType* QueryMaker::createBinaryQuery(const char *words,  std::set<std::string>* stopWords) {
    QueryType *resultQuery = new QueryType("");

    autil::StringTokenizer st(words, ",", 
                             autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        std::string termStr = *it;
        common::RequiredFields requiredFields;
        common::TermQuery* termQuery = new common::TermQuery(
                termStr.c_str(), "phrase", requiredFields, "");
        common::QueryPtr queryPtr(termQuery);
        bool isStopWord = stopWords != NULL 
                          && stopWords->find(termStr) != stopWords->end();
        if (isStopWord) {
            termQuery->getTerm().getToken().setIsStopWord(true);
        }
        resultQuery->addQuery(queryPtr);
    }    
    return resultQuery;
}

template <typename QueryType>
inline QueryType* QueryMaker::packageBinaryQuery(common::QueryPtr theFirstChildQueryPtr, 
        const char *words) 
{
    QueryType *resultQuery = new QueryType("");
    resultQuery->addQuery(theFirstChildQueryPtr);

    autil::StringTokenizer st(words, ",", 
                             autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        std::string termStr = *it;
        common::RequiredFields requiredFields;
        common::QueryPtr termQueryPtr(new common::TermQuery(
                        termStr.c_str(), "phrase", requiredFields, ""));
        resultQuery->addQuery(termQueryPtr);
    }    
    return resultQuery;
}

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYMAKER_H
