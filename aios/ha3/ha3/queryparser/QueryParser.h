#ifndef ISEARCH_QUERYPARSER_H
#define ISEARCH_QUERYPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <string>
#include <vector>
#include <ha3/queryparser/ParserContext.h>
#include <ha3/queryparser/QueryExpr.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/IndexIdentifier.h>
#include <ha3/config/QueryInfo.h>

BEGIN_HA3_NAMESPACE(queryparser);

class QueryParser
{
public:
    QueryParser(const char *defaultIndex,
                QueryOperator defaultOP = OP_AND,
                bool flag = false);
    ~QueryParser();
public:
    typedef std::vector<std::string*> TextVec;

    ParserContext* parse(const char *queryText);
    ParserContext* parseExpr(const char *queryText);

    QueryExpr* createDefaultOpExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createAndExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createOrExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createAndNotExpr(QueryExpr *exprA, QueryExpr *exprB);
    QueryExpr* createRankExpr(QueryExpr *exprA, QueryExpr *exprB);
    AtomicQueryExpr *createWordsTermExpr(std::string *text);
    AtomicQueryExpr *createWordsWithIndexTermExpr(char* indexName, char* text);
    AtomicQueryExpr *createPhraseTermExpr(std::string *text);
    AtomicQueryExpr *createNumberTermExpr(std::string *leftNumber, bool leftInclusive,
            std::string *rightNumber, bool rightInclusive, ParserContext *ctx);
    AtomicQueryExpr* createOrTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB);
    AtomicQueryExpr* createAndTermExpr(AtomicQueryExpr *exprA, AtomicQueryExpr *exprB);
    AtomicQueryExpr* createMultiTermQueryExpr(AtomicQueryExpr *exprA,
            AtomicQueryExpr *exprB, QueryOperator opExpr);
    void setDefaultOP(QueryOperator defaultOP) {_defaultOP = defaultOP;}
    void evaluateQuery(ParserContext* ctx);

    std::string getDefaultIndex() const { return _defaultIndex;}
    void setCurrentIndex(const std::string &currentIndex) { _currentIndex = currentIndex; }
    std::string getCurrentIndex() const { return _currentIndex;}
private:
    bool isTerminalTermExpr(AtomicQueryExpr *expr);
    bool isMultiTermExpr(AtomicQueryExpr *expr);
    bool isSameOP(AtomicQueryExpr *expr, QueryOperator opExpr);

private:
    std::string _defaultIndex;
    std::string _currentIndex;
    QueryOperator _defaultOP;
    bool _enableMultiTermQuery;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_QUERYPARSER_H
