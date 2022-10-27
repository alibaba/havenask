#ifndef ISEARCH_QUERYTOKENIZER_H
#define ISEARCH_QUERYTOKENIZER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/Query.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/Token.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <assert.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/ConfigClause.h>

BEGIN_HA3_NAMESPACE(qrs);

class QueryTokenizer : public common::QueryVisitor
{
public:
    typedef std::vector<build_service::analyzer::Token> TokenVect;

public:
    QueryTokenizer(const build_service::analyzer::AnalyzerFactory *analyzerFactory);
    QueryTokenizer(const QueryTokenizer &tokenzier);
    virtual ~QueryTokenizer();

public:
    void setConfigClause(common::ConfigClause *configClause) {
        setNoTokenizeIndexes(configClause->getNoTokenizeIndexes());
        setGlobalAnalyzerName(configClause->getAnalyzerName());
        setIndexAnalyzerNameMap(configClause->getIndexAnalyzerMap());
    }
    void setNoTokenizeIndexes(const std::set<std::string>& indexes) {
        _noTokenizeIndexes = indexes;
    }
    void setGlobalAnalyzerName(const std::string &analyzerName) {
        _globalAnalyzerName = analyzerName;
    }
    void addIndexAnalyzerName(const std::string& indexName,
                              const std::string& analyzerName) {
        _indexAnalyzerMap[indexName] = analyzerName;
    }
    void setIndexAnalyzerNameMap(const std::map<std::string, std::string> &indexMap) {
        _indexAnalyzerMap = indexMap;
    }
    std::string getIndexAnalyzerName(const std::string& indexName) const {
        auto it = _indexAnalyzerMap.find(indexName);
        if (it != _indexAnalyzerMap.end()) {
            return it->second;
        }
        return _globalAnalyzerName;
    }

    bool tokenizeQuery(const common::Query *query,
                       const suez::turing::IndexInfos *indexInfos,
                       QueryOperator defaultOP = OP_AND)
    {
        _errorResult.resetError(ERROR_NONE);
        _indexInfos = indexInfos;
        _defaultOP = defaultOP;
        DELETE_AND_SET_NULL(_visitedQuery);
        assert(query);
        query->accept(this);
        return !_errorResult.hasError();
    }
public:
    virtual void visitTermQuery(const common::TermQuery *query);
    virtual void visitPhraseQuery(const common::PhraseQuery *query);
    virtual void visitAndQuery(const common::AndQuery *query);
    virtual void visitOrQuery(const common::OrQuery *query);
    virtual void visitAndNotQuery(const common::AndNotQuery *query);
    virtual void visitRankQuery(const common::RankQuery *query);
    virtual void visitNumberQuery(const common::NumberQuery *query);
    virtual void visitMultiTermQuery(const common::MultiTermQuery *query);
    common::Query* stealQuery();
    const common::ErrorResult getErrorResult() const { return _errorResult; }

private:

    enum TokenType {
        TERM_TOKEN = 1,
        PHRASE_TOKEN = 2,
        MULTI_TOKEN = 3
    };

    void tokenize(const std::string &indexName,
                  const common::Term &term,
                  TokenVect &tokens);
    bool needTokenize(const std::string &indexName);
    void doTokenize(const std::string &indexName,
                  const std::string &word,
                  TokenVect &tokens);
    void reConstructBinaryQuery(const common::MultiTermQuery *query);
    void visitBinaryQuery(common::Query *resultQuery,
                          const common::Query *srcQuery);

    common::Query* createTermQuery(const build_service::analyzer::Token &token,
                                   const common::Term &term,
                                   const std::string &label);
    common::Query* createPhraseQuery(const TokenVect &tokens,
            const common::Term &term, const std::string &label);
    common::Query* createDefaultQuery(const TokenVect &tokens,
            const common::Term &term, const std::string &label);
    common::Query *createTerminalQuery(
            const common::MultiTermQuery::TermArray &resultTermArray,
            QueryOperator op, const std::string &label);
    void addMultiTerms(common::MultiTermQuery::TermArray &termArray,
                       const TokenVect &tokens, const common::Term &term);
    bool isIndexNoTokenize(const std::string &indexName) const {
        return _noTokenizeIndexes.find(indexName) != _noTokenizeIndexes.end();
    }

private:
    const suez::turing::IndexInfos *_indexInfos;
    QueryOperator _defaultOP;
    const build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    common::Query *_visitedQuery;
    common::ErrorResult _errorResult;
    std::string _globalAnalyzerName;
    std::set<std::string> _noTokenizeIndexes;
    std::map<std::string, std::string> _indexAnalyzerMap;
private:
    friend class QueryTokenizerTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryTokenizer);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QUERYTOKENIZER_H
