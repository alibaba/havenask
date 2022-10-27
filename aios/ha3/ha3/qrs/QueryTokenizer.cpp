#include <ha3/qrs/QueryTokenizer.h>
#include <build_service/analyzer/Analyzer.h>
#include <ha3/common/Term.h>

using namespace std;
using namespace suez::turing;
using namespace build_service::analyzer;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QueryTokenizer);

QueryTokenizer::QueryTokenizer(const AnalyzerFactory *analyzerFactory) {
    _indexInfos = NULL;
    _analyzerFactory = analyzerFactory;
    _visitedQuery = NULL;
    _defaultOP = OP_AND;
}

QueryTokenizer::QueryTokenizer(const QueryTokenizer &tokenzier) {
    _indexInfos = tokenzier._indexInfos;
    _analyzerFactory = tokenzier._analyzerFactory;
    _visitedQuery = NULL;
    _defaultOP = OP_AND;
    _noTokenizeIndexes = tokenzier._noTokenizeIndexes;
    _globalAnalyzerName = tokenzier._globalAnalyzerName;
    _indexAnalyzerMap = tokenzier._indexAnalyzerMap;
}

QueryTokenizer::~QueryTokenizer() {
    DELETE_AND_SET_NULL(_visitedQuery);
}

void QueryTokenizer::tokenize(const string &indexName,
                              const Term &term,
                              TokenVect &tokens)
{
    if (needTokenize(indexName)) {
        doTokenize(indexName, term.getWord(), tokens);
    } else {
        tokens.push_back(Token(term.getWord(), 0));
    }
}

bool QueryTokenizer::needTokenize(const string &indexName) {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (NULL == indexInfo) {
        _errorResult.resetError(ERROR_INDEX_NOT_EXIST,
                                string("Index name:") + indexName);
        return false;
    }

    IndexType indexType = indexInfo->getIndexType();
    return (it_text == indexType) || (it_pack == indexType)
        || (it_expack == indexType);
}

void QueryTokenizer::doTokenize(const string &indexName,
                              const string &word,
                              TokenVect &tokens)
{
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    string analyzerName = getIndexAnalyzerName(indexName);
    Analyzer* analyzer = NULL;
    if (analyzerName.empty()) {
        analyzerName = indexInfo->getAnalyzerName();
    }
    bool needIdleTokenize = isIndexNoTokenize(indexName);
    analyzer = _analyzerFactory->createAnalyzer(analyzerName, needIdleTokenize);
    if (!analyzer){
        _errorResult.resetError(ERROR_ANALYZER_NOT_EXIST,
                                string("Analyzer name: ") + analyzerName);
        return;
    }

    analyzer->tokenize(word.data(), word.size());
    Token token;
    while (analyzer->next(token)) {
        if (!token.isSpace()
            && !token.isDelimiter()
            && token.isRetrieve())
        {
            tokens.push_back(token);
        }
    }
    DELETE_AND_SET_NULL(analyzer);
}

void QueryTokenizer::visitTermQuery(const TermQuery *query) {
    const Term &term = query->getTerm();
    const string indexName = term.getIndexName();
    TokenVect tokens;
    tokenize(indexName, term, tokens);
    uint32_t tokenNum = tokens.size();
    if (tokenNum == 1) {
        _visitedQuery = createTermQuery(tokens[0], term, query->getQueryLabel());
    } else if (tokenNum > 1) {
        _visitedQuery = createDefaultQuery(tokens, term, query->getQueryLabel());
    }
}

void QueryTokenizer::visitPhraseQuery(const PhraseQuery *query) {
    const PhraseQuery::TermArray &termArray = query->getTermArray();
    assert(termArray.size() == 1);
    TermPtr term = termArray[0];
    const string indexName = term->getIndexName();
    TokenVect tokens;
    tokenize(indexName, *term, tokens);
    uint32_t tokenNum = tokens.size();
    if (tokenNum == 1) {
        _visitedQuery = createTermQuery(tokens[0], *term, query->getQueryLabel());
    } else if (tokenNum > 1) {
        _visitedQuery = createPhraseQuery(tokens, *term, query->getQueryLabel());
    }
}

void QueryTokenizer::visitMultiTermQuery(const MultiTermQuery *query) {
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    assert(termArray.size());

    const string indexName = termArray[0]->getIndexName();
    if (query->getOpExpr() != OP_WEAKAND && needTokenize(indexName)) {
        QueryOperator op = query->getOpExpr();
        if (op != _defaultOP) {
            reConstructBinaryQuery(query);
            return;
        }

        MultiTermQuery::TermArray resultTermArray;
        for (size_t i = 0; i < termArray.size(); ++i) {
            TokenVect tokens;
            doTokenize(indexName, termArray[i]->getWord().c_str(), tokens);
            addMultiTerms(resultTermArray, tokens, *termArray[i]);
        }
        _visitedQuery = createTerminalQuery(resultTermArray, op,
                query->getQueryLabel());
    } else {
        _visitedQuery = new MultiTermQuery(*query);
    }
}

void QueryTokenizer::reConstructBinaryQuery(const MultiTermQuery *query) {
    const MultiTermQuery::TermArray &termArray = query->getTermArray();
    const string indexName = termArray[0]->getIndexName();
    QueryOperator op = query->getOpExpr();
    Query *resultQuery = NULL;
    if (op == OP_AND) {
        resultQuery =  new AndQuery(query->getQueryLabel());
    } else if (op == OP_OR) {
        resultQuery = new OrQuery(query->getQueryLabel());
    } else {
        assert(false);
    }

    MultiTermQuery::TermArray resultTermArray;
    for (size_t i = 0; i < termArray.size(); ++i) {
        TokenVect tokens;
        doTokenize(indexName, termArray[i]->getWord().c_str(), tokens);
        size_t tokenSize = tokens.size();
        if (tokenSize == 1) {
            addMultiTerms(resultTermArray, tokens, *termArray[i]);
        } else if (tokenSize > 1) {
            Query *oneQuery = createDefaultQuery(tokens, *termArray[i], "");
            resultQuery->addQuery(QueryPtr(oneQuery));
        } // do nothing when token size == 0
    }

    Query *lastQuery = createTerminalQuery(resultTermArray, op, "");
    if (resultQuery->getChildQuery()->size() > 0) {
        if(NULL != lastQuery) {
            resultQuery->addQuery(QueryPtr(lastQuery));
        }
        _visitedQuery = resultQuery;
    } else {
        if (NULL != lastQuery) {
            lastQuery->setQueryLabelWithDefaultLevel(query->getQueryLabel());
        }
        _visitedQuery = lastQuery;
        delete resultQuery;
    }
}

Query* QueryTokenizer::createTerminalQuery(
        const MultiTermQuery::TermArray &resultTermArray,
        QueryOperator op, const std::string &label)
{
    if(resultTermArray.size() == 0) {
        return NULL;
    } else if (resultTermArray.size() == 1) {
        TermQuery *resultQuery = new TermQuery(*resultTermArray[0], label);
        return resultQuery;
    } else {
        MultiTermQuery *resultQuery = new MultiTermQuery(label);
        for(auto &term : resultTermArray) {
            resultQuery->addTerm(term);
        }
        resultQuery->setOPExpr(op);
        return resultQuery;
    }
}

void QueryTokenizer::visitNumberQuery(const NumberQuery *query) {
    const NumberTerm &numberTerm = query->getTerm();
    Query *resultQuery = new NumberQuery(numberTerm, query->getQueryLabel());
    _visitedQuery = resultQuery;
}

void QueryTokenizer::visitAndQuery(const AndQuery *query) {
    Query *resultQuery = new AndQuery(query->getQueryLabel());
    visitBinaryQuery(resultQuery, query);
}

void QueryTokenizer::visitOrQuery(const OrQuery *query) {
    Query *resultQuery = new OrQuery(query->getQueryLabel());
    visitBinaryQuery(resultQuery, query);
}

void QueryTokenizer::visitAndNotQuery(const AndNotQuery *query) {
    Query *resultQuery = new AndNotQuery(query->getQueryLabel());
    visitBinaryQuery(resultQuery, query);
}

void QueryTokenizer::visitRankQuery(const RankQuery *query) {
    Query *resultQuery = new RankQuery(query->getQueryLabel());
    visitBinaryQuery(resultQuery, query);
}

void QueryTokenizer::visitBinaryQuery(Query *resultQuery,
                                      const Query *srcQuery)
{
    const vector<QueryPtr> *childQueryPtr = srcQuery->getChildQuery();
    assert(childQueryPtr);
    assert(childQueryPtr->size() == 2);

    (*childQueryPtr)[0]->accept(this);
    QueryPtr queryPtr0(stealQuery());
    resultQuery->addQuery(queryPtr0);

    (*childQueryPtr)[1]->accept(this);
    QueryPtr queryPtr1(stealQuery());
    resultQuery->addQuery(queryPtr1);

    _visitedQuery = resultQuery;
}

Query* QueryTokenizer::createTermQuery(const Token &token, const Term &term,
                                       const string &label) {
    const string &indexName = term.getIndexName();
    const RequiredFields &requiredFields = term.getRequiredFields();
    const int32_t boost = term.getBoost();
    const string &secondaryChain = term.getTruncateName();
    Query *query = new TermQuery(token, indexName.c_str(),
                                 requiredFields, label, boost, secondaryChain);

    HA3_LOG(DEBUG, "token : %s, pos : %d",
            token.getText().c_str(), (int32_t)token.getPosition());
    return query;
}

Query* QueryTokenizer::createPhraseQuery(const TokenVect &tokens, const Term &term,
        const string &label) {
    const string &indexName = term.getIndexName();
    const RequiredFields &requiredFields = term.getRequiredFields();
    const int32_t boost = term.getBoost();
    const string &secondaryChain = term.getTruncateName();
    PhraseQuery *query = new PhraseQuery(label);

    for (TokenVect::const_iterator it = tokens.begin();
         it != tokens.end(); ++it)
    {
        const Token &token = *it;
        TermPtr term(new Term(token, indexName.c_str(),
                              requiredFields, boost, secondaryChain));
        query->addTerm(term);
    }

    return query;
}

void QueryTokenizer::addMultiTerms(MultiTermQuery::TermArray &termArray,
        const TokenVect &tokens, const Term &term)
{
    const string &indexName = term.getIndexName();
    const RequiredFields &requiredFields = term.getRequiredFields();
    const int32_t boost = term.getBoost();
    const string &secondaryChain = term.getTruncateName();

    for (TokenVect::const_iterator it = tokens.begin();
         it != tokens.end(); ++it)
    {
        const Token &token = *it;
        TermPtr term(new Term(token, indexName.c_str(),
                        requiredFields, boost, secondaryChain));
        termArray.push_back(term);
    }
}

Query* QueryTokenizer::createDefaultQuery(const TokenVect &tokens, const Term &term,
                                          const string& label) {
    Query *query = NULL;
    if (_defaultOP == OP_AND) {
        query = new AndQuery(label);
    } else if (_defaultOP == OP_OR) {
        query = new OrQuery(label);
    } else {
        HA3_LOG(WARN, "Unknow default query type!");
        return NULL;
    }

    for (TokenVect::const_iterator it = tokens.begin();
         it != tokens.end(); ++it)
    {
        const Token &token = *it;
        Query *tmpQuery = createTermQuery(token, term, "");
        QueryPtr tmpQueryPtr(tmpQuery);
        query->addQuery(tmpQueryPtr);
    }

    return query;
}

Query* QueryTokenizer::stealQuery() {
    Query *tmpQuery = _visitedQuery;
    _visitedQuery = NULL;
    return tmpQuery;
}

END_HA3_NAMESPACE(qrs);
