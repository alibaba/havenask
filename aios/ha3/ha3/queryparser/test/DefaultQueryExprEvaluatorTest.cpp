#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/DefaultQueryExprEvaluator.h>
#include <ha3/common/TermQuery.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <string>
#include <string>
#include <ha3/common/TermQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/OrQueryExpr.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(queryparser);

class DefaultQueryExprEvaluatorTest : public TESTBASE {
public:
    DefaultQueryExprEvaluatorTest();
    ~DefaultQueryExprEvaluatorTest();
public:
    void setUp();
    void tearDown();
protected:
    DefaultQueryExprEvaluator *_evaluator;
    WordsTermExpr *_wordsExpr;
    PhraseTermExpr *_phraseExpr;
    NumberTermExpr *_numberExpr;
    IndexInfos *_infos;
    TableInfo *_tableInfo;
    QueryParser *_queryParser;
    string _wordsLabel;
    string _phraseLabel;
    string _numberLabel;
protected:
    HA3_LOG_DECLARE();
};


HA3_LOG_SETUP(queryparser, DefaultQueryExprEvaluatorTest);


DefaultQueryExprEvaluatorTest::DefaultQueryExprEvaluatorTest() {
    _evaluator = NULL;
    _wordsExpr = NULL;
    _phraseExpr = NULL;
    _numberExpr = NULL;
    _queryParser = NULL;
    _infos = NULL;
    _wordsLabel = "words_label";
    _phraseLabel = "phrase_label";
    _numberLabel = "number_label";
}

DefaultQueryExprEvaluatorTest::~DefaultQueryExprEvaluatorTest() {
}

void DefaultQueryExprEvaluatorTest::setUp() {
    _infos = new IndexInfos();
    IndexInfo *indexInfo = new IndexInfo();
    indexInfo->setIndexName("phrase");
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->addField("description", 100);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("SimpleAnalyzer");
    _infos->addIndexInfo(indexInfo);

    _tableInfo = new TableInfo();
    _tableInfo->setIndexInfos(_infos);
    _queryParser = new QueryParser("phrase");
    _evaluator = new DefaultQueryExprEvaluator();
    _wordsExpr = new WordsTermExpr("abc");
    _wordsExpr->setIndexName("phrase");
    _wordsExpr->setBoost(10);
    _wordsExpr->setLabel(_wordsLabel);

    _phraseExpr = new PhraseTermExpr("abc def");
    _phraseExpr->setIndexName("phrase");
    _phraseExpr->setBoost(10);
    _phraseExpr->setLabel(_phraseLabel);

    _numberExpr = new NumberTermExpr(1, true, 2, true);
    _numberExpr->setIndexName("number");
    _numberExpr->setLabel(_numberLabel);
}

void DefaultQueryExprEvaluatorTest::tearDown() {
    delete _tableInfo;
    delete _evaluator;
    delete _wordsExpr;
    delete _phraseExpr;
    delete _numberExpr;
    delete _queryParser;
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateWordsExpr) {
    HA3_LOG(DEBUG, "Begin Test!");
    _evaluator->evaluateWordsExpr(_wordsExpr);
    Query *query =  _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);
    TermQuery *result = (TermQuery*)query;
    RequiredFields requiredFields;
    TermQuery expectTermQuery("abc", "phrase", requiredFields, "", 10);
    ASSERT_EQ(expectTermQuery, *result);
    ASSERT_EQ(_wordsLabel, result->getQueryLabel());
}


TEST_F(DefaultQueryExprEvaluatorTest, testEvaluatePhraseExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    _phraseExpr->setText("abc def");
    _evaluator->evaluatePhraseExpr(_phraseExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    PhraseQuery expectedQuery("");
    RequiredFields requiredFields;
    TermPtr term1(new Term("abc def", "phrase", requiredFields,10));
    expectedQuery.addTerm(term1);

    PhraseQuery *result = (PhraseQuery*)query;
    ASSERT_EQ(expectedQuery, *result);
    ASSERT_EQ(_phraseLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateNumberExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    _numberExpr->setLeftNumber(2, true);
    _numberExpr->setRightNumber(10, true);
    _evaluator->evaluateNumberExpr(_numberExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    NumberQuery *result = (NumberQuery*)query;
    RequiredFields requiredFields;
    NumberQuery expectNumberQuery(2, true, 10, true, "number", requiredFields, "");
    ASSERT_EQ(expectNumberQuery, *result);
    ASSERT_EQ(_numberLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateNumberExprWithRightInclusive) {
    HA3_LOG(DEBUG, "Begin Test!");

    _numberExpr->setLeftNumber(2, false);
    _numberExpr->setRightNumber(10, true);
    _evaluator->evaluateNumberExpr(_numberExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    NumberQuery *result = (NumberQuery*)query;
    RequiredFields requiredFields;
    NumberQuery expectNumberQuery(2, false, 10, true,
                                  "number", requiredFields, "");
    ASSERT_EQ(expectNumberQuery, *result);
    ASSERT_EQ(_numberLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateNumberExprWithLeftInclusive) {
    HA3_LOG(DEBUG, "Begin Test!");

    _numberExpr->setLeftNumber(2, true);
    _numberExpr->setRightNumber(10, false);
    _evaluator->evaluateNumberExpr(_numberExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    NumberQuery *result = (NumberQuery*)query;
    RequiredFields requiredFields;
    NumberQuery expectNumberQuery(2, true, 10, false,
                                  "number", requiredFields, "");
    ASSERT_EQ(expectNumberQuery, *result);
    ASSERT_EQ(_numberLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateNumberExprWithoutInclusive) {
    HA3_LOG(DEBUG, "Begin Test!");

    _numberExpr->setLeftNumber(210, false);
    _numberExpr->setRightNumber(344, false);
    _evaluator->evaluateNumberExpr(_numberExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    NumberQuery *result = (NumberQuery*)query;
    RequiredFields requiredFields;
    NumberQuery expectNumberQuery(210, false, 344, false,
                                  "number", requiredFields, "");
    ASSERT_EQ(expectNumberQuery, *result);
    ASSERT_EQ(_numberLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateNumberExprWithSingleTerm) {
    HA3_LOG(DEBUG, "Begin Test!");

    _numberExpr->setLeftNumber(2, true);
    _numberExpr->setRightNumber(2, true);
    _evaluator->evaluateNumberExpr(_numberExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    NumberQuery *result = (NumberQuery*)query;
    RequiredFields requiredFields;
    NumberQuery expectNumberQuery(2, "number", requiredFields, "");
    ASSERT_EQ(expectNumberQuery, *result);
    ASSERT_EQ(_numberLabel, result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateOrExprTwoTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
    wordsTermExpr1->setIndexName("phrase");
    wordsTermExpr1->setBoost(10);

    WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
    wordsTermExpr2->setIndexName("phrase");
    wordsTermExpr2->setBoost(10);

    OrQueryExpr orQueryExpr(wordsTermExpr1, wordsTermExpr2);
    orQueryExpr.setLabel("orLabel");
    _evaluator->evaluateOrExpr(&orQueryExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    OrQuery expectedQuery("");
    RequiredFields requiredFields;
    QueryPtr termQueryPtr1(new TermQuery("abc", "phrase", requiredFields, "", 10));
    QueryPtr termQueryPtr2(new TermQuery("def", "phrase", requiredFields, "", 10));
    expectedQuery.addQuery(termQueryPtr1);
    expectedQuery.addQuery(termQueryPtr2);

    OrQuery *result = (OrQuery*)query;
    ASSERT_EQ(expectedQuery, *result);
    ASSERT_EQ("orLabel", result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateMultiTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
    wordsTermExpr1->setIndexName("phrase");
    wordsTermExpr1->setBoost(10);

    WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
    wordsTermExpr2->setIndexName("phrase");
    wordsTermExpr2->setBoost(10);

    WordsTermExpr *wordsTermExpr3 = new WordsTermExpr("g");
    wordsTermExpr3->setIndexName("phrase");
    wordsTermExpr3->setBoost(10);

    MultiTermQueryExpr multiTermQueryExpr(OP_WEAKAND);
    multiTermQueryExpr.addTermQueryExpr(wordsTermExpr1);
    multiTermQueryExpr.addTermQueryExpr(wordsTermExpr2);
    multiTermQueryExpr.addTermQueryExpr(wordsTermExpr3);
    multiTermQueryExpr.setMinShouldMatch(2);
    multiTermQueryExpr.setLabel("multiLabel");

    _evaluator->evaluateMultiTermExpr(&multiTermQueryExpr);
    Query *query = _evaluator->stealQuery();
    std::unique_ptr<Query> queryPtr(query);
    ASSERT_TRUE(query != NULL);

    MultiTermQuery expectedQuery("multiLabel", OP_WEAKAND);
    RequiredFields requiredFields;
    TermPtr term1(new Term("abc", "phrase", requiredFields, 10));
    TermPtr term2(new Term("def", "phrase", requiredFields, 10));
    TermPtr term3(new Term("g", "phrase", requiredFields, 10));
    expectedQuery.addTerm(term1);
    expectedQuery.addTerm(term2);
    expectedQuery.addTerm(term3);
    expectedQuery.setMinShouldMatch(2);

    MultiTermQuery *result = (MultiTermQuery*)query;
    ASSERT_EQ(expectedQuery, *result);
    ASSERT_EQ("multiLabel", result->getQueryLabel());
}

TEST_F(DefaultQueryExprEvaluatorTest, testEvaluateLabelTwoTermQuery) {
    {
        WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
        wordsTermExpr1->setIndexName("phrase");
        wordsTermExpr1->setBoost(10);

        WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
        wordsTermExpr2->setIndexName("phrase");
        wordsTermExpr2->setBoost(10);
        AndQueryExpr queryExpr(wordsTermExpr1, wordsTermExpr2);
        queryExpr.setLabel("testlabel");
        _evaluator->evaluateAndExpr(&queryExpr);
        Query *query = _evaluator->stealQuery();
        std::unique_ptr<Query> queryPtr(query);
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("testlabel", query->getQueryLabel());
    }
    {
        WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
        wordsTermExpr1->setIndexName("phrase");
        wordsTermExpr1->setBoost(10);

        WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
        wordsTermExpr2->setIndexName("phrase");
        wordsTermExpr2->setBoost(10);
        OrQueryExpr queryExpr(wordsTermExpr1, wordsTermExpr2);
        queryExpr.setLabel("testlabel");
        _evaluator->evaluateOrExpr(&queryExpr);
        Query *query = _evaluator->stealQuery();
        std::unique_ptr<Query> queryPtr(query);
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("testlabel", query->getQueryLabel());
    }
    {
        WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
        wordsTermExpr1->setIndexName("phrase");
        wordsTermExpr1->setBoost(10);

        WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
        wordsTermExpr2->setIndexName("phrase");
        wordsTermExpr2->setBoost(10);
        AndNotQueryExpr queryExpr(wordsTermExpr1, wordsTermExpr2);
        queryExpr.setLabel("testlabel");
        _evaluator->evaluateAndNotExpr(&queryExpr);
        Query *query = _evaluator->stealQuery();
        std::unique_ptr<Query> queryPtr(query);
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("testlabel", query->getQueryLabel());
    }
    {
        WordsTermExpr *wordsTermExpr1 = new WordsTermExpr("abc");
        wordsTermExpr1->setIndexName("phrase");
        wordsTermExpr1->setBoost(10);

        WordsTermExpr *wordsTermExpr2 = new WordsTermExpr("def");
        wordsTermExpr2->setIndexName("phrase");
        wordsTermExpr2->setBoost(10);
        RankQueryExpr queryExpr(wordsTermExpr1, wordsTermExpr2);
        queryExpr.setLabel("testlabel");
        _evaluator->evaluateRankExpr(&queryExpr);
        Query *query = _evaluator->stealQuery();
        std::unique_ptr<Query> queryPtr(query);
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("testlabel", query->getQueryLabel());
    }
}

END_HA3_NAMESPACE(queryparser);
