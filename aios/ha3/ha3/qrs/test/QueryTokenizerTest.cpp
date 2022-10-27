#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/test/test.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <build_service/config/ResourceReader.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/MultiTermQuery.h>

using namespace std;
using namespace build_service::config;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(qrs);

class QueryTokenizerTest : public TESTBASE {
public:
    QueryTokenizerTest();
    ~QueryTokenizerTest();
public:
    void setUp();
    void tearDown();
protected:
    void initInstances(bool caseSensitive = false);
protected:
    static const std::string ALIWS_CONFIG_PATH;

    template<typename QueryType>
    void testTokenizeBinaryQuery();

    template <typename QueryType>
    QueryType* createBinaryQuery(const char *word1,
                                 const char *word2,
                                 const char *word3);
protected:
    IndexInfos *_infos;
    build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    QueryTokenizer *_queryTokenizer;
    common::ConfigClause _configClause;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QueryTokenizerTest);


const string QueryTokenizerTest::ALIWS_CONFIG_PATH =
    string(ALIWSLIB_DATA_PATH) + "/conf/";

QueryTokenizerTest::QueryTokenizerTest() {
}

QueryTokenizerTest::~QueryTokenizerTest() {
}

void QueryTokenizerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    initInstances();
}

void QueryTokenizerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_infos);
    DELETE_AND_SET_NULL(_queryTokenizer);
    DELETE_AND_SET_NULL(_analyzerFactory);
}

TEST_F(QueryTokenizerTest, testTokenizePhraseQueryWithTwoToken) {
    HA3_LOG(DEBUG, "Begin Test!");

    string label("testLabelName");
    PhraseQuery originalQuery(label);
    RequiredFields requiredFields;
    originalQuery.addTerm(TermPtr(new Term("abc def", "default", requiredFields,
                            100, "secondaryChain")));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    PhraseQuery *resultQuery = dynamic_cast<PhraseQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    PhraseQuery expectedQuery(label);
    expectedQuery.addTerm(TermPtr(new Term(Token("abc", 0, "abc", false),
                            "default", requiredFields, 100, "secondaryChain")));
    expectedQuery.addTerm(TermPtr(new Term(Token("def", 2, "def", false),
                            "default", requiredFields, 100, "secondaryChain")));
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    ASSERT_EQ(label, resultQuery->getQueryLabel());
}

TEST_F(QueryTokenizerTest, testTokenizePhraseQueryWithNoTokenizeIndexes) {
    HA3_LOG(DEBUG, "Begin Test!");

    string label("testLabelName");
    PhraseQuery originalQuery(label);
    RequiredFields requiredFields;
    originalQuery.addTerm(TermPtr(new Term("aBc dEf", "default", requiredFields,
                            100, "secondaryChain")));
    ConfigClause configClause;
    configClause.addNoTokenizeIndex("default");
    _queryTokenizer->setConfigClause(&configClause);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    TermQuery *resultQuery =
        dynamic_cast<TermQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    TermQuery expectedQuery(Token("aBc dEf", 0, "abc def", false),
                            "default", requiredFields, label, 100, "secondaryChain");
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    ASSERT_EQ(label, resultQuery->getQueryLabel());
}

TEST_F(QueryTokenizerTest, testTokenizePhraseQueryWithOneToken) {
    HA3_LOG(DEBUG, "Begin Test!");
    string label("testLabelName");
    PhraseQuery originalQuery(label);
    RequiredFields requiredField;
    originalQuery.addTerm(TermPtr(new Term("abc", "default", requiredField,
                            200, "secondaryChain")));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    TermQuery *resultQuery = dynamic_cast<TermQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    TermQuery expectedQuery("abc", "default", requiredField, label,
                            200, "secondaryChain");
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    ASSERT_EQ(label, resultQuery->getQueryLabel());
}

TEST_F(QueryTokenizerTest, testTokenizeTermQueryWithOneToken) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    string label("testLabel");
    TermQuery originalQuery("abc", "default", requiredField, label,
                            300, "secondaryChain");

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    TermQuery *resultQuery = dynamic_cast<TermQuery*>(tokenizedQueryPtr.get());
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    ASSERT_TRUE(resultQuery != NULL);

    TermQuery expectedQuery("abc", "default", requiredField, label,
                            300, "secondaryChain");
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    ASSERT_EQ(label, resultQuery->getQueryLabel());
}


TEST_F(QueryTokenizerTest, testTokenizeTermQueryWithTwoToken) {
    HA3_LOG(DEBUG, "Begin Test!");

    {
        RequiredFields requiredField;
        string label("testLabelName");
        TermQuery originalQuery("abc def", "default", requiredField, label,
                                400, "secondaryChain");

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery(label);
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                        400, "secondaryChain"));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2), "default",
                        requiredField,"",
                        400, "secondaryChain"));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
        ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
        ASSERT_EQ(label, resultQuery->getQueryLabel());
    }

    {
        RequiredFields requiredField;
        string label("testLabelName");
        TermQuery originalQuery("abc def", "default", requiredField, label,
                                400, "secondaryChain");

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery(label);
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                        400, "secondaryChain"));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2), "default",
                        requiredField,"",
                        400, "secondaryChain"));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
        ASSERT_EQ(label, resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
    }
}

TEST_F(QueryTokenizerTest, testTokenizeAndQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    testTokenizeBinaryQuery<AndQuery>();
}


TEST_F(QueryTokenizerTest, testTokenizeOrQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    testTokenizeBinaryQuery<OrQuery>();
}

TEST_F(QueryTokenizerTest, testTokenizeRankQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    testTokenizeBinaryQuery<RankQuery>();
}

TEST_F(QueryTokenizerTest, testTokenizeAndNotQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    testTokenizeBinaryQuery<AndNotQuery>();
}

template<typename QueryType>
void QueryTokenizerTest::testTokenizeBinaryQuery() {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                    101, "secondaryChain"));
    QueryPtr rightQueryPtr(new TermQuery("def", "default", requiredField, "",
                    102, "secondaryChain"));
    string label("testLabelName");
    QueryType originalQuery(label);
    originalQuery.addQuery(leftQueryPtr);
    originalQuery.addQuery(rightQueryPtr);

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    QueryType *resultQuery = dynamic_cast<QueryType*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    QueryType expectedQuery(label);
    QueryPtr leftExpectQueryPtr(new TermQuery("abc",
                    "default", requiredField, "", 101, "secondaryChain"));
    QueryPtr rightExpectQueryPtr(new TermQuery("def",
                    "default", requiredField, "", 102, "secondaryChain"));
    expectedQuery.addQuery(leftExpectQueryPtr);
    expectedQuery.addQuery(rightExpectQueryPtr);
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenizeNumberQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    string label("testLabel");
    NumberQuery originalQuery(2, true, 10, true, "numberIndexName1",
                              requiredField, label, 200, "secondaryChain");

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    NumberQuery *resultQuery = dynamic_cast<NumberQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    NumberQuery expectedQuery(2, true, 10, true, "numberIndexName1",
                              requiredField, label, 200, "secondaryChain");
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
}


TEST_F(QueryTokenizerTest, testTokenizeComplicatedQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    OrQuery originalQuery("");
    QueryPtr andQueryPtr(createBinaryQuery<AndQuery>("abc", "def", NULL));
    RequiredFields requiredField;
    QueryPtr termQueryPtr3(
            new TermQuery("gh", "default", requiredField,"", 700));
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(termQueryPtr3);

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery("");
    QueryPtr expectedAndQueryPtr(createBinaryQuery<AndQuery>("abc", "def", NULL));
    QueryPtr expectedTermQueryPtr3(new TermQuery("gh", "default",
                    requiredField, "", 700));
    expectedQuery.addQuery(expectedAndQueryPtr);
    expectedQuery.addQuery(expectedTermQueryPtr3);

    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeComplicatedQueryWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");

    string originLabel("originLabel"), andLabel("andLabel"), termLabel("termLabel");
    OrQuery originalQuery(originLabel);
    QueryPtr andQueryPtr(createBinaryQuery<AndQuery>("abc", "def", NULL));
    andQueryPtr->setQueryLabelWithDefaultLevel(andLabel);
    RequiredFields requiredField;
    QueryPtr termQueryPtr3(
            new TermQuery("gh", "default", requiredField, termLabel, 700));
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(termQueryPtr3);

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    ASSERT_EQ(originLabel, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());

    ASSERT_EQ((size_t)2u, resultQuery->getChildQuery()->size());
    QueryPtr andQueryNew = (*resultQuery->getChildQuery())[0];
    ASSERT_EQ(andLabel, andQueryNew->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, andQueryNew->getMatchDataLevel());

    QueryPtr termQueryNew = (*resultQuery->getChildQuery())[1];
    ASSERT_EQ(termLabel, termQueryNew->getQueryLabel());
    ASSERT_EQ(MDL_TERM, termQueryNew->getMatchDataLevel());

    OrQuery expectedQuery("");
    QueryPtr expectedAndQueryPtr(createBinaryQuery<AndQuery>("abc", "def", NULL));
    QueryPtr expectedTermQueryPtr3(new TermQuery("gh", "default",
                    requiredField, "", 700));
    expectedQuery.addQuery(expectedAndQueryPtr);
    expectedQuery.addQuery(expectedTermQueryPtr3);

    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeQueryWithDefaultQueryOperator_AND) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    TermQuery originalQuery("abc def", "default", requiredField, "", 800);

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                    800));
    QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                    "default", requiredField, "", 800));
    expectedQuery.addQuery(leftQueryPtr);
    expectedQuery.addQuery(rightQueryPtr);
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeQueryWithDefaultQueryOperator_ANDWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequiredFields requiredField;
        TermQuery originalQuery("abc def", "default", requiredField, "", 800);

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        ASSERT_EQ(string(""), resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_NONE, resultQuery->getMatchDataLevel());

        ASSERT_EQ((size_t)2u, resultQuery->getChildQuery()->size());
        QueryPtr leftQueryNew = (*resultQuery->getChildQuery())[0];
        ASSERT_EQ(string(""), leftQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, leftQueryNew->getMatchDataLevel());

        QueryPtr rightQueryNew = (*resultQuery->getChildQuery())[1];
        ASSERT_EQ(string(""), rightQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, rightQueryNew->getMatchDataLevel());

        AndQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                        800));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                        "default", requiredField, "", 800));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequiredFields requiredField;
        string label("testLabel");
        TermQuery originalQuery("abc def", "default", requiredField, label, 800);

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        ASSERT_EQ(label, resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());

        ASSERT_EQ((size_t)2u, resultQuery->getChildQuery()->size());
        QueryPtr leftQueryNew = (*resultQuery->getChildQuery())[0];
        ASSERT_EQ(string(""), leftQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, leftQueryNew->getMatchDataLevel());

        QueryPtr rightQueryNew = (*resultQuery->getChildQuery())[1];
        ASSERT_EQ(string(""), rightQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, rightQueryNew->getMatchDataLevel());

        AndQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "",
                        800));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                        "default", requiredField, "", 800));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(QueryTokenizerTest, testTokenizeStopWordQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    TermQuery originalQuery("abc, def.", "default", requiredField, "", 900);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_OR);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery("");
    QueryPtr queryPtr1(new TermQuery("abc", "default", requiredField, "", 900));
    QueryPtr queryPtr2(new TermQuery(Token(",", 1, ",", true),
                    "default", requiredField, "", 900));
    QueryPtr queryPtr3(new TermQuery(Token("def", 3),
                    "default", requiredField, "", 900));
    QueryPtr queryPtr4(new TermQuery(Token(".", 4, ".", true),
                    "default", requiredField, "", 900));
    expectedQuery.addQuery(queryPtr1);
    expectedQuery.addQuery(queryPtr2);
    expectedQuery.addQuery(queryPtr3);
    expectedQuery.addQuery(queryPtr4);
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeQueryWithDefaultQueryOperator_OR) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    TermQuery originalQuery("abc def", "default", requiredField, "", 900);

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_OR);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery("");
    QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "", 900));
    QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                    "default", requiredField, "", 900));
    expectedQuery.addQuery(leftQueryPtr);
    expectedQuery.addQuery(rightQueryPtr);
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeQueryWithDefaultQueryOperator_ORWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequiredFields requiredField;
        TermQuery originalQuery("abc def", "default", requiredField, "", 900);

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_OR);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        ASSERT_EQ(string(""), resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_NONE, resultQuery->getMatchDataLevel());

        ASSERT_EQ((size_t)2u, resultQuery->getChildQuery()->size());
        QueryPtr leftQueryNew = (*resultQuery->getChildQuery())[0];
        ASSERT_EQ(string(""), leftQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, leftQueryNew->getMatchDataLevel());

        QueryPtr rightQueryNew = (*resultQuery->getChildQuery())[1];
        ASSERT_EQ(string(""), rightQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, rightQueryNew->getMatchDataLevel());

        OrQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "", 900));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                        "default", requiredField, "", 900));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequiredFields requiredField;
        string label("testLabel");
        TermQuery originalQuery("abc def", "default", requiredField, label, 900);

        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_OR);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        ASSERT_EQ(label, resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());

        ASSERT_EQ((size_t)2u, resultQuery->getChildQuery()->size());
        QueryPtr leftQueryNew = (*resultQuery->getChildQuery())[0];
        ASSERT_EQ(string(""), leftQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, leftQueryNew->getMatchDataLevel());

        QueryPtr rightQueryNew = (*resultQuery->getChildQuery())[1];
        ASSERT_EQ(string(""), rightQueryNew->getQueryLabel());
        ASSERT_EQ(MDL_TERM, rightQueryNew->getMatchDataLevel());

        OrQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("abc", "default", requiredField, "", 900));
        QueryPtr rightQueryPtr(new TermQuery(Token("def", 2),
                        "default", requiredField, "", 900));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(QueryTokenizerTest, testCaseSensitiveFalse) {
    HA3_LOG(DEBUG, "Begin Test!");

    PhraseQuery originalQuery("");
    RequiredFields requiredField;
    originalQuery.addTerm(TermPtr(new Term("aBc DeF", "default", requiredField,
                            900)));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_OR);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    PhraseQuery *resultQuery = dynamic_cast<PhraseQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    PhraseQuery expectedQuery("");
    expectedQuery.addTerm(TermPtr(new Term(Token("aBc", 0, "abc", false),
                            "default", requiredField, 900)));
    expectedQuery.addTerm(TermPtr(new Term(Token("DeF", 2, "def", false),
                            "default", requiredField, 900)));
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testCaseSensitiveTrue) {
    HA3_LOG(DEBUG, "Begin Test!");

    AnalyzerFactoryPtr analyzerFactoryPtr(new AnalyzerFactory);

    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());

    unique_ptr<AnalyzerInfo> sinlgeInfo(new AnalyzerInfo());
    sinlgeInfo->setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    sinlgeInfo->addStopWord(string("."));
    sinlgeInfo->addStopWord(string(","));
    sinlgeInfo->addStopWord(string("-"));
    sinlgeInfo->addStopWord(string("mm"));
    NormalizeOptions option;
    option.caseSensitive = true;
    sinlgeInfo->setNormalizeOptions(option);
    infosPtr->addAnalyzerInfo("singlews", *sinlgeInfo);

    infosPtr->makeCompatibleWithOldConfig();
    ResourceReaderPtr resourceReader(new ResourceReader(ALIWS_CONFIG_PATH));
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(resourceReader));
    tokenizerManagerPtr->init(infosPtr->getTokenizerConfig());

    analyzerFactoryPtr->init(infosPtr, tokenizerManagerPtr);
    ConfigClause configClause;
    QueryTokenizerPtr queryTokenizerPtr(new QueryTokenizer(analyzerFactoryPtr.get()));
    queryTokenizerPtr->setConfigClause(&configClause);
    RequiredFields requiredField;
    TermQuery originalQuery("aBc DeF", "default", requiredField, "", 900);

    queryTokenizerPtr->tokenizeQuery(&originalQuery, _infos, OP_OR);
    QueryPtr tokenizedQueryPtr(queryTokenizerPtr->stealQuery());

    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery("");
    QueryPtr leftQueryPtr(new TermQuery("aBc", "default", requiredField, "", 900));
    QueryPtr rightQueryPtr(new TermQuery(Token("DeF", 2),
                    "default", requiredField, "", 900));
    expectedQuery.addQuery(leftQueryPtr);
    expectedQuery.addQuery(rightQueryPtr);
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenizeWithAllStopWords) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequiredFields requiredField;
    QueryTokenizer::TokenVect tokens;
    _queryTokenizer->_indexInfos = _infos;
    Term term("is stop word", "default", requiredField, 200);
    _queryTokenizer->tokenize("default", term, tokens);

    ASSERT_EQ((size_t)3, tokens.size());
}

TEST_F(QueryTokenizerTest, testTokenizeWithOneWord) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequiredFields requiredField;
    QueryTokenizer::TokenVect tokens;
    _queryTokenizer->_indexInfos = _infos;
    Term term("it is stop word", "default", requiredField, 200);
    _queryTokenizer->tokenize("default", term, tokens);
    ASSERT_EQ((size_t)4, tokens.size());
}

TEST_F(QueryTokenizerTest, testTokenizeWithSpecifiedAnalyzer) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequiredFields requiredField;
    Term term("it is stop word", "default", requiredField, 200);
    {
        QueryTokenizer::TokenVect tokens;
        _queryTokenizer->_indexInfos = _infos;
        ConfigClause configClause;
        configClause.setAnalyzerName("simple");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenize("default", term, tokens);

        ASSERT_EQ((size_t)4, tokens.size());
        ASSERT_TRUE(!_queryTokenizer->_errorResult.hasError());
    }

    {//set not exist analyzer name

        QueryTokenizer::TokenVect tokens;
        _queryTokenizer->_indexInfos = _infos;
        ConfigClause configClause;
        configClause.setAnalyzerName("notExist");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenize("default", term, tokens);

        ASSERT_EQ((size_t)0, tokens.size());
        ErrorResult &errorResult = _queryTokenizer->_errorResult;
        ASSERT_EQ(ERROR_ANALYZER_NOT_EXIST,
                             errorResult.getErrorCode());
    }

    {//set empty analyzer name
        QueryTokenizer::TokenVect tokens;
        _queryTokenizer->_indexInfos = _infos;
        ConfigClause configClause;
        configClause.setAnalyzerName("");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenize("default", term, tokens);
        ASSERT_EQ((size_t)4, tokens.size());
        ASSERT_EQ(ERROR_ANALYZER_NOT_EXIST,
                             _queryTokenizer->_errorResult.getErrorCode());
    }
}


TEST_F(QueryTokenizerTest, testTokenizeWithMutilWords) {

    HA3_LOG(DEBUG, "Begin Test!");
    RequiredFields requiredField;
    Term term("it is not a stop word", "default", requiredField, 200);
    QueryTokenizer::TokenVect tokens;
    _queryTokenizer->_indexInfos = _infos;
    _queryTokenizer->tokenize("default", term, tokens);
    ASSERT_EQ((size_t)6, tokens.size());
}

TEST_F(QueryTokenizerTest, testVisitPhraseQueryWithAllStopWord) {
    HA3_LOG(DEBUG, "Begin Test!");

    PhraseQuery originalQuery("");
    RequiredFields requiredField;
    originalQuery.addTerm(TermPtr(new Term("is stop word", "default", requiredField, 200)));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    ASSERT_TRUE(tokenizedQueryPtr != NULL);
}

TEST_F(QueryTokenizerTest, testVisitPhraseQueryWithOneWord) {
    HA3_LOG(DEBUG, "Begin Test!");

    PhraseQuery originalQuery("");
    RequiredFields requiredField;
    originalQuery.addTerm(TermPtr(new Term("it is stop word", "default", requiredField, 200)));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    ASSERT_TRUE(tokenizedQueryPtr != NULL);
    PhraseQuery *resultQuery = dynamic_cast<PhraseQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);
}

TEST_F(QueryTokenizerTest, testVisitPhraseQueryWithMultiWords) {
    HA3_LOG(DEBUG, "Begin Test!");

    PhraseQuery originalQuery("");
    RequiredFields requiredField;
    originalQuery.addTerm(TermPtr(new Term("it is not a stop word",
                            "default", requiredField, 200)));

    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    ASSERT_TRUE(tokenizedQueryPtr != NULL);
    PhraseQuery *resultQuery = dynamic_cast<PhraseQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);
    PhraseQuery::TermArray termArray = resultQuery->getTermArray();
    ASSERT_EQ(std::string("it"), termArray[0]->getWord());
    ASSERT_EQ(std::string("is"), termArray[1]->getWord());
    ASSERT_EQ(std::string("not"), termArray[2]->getWord());
    ASSERT_EQ(std::string("a"), termArray[3]->getWord());
    ASSERT_EQ(std::string("stop"), termArray[4]->getWord());
    ASSERT_EQ(std::string("word"), termArray[5]->getWord());
}

TEST_F(QueryTokenizerTest, testTokenizeWithAnalyzerClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    //test use scheme analyzer
    {
        RequiredFields requiredField;
        TermQuery originalQuery("菊花", "default", requiredField, "", 900);
        ConfigClause configClause;
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("菊", "default", requiredField, "", 900));
        QueryPtr rightQueryPtr(new TermQuery(Token("花", 1),
                        "default", requiredField, "", 900));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    //test use specific index analyzer
    {
        RequiredFields requiredField;
        TermQuery originalQuery("菊花茶", "default", requiredField, "", 900);
        ConfigClause configClause;
        configClause.addIndexAnalyzerName("default", "singlews");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, "", 900));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, "", 900));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, "", 900));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    //test use global index analyzer
    {
        RequiredFields requiredField;
        TermQuery originalQuery("菊花茶", "default", requiredField, "", 900);
        ConfigClause configClause;
        configClause.setAnalyzerName("singlews");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, "", 900));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, "", 900));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, "", 900));

        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    //test use specific index analyzer when has global index analyzer
    {
        RequiredFields requiredField;
        TermQuery originalQuery("菊花茶", "default", requiredField, "", 900);
        ConfigClause configClause;
        configClause.setAnalyzerName("alibaba");
        configClause.addIndexAnalyzerName("default", "singlews");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);

        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, "", 900));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, "", 900));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, "", 900));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    // test not use analyzer
    {
        RequiredFields requiredField;
        TermQuery originalQuery("菊花茶", "default", requiredField, "", 900);
        ConfigClause configClause;
        configClause.setAnalyzerName("singlews");
        configClause.addIndexAnalyzerName("default", "singlews");
        configClause.addNoTokenizeIndex("default");
        _queryTokenizer->setConfigClause(&configClause);
        _queryTokenizer->tokenizeQuery(&originalQuery, _infos, OP_AND);
        QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
        TermQuery *resultQuery = dynamic_cast<TermQuery*>(tokenizedQueryPtr.get());
        ASSERT_TRUE(resultQuery != NULL);
        TermQuery expectedQuery("菊花茶", "default", requiredField, "", 900);;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(QueryTokenizerTest, testMultiEmptyMultiTermQuery) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("", "default", requiredFields, 100)));
    {
        multiQuery->setOPExpr(OP_OR);
        _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
        ASSERT_EQ(nullptr, _queryTokenizer->stealQuery());
    }
    {
        multiQuery->setOPExpr(OP_AND);
        _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
        ASSERT_EQ(nullptr, _queryTokenizer->stealQuery());
    }
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQuery) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc def", "default", requiredFields, 100)));
    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    MultiTermQuery *resultQuery = dynamic_cast<MultiTermQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    MultiTermQueryPtr expectedQuery(new MultiTermQuery(label));
    TermPtr term1(new Term("abc", "default", requiredFields, 100));
    TermPtr term2(new Term(Token("def", 2), "default", requiredFields, 100));
    expectedQuery->addTerm(term1);
    expectedQuery->addTerm(term2);

    ASSERT_EQ(expectedQuery->toString(), resultQuery->toString());
    ASSERT_EQ(*expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryWithOneTerm) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery(""));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 100)));

    AndQuery originalQuery("");
    QueryPtr termQueryPtr(new TermQuery("xxx", "default", requiredFields, "", 201));
    originalQuery.addQuery(termQueryPtr);
    originalQuery.addQuery(multiQuery);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    QueryPtr rightQuery(new TermQuery("abc", "default", requiredFields, "", 100));
    expectedQuery.addQuery(termQueryPtr);
    expectedQuery.addQuery(rightQuery);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryWithOneTermWithLabel) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 100)));

    AndQuery originalQuery("");
    QueryPtr termQueryPtr(new TermQuery("xxx", "default", requiredFields, "", 201));
    originalQuery.addQuery(termQueryPtr);
    originalQuery.addQuery(multiQuery);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    QueryPtr rightQuery(new TermQuery("abc", "default", requiredFields, label, 100));
    expectedQuery.addQuery(termQueryPtr);
    expectedQuery.addQuery(rightQuery);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ("", resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_NONE, resultQuery->getMatchDataLevel());
    ASSERT_EQ(label, (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryWithDefaultOp) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label, OP_OR));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc def", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("xxx", "default", requiredFields, 100)));

    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    QueryPtr andQuery(new AndQuery(""));
    QueryPtr leftQuery(new TermQuery("abc", "default", requiredFields, "", 100));
    QueryPtr rightQuery(new TermQuery(Token("def", 2), "default", requiredFields, "", 100));
    QueryPtr rightQuery2(new TermQuery("xxx", "default", requiredFields, "", 100));
    andQuery->addQuery(leftQuery);
    andQuery->addQuery(rightQuery);

    OrQuery expectedQuery(label);
    expectedQuery.addQuery(andQuery);
    expectedQuery.addQuery(rightQuery2);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_NONE, (*resultQuery->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryBoostTruncate) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery(""));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 10, "trunc1")));
    multiQuery->addTerm(TermPtr(new Term("def ghi", "default", requiredFields, 20, "trunc2")));
    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    MultiTermQuery *resultQuery = dynamic_cast<MultiTermQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    MultiTermQueryPtr expectedQuery(new MultiTermQuery(""));
    TermPtr term1(new Term("abc", "default", requiredFields, 10, "trunc1"));
    TermPtr term2(new Term("def", "default", requiredFields, 20, "trunc2"));
    TermPtr term3(new Term(Token("ghi", 2), "default", requiredFields, 20, "trunc2"));
    expectedQuery->addTerm(term1);
    expectedQuery->addTerm(term2);
    expectedQuery->addTerm(term3);

    ASSERT_EQ(expectedQuery->toString(), resultQuery->toString());
    ASSERT_EQ(*expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryWithDiffOp) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label, OP_OR));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("bcd efg", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("cde fgh hij", "default", requiredFields, 100)));

    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery(label);
    QueryPtr left(new AndQuery(""));
    QueryPtr ll(new TermQuery("bcd", "default", requiredFields, "", 100));
    QueryPtr lr(new TermQuery(Token("efg", 2), "default", requiredFields, "", 100));
    left->addQuery(ll);
    left->addQuery(lr);
    expectedQuery.addQuery(left);

    QueryPtr middle(new AndQuery("")); //token 默认从0开始，空格也算位置
    QueryPtr ml(new TermQuery("cde", "default", requiredFields, "", 100));
    QueryPtr mm(new TermQuery(Token("fgh", 2), "default", requiredFields, "", 100));
    QueryPtr mr(new TermQuery(Token("hij", 4), "default", requiredFields, "", 100));
    middle->addQuery(ml);
    middle->addQuery(mm);
    middle->addQuery(mr);
    expectedQuery.addQuery(middle);

    QueryPtr right(new TermQuery("abc", "default", requiredFields, "", 100));
    expectedQuery.addQuery(right);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_NONE, (*resultQuery->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_NONE, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[2]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[2]->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryWithDiffOpAlltoken) {
    string label("testLabelName");
    MultiTermQueryPtr multiQuery(new MultiTermQuery(label, OP_OR));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("bcd efg", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("cde fgh hij", "default", requiredFields, 100)));

    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery(label);
    QueryPtr left(new AndQuery(""));
    QueryPtr ll(new TermQuery("bcd", "default", requiredFields, "", 100));
    QueryPtr lr(new TermQuery(Token("efg", 2), "default", requiredFields, "", 100));
    left->addQuery(ll);
    left->addQuery(lr);
    expectedQuery.addQuery(left);

    QueryPtr right(new AndQuery("")); //token 默认从0开始，空格也算位置
    QueryPtr rl(new TermQuery("cde", "default", requiredFields, "", 100));
    QueryPtr rm(new TermQuery(Token("fgh", 2), "default", requiredFields, "", 100));
    QueryPtr rr(new TermQuery(Token("hij", 4), "default", requiredFields, "", 100));
    right->addQuery(rl);
    right->addQuery(rm);
    right->addQuery(rr);
    expectedQuery.addQuery(right);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, resultQuery->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_NONE, (*resultQuery->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_NONE, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryDiffOpBoostTruncate) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery("", OP_OR));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("bcd efg", "default", requiredFields, 20, "trunc2")));
    multiQuery->addTerm(TermPtr(new Term("cde fgh hij", "default", requiredFields, 30, "trunc3")));

    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    OrQuery *resultQuery = dynamic_cast<OrQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    OrQuery expectedQuery("");
    QueryPtr left(new AndQuery(""));
    QueryPtr ll(new TermQuery("bcd", "default", requiredFields, "", 20, "trunc2"));
    QueryPtr lr(new TermQuery(Token("efg", 2), "default", requiredFields, "", 20, "trunc2"));
    left->addQuery(ll);
    left->addQuery(lr);
    expectedQuery.addQuery(left);

    QueryPtr middle(new AndQuery("")); //token 默认从0开始，空格也算位置
    QueryPtr ml(new TermQuery("cde", "default", requiredFields, "", 30, "trunc3"));
    QueryPtr mm(new TermQuery(Token("fgh", 2), "default", requiredFields, "", 30, "trunc3"));
    QueryPtr mr(new TermQuery(Token("hij", 4), "default", requiredFields, "", 30, "trunc3"));
    middle->addQuery(ml);
    middle->addQuery(mm);
    middle->addQuery(mr);
    expectedQuery.addQuery(middle);

    QueryPtr right(new TermQuery("abc", "default", requiredFields, "", 100));
    expectedQuery.addQuery(right);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testTokenMultiTermQueryOneTokenDiffOp) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery("", OP_OR));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("def", "default", requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new Term("ghi", "default", requiredFields, 100)));

    _queryTokenizer->tokenizeQuery(multiQuery.get(), _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    MultiTermQuery *resultQuery = dynamic_cast<MultiTermQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    ASSERT_EQ(multiQuery->toString(), resultQuery->toString());
    ASSERT_EQ(*multiQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testNoNeedTokenizeMultiTermQuery) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery(""));
    RequiredFields requiredFields;
    const char *indexName = "default";
    multiQuery->addTerm(TermPtr(new NumberTerm(123, indexName, requiredFields, 100)));
    multiQuery->addTerm(TermPtr(new NumberTerm(456, indexName, requiredFields, 100)));

    AndQuery originalQuery("");
    QueryPtr termQueryPtr(new TermQuery("xxx", indexName, requiredFields, "", 201));
    originalQuery.addQuery(termQueryPtr);
    originalQuery.addQuery(multiQuery);

    for (IndexInfos::Iterator it = _infos->begin(); it != _infos->end(); ++it) {
        if ((*it)->getIndexName() == indexName) {
            (*it)->setIndexType(it_number);
            _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
            (*it)->setIndexType(it_text);
            break;
        }
    }

    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    QueryPtr rightQuery = multiQuery;
    expectedQuery.addQuery(termQueryPtr);
    expectedQuery.addQuery(rightQuery);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testNoTokenizerMultiTermQuery) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery(""));
    RequiredFields requiredFields;
    multiQuery->addTerm(TermPtr(new Term("abc def", "default", requiredFields, 100)));

    AndQuery originalQuery("");
    QueryPtr termQueryPtr(new TermQuery("xxx", "default", requiredFields, "", 201));

    ConfigClause configClause;
    configClause.addNoTokenizeIndex("default");
    _queryTokenizer->setConfigClause(&configClause);

    originalQuery.addQuery(termQueryPtr);
    originalQuery.addQuery(multiQuery);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    QueryPtr rightQuery(new TermQuery("abc def", "default", requiredFields, "", 100));
    expectedQuery.addQuery(termQueryPtr);
    expectedQuery.addQuery(rightQuery);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
}

TEST_F(QueryTokenizerTest, testWeakAndNotTokenizerMultiTermQuery) {
    MultiTermQueryPtr multiQuery(new MultiTermQuery(""));
    RequiredFields requiredFields;
    multiQuery->setOPExpr(OP_WEAKAND);
    multiQuery->addTerm(TermPtr(new Term("abc def", "default", requiredFields, 100)));

    AndQuery originalQuery("");
    QueryPtr termQueryPtr(new TermQuery("xxx", "default", requiredFields, "", 201));

    ConfigClause configClause;
    _queryTokenizer->setConfigClause(&configClause);

    originalQuery.addQuery(termQueryPtr);
    originalQuery.addQuery(multiQuery);
    _queryTokenizer->tokenizeQuery(&originalQuery, _infos);
    QueryPtr tokenizedQueryPtr(_queryTokenizer->stealQuery());
    AndQuery *resultQuery = dynamic_cast<AndQuery*>(tokenizedQueryPtr.get());
    ASSERT_TRUE(resultQuery != NULL);

    AndQuery expectedQuery("");
    expectedQuery.addQuery(termQueryPtr);
    expectedQuery.addQuery(multiQuery);

    ASSERT_EQ(expectedQuery.toString(), resultQuery->toString());
    ASSERT_EQ(expectedQuery, *resultQuery);
}

template <typename QueryType>
QueryType* QueryTokenizerTest::createBinaryQuery(const char *word1,
        const char *word2,
        const char *word3)
{
    QueryType *resultQuery = new QueryType("");
    RequiredFields requiredField;
    QueryPtr termQueryPtr1(new TermQuery(word1, "default", requiredField, "", 201));
    QueryPtr termQueryPtr2(new TermQuery(word2, "default", requiredField, "", 202));
    resultQuery->addQuery(termQueryPtr1);
    resultQuery->addQuery(termQueryPtr2);

    if (word3 != NULL) {
        QueryPtr termQueryPtr3(new TermQuery(word3,
                        "default", requiredField, "", 203));
        resultQuery->addQuery(termQueryPtr3);
    }
    return resultQuery;
}

void QueryTokenizerTest::initInstances(bool caseSensitive) {
    _analyzerFactory = new AnalyzerFactory;

    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());

    //init simple analyzer info
    AnalyzerInfo simpleInfo;
    simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    simpleInfo.setTokenizerConfig(DELIMITER, " ");
    infosPtr->addAnalyzerInfo("simple", simpleInfo);

    unique_ptr<AnalyzerInfo> sinlgeInfo(new AnalyzerInfo());
    sinlgeInfo->setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    sinlgeInfo->addStopWord(string("."));
    sinlgeInfo->addStopWord(string(","));
    sinlgeInfo->addStopWord(string("-"));
    sinlgeInfo->addStopWord(string("mm"));
    sinlgeInfo->addStopWord(string("stop"));
    sinlgeInfo->addStopWord(string("word"));
    sinlgeInfo->addStopWord(string("is"));
    NormalizeOptions option;
    option.caseSensitive = caseSensitive;
    sinlgeInfo->setNormalizeOptions(option);
    infosPtr->addAnalyzerInfo("singlews", *sinlgeInfo);

    infosPtr->makeCompatibleWithOldConfig();
    ResourceReaderPtr resourceReader(new ResourceReader(ALIWS_CONFIG_PATH));
    HA3_LOG(ERROR, "aliws path: %s", ALIWS_CONFIG_PATH.c_str());
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(resourceReader));
    ASSERT_TRUE(tokenizerManagerPtr->init(infosPtr->getTokenizerConfig()));
    ASSERT_TRUE(_analyzerFactory->init(infosPtr, tokenizerManagerPtr));

    _infos = new IndexInfos();
    IndexInfo *indexInfo = new IndexInfo();
    indexInfo->setIndexName("default");
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->addField("description", 100);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("singlews");
    _infos->addIndexInfo(indexInfo);

    _queryTokenizer = new QueryTokenizer(_analyzerFactory);
    _queryTokenizer->setConfigClause(&_configClause);
}

END_HA3_NAMESPACE(qrs);
