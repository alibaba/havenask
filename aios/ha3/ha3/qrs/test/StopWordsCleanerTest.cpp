#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/StopWordsCleaner.h>
#include <build_service/analyzer/Token.h>

BEGIN_HA3_NAMESPACE(qrs);

class StopWordsCleanerTest : public TESTBASE {
public:
    StopWordsCleanerTest();
    ~StopWordsCleanerTest();
public:
    void setUp();
    void tearDown();
protected:
    StopWordsCleaner *_nullQueryCleaner;
    common::QueryPtr createAllStopWordPhraseQuery();
    common::QueryPtr createRestOneWordPhraseQuery();
    common::QueryPtr createPhraseQuery();
    common::QueryPtr createStopWordTermQuery();
    common::QueryPtr createTermQuery();
    common::TermPtr createNormalTerm();
    common::TermPtr createStopWordTerm();
    
protected:
    HA3_LOG_DECLARE();
};


using namespace std;
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;

HA3_LOG_SETUP(qrs, StopWordsCleanerTest);


StopWordsCleanerTest::StopWordsCleanerTest() { 
    _nullQueryCleaner = NULL;
}

StopWordsCleanerTest::~StopWordsCleanerTest() {    
    if (_nullQueryCleaner) {
        delete _nullQueryCleaner;
        _nullQueryCleaner = NULL;
    }
}

void StopWordsCleanerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _nullQueryCleaner = new StopWordsCleaner;
}

void StopWordsCleanerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_nullQueryCleaner) {
        delete _nullQueryCleaner;
        _nullQueryCleaner = NULL;
    }
}

TEST_F(StopWordsCleanerTest, testNullTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    TermQuery *nullQuery = NULL;
    _nullQueryCleaner->visitTermQuery(nullQuery);
    ASSERT_TRUE(NULL == _nullQueryCleaner->stealQuery());
}

TEST_F(StopWordsCleanerTest, testTermQuery) 
{
    HA3_LOG(DEBUG, "Begin Test!");
    Term term;
    string label("testLabelName");
    TermQuery *query = new TermQuery(term, label);
    _nullQueryCleaner->visitTermQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    delete resultQuery;
    delete query;
}
TEST_F(StopWordsCleanerTest, testNullPhraseQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    PhraseQuery *nullQuery = NULL;
    _nullQueryCleaner->visitPhraseQuery(nullQuery);
    ASSERT_TRUE(NULL == _nullQueryCleaner->stealQuery());
}

TEST_F(StopWordsCleanerTest, testPhraseQuery) 
{
    HA3_LOG(DEBUG, "Begin Test!");
    PhraseQuery *query = new PhraseQuery("");
    _nullQueryCleaner->visitPhraseQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}

TEST_F(StopWordsCleanerTest, testMultiTermQuery) 
{
    HA3_LOG(DEBUG, "Begin Test!");
    MultiTermQuery *query = new MultiTermQuery("");
    _nullQueryCleaner->visitMultiTermQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}

TEST_F(StopWordsCleanerTest, testNullNumberQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    NumberQuery *nullQuery = NULL;
    _nullQueryCleaner->visitNumberQuery(nullQuery);
    ASSERT_TRUE(NULL == _nullQueryCleaner->stealQuery());
}

TEST_F(StopWordsCleanerTest, testNumberQuery) 
{
    HA3_LOG(DEBUG, "Begin Test!");

    RequiredFields requiredField;
    string label("testLabelName");
    NumberQuery *query = new NumberQuery(2, "", requiredField, label);
    _nullQueryCleaner->visitNumberQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
    delete resultQuery;
    delete query;
}

TEST_F(StopWordsCleanerTest, testNullAndQuery)
{
    HA3_LOG(DEBUG, "Begin Test!");
    
    AndQuery *query = NULL;
    _nullQueryCleaner->visitAndQuery(query);
    ASSERT_TRUE(NULL == _nullQueryCleaner->stealQuery());
    
}
TEST_F(StopWordsCleanerTest, testAndQueryWithLeftChild) 
{
    string label("testLabelName");
    AndQuery *query = new AndQuery(label);
    PhraseQuery *query1 = new PhraseQuery("");
    QueryPtr queryPtr1(query1);
    RequiredFields requiredField;
    query1->addTerm(TermPtr(new Term(Token("abc", 0), "default", requiredField)));
    query1->addTerm(TermPtr(new Term(Token("def", 1), "default", requiredField)));
    QueryPtr queryPtr2;
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_TRUE(*query1 == *typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testAndQueryWithRightChild)
{
    string label("testLabelName");
    AndQuery *query = new AndQuery(label);
    QueryPtr queryPtr1;
    QueryPtr queryPtr2 = createPhraseQuery();
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;

}
TEST_F(StopWordsCleanerTest, testAndQueryWithoutChildren)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
   
}
TEST_F(StopWordsCleanerTest, testAndQueryWithoutLeftChildAfterSteal)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11;
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);

    QueryPtr queryPtr2 = createPhraseQuery();
    string label("testLabelName");
    queryPtr2->setQueryLabelWithDefaultLevel(label);
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, resultQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testAndQueryWithoutRightChildAfterSteal)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1 = createPhraseQuery();
    string label("testLabelName");
    queryPtr1->setQueryLabelWithDefaultLevel(label);
    QueryPtr queryPtr2(new AndQuery(""));
    QueryPtr queryPtr21;
    QueryPtr queryPtr22;
    queryPtr2->addQuery(queryPtr21);
    queryPtr2->addQuery(queryPtr22);

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;

}
TEST_F(StopWordsCleanerTest, testAndQuery) 
{
    string label("testLabelName");
    AndQuery *query = new AndQuery(label);
    QueryPtr queryPtr1 = createTermQuery();
    QueryPtr queryPtr2 = createRestOneWordPhraseQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    AndQuery *typeQuery = dynamic_cast<AndQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, typeQuery->getMatchDataLevel());
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testOrQuery) 
{
    string label("testLabelName");
    OrQuery *query = new OrQuery(label);
    QueryPtr queryPtr1 = createTermQuery();
    QueryPtr queryPtr2 = createRestOneWordPhraseQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    OrQuery *typeQuery = dynamic_cast<OrQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, typeQuery->getMatchDataLevel());
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testNullOrQuery) 
{
    OrQuery *query = new OrQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testAndNotQuery) 
{
    string label("testLabelName");
    AndNotQuery *query = new AndNotQuery(label);
    QueryPtr queryPtr1 = createTermQuery();
    QueryPtr queryPtr2 = createTermQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    AndNotQuery *typeQuery = dynamic_cast<AndNotQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, typeQuery->getMatchDataLevel());
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testNullAndNotQuery) 
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testRankQuery) 
{
    string label("testLabelName");
    RankQuery *query = new RankQuery(label);
    QueryPtr queryPtr1 = createPhraseQuery();
    QueryPtr queryPtr2 = createPhraseQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    RankQuery *typeQuery = dynamic_cast<RankQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, typeQuery->getMatchDataLevel());
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testNullRankQuery) 
{
    RankQuery *query = new RankQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    ASSERT_EQ(ERROR_QUERY_INVALID,
                         _nullQueryCleaner->getErrorCode());
    delete query;
}

TEST_F(StopWordsCleanerTest, testRankQueryLeftChildWithStopWord) 
{
    RankQuery *query = new RankQuery("");
    QueryPtr queryPtr1 = createStopWordTermQuery();
    QueryPtr queryPtr2 = createTermQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    ASSERT_EQ(ERROR_QUERY_INVALID,
                         _nullQueryCleaner->getErrorCode());
    delete query;
}

TEST_F(StopWordsCleanerTest, testRankQueryRightChildWithStopWord) 
{
    string label("testLabelName");
    RankQuery *query = new RankQuery(label);
    QueryPtr queryPtr1 = createTermQuery();
    QueryPtr queryPtr2 = createStopWordTermQuery();
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    TermQuery *typeQuery = dynamic_cast<TermQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());
    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testInvalidMultiAndQueryWithStopWord)
{
    AndQuery *query = new AndQuery("");
    QueryPtr rightQueryPtr(new AndNotQuery(""));
    QueryPtr queryPtr1 = createStopWordTermQuery();
    QueryPtr queryPtr2 = createPhraseQuery();
    QueryPtr queryPtr3 = createPhraseQuery();

    rightQueryPtr->addQuery(queryPtr1);
    rightQueryPtr->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    query->addQuery(rightQueryPtr);

    _nullQueryCleaner->clean(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    ASSERT_EQ(ERROR_QUERY_INVALID,
                         _nullQueryCleaner->getErrorCode());
    ASSERT_EQ(string("AndNotQuery or RankQuery's leftQuery " 
                                "is NULL or stop word.")
                         , _nullQueryCleaner->getErrorMsg());
    delete query;

}

TEST_F(StopWordsCleanerTest, testMultiAndQueryWithStopWord)
{
    string label("testLabelName");
    AndQuery *query = new AndQuery(label);
    QueryPtr queryPtr1 = createTermQuery();
    QueryPtr queryPtr2;
    QueryPtr queryPtr3 = createPhraseQuery();
    QueryPtr queryPtr4;
    QueryPtr queryPtr5 = createRestOneWordPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    query->addQuery(queryPtr4);
    query->addQuery(queryPtr5);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    AndQuery *typeQuery = dynamic_cast<AndQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    const std::vector<QueryPtr>* childQuery = typeQuery->getChildQuery();
    ASSERT_EQ(size_t(3),childQuery->size());
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY, typeQuery->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[2]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[2]->getMatchDataLevel());

    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testMultiAndQueryWithAllStopWord)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    QueryPtr queryPtr3;
    QueryPtr queryPtr4;
    QueryPtr queryPtr5;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    query->addQuery(queryPtr4);
    query->addQuery(queryPtr5);
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testAndQueryWithMultiLevel)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11 = createPhraseQuery();
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2(new AndQuery(""));
    QueryPtr queryPtr21;
    QueryPtr queryPtr22;
    queryPtr2->addQuery(queryPtr21);
    queryPtr2->addQuery(queryPtr22);
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testAndQueryWithoutChild)
{
    AndQuery *query = new AndQuery("");
    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testMultiOrQueryWithStopWord)
{
    OrQuery *query = new OrQuery("");
    QueryPtr queryPtr1 = createPhraseQuery();
    QueryPtr queryPtr2;
    QueryPtr queryPtr3 = createPhraseQuery();
    QueryPtr queryPtr4;
    QueryPtr queryPtr5 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    query->addQuery(queryPtr4);
    query->addQuery(queryPtr5);
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    OrQuery *typeQuery = dynamic_cast<OrQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    const std::vector<QueryPtr>* childQuery = typeQuery->getChildQuery();
    ASSERT_EQ(size_t(3),childQuery->size());
    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testMultiOrQueryLabelWithStopWord)
{
    OrQuery *query = new OrQuery("");
    QueryPtr queryPtr1 = createPhraseQuery();
    RequiredFields requiredField;
    string label("testLabelName");
    QueryPtr queryPtr2(new TermQuery(
                    Term(Token("stop", 0, "stop", true), "default",
                            requiredField), label));
    QueryPtr queryPtr3 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    OrQuery *typeQuery = dynamic_cast<OrQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    const std::vector<QueryPtr>* childQuery = typeQuery->getChildQuery();
    ASSERT_EQ(size_t(2),childQuery->size());
    ASSERT_EQ("", typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_NONE, typeQuery->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("", (*resultQuery->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_TERM, (*resultQuery->getChildQuery())[1]->getMatchDataLevel());

    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testMultiOrQueryWithAllStopWord)
{
    OrQuery *query = new OrQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2;
    QueryPtr queryPtr3;
    QueryPtr queryPtr4;
    QueryPtr queryPtr5;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);
    query->addQuery(queryPtr4);
    query->addQuery(queryPtr5);
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testOrQueryWithMultiLevel)
{
    string label("testLabelName");
    OrQuery *query = new OrQuery(label);
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11 = createPhraseQuery();
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2(new OrQuery(""));
    QueryPtr queryPtr21;
    QueryPtr queryPtr22;
    queryPtr2->addQuery(queryPtr21);
    queryPtr2->addQuery(queryPtr22);
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testOrQueryWithMultiLevelHas2Labels)
{
    string label1("testLabelName1");
    string label2("testLabelName2");
    OrQuery *query = new OrQuery(label1);
    QueryPtr queryPtr1 = createPhraseQuery();
    queryPtr1->setQueryLabelWithDefaultLevel(label2);
    QueryPtr queryPtr2;
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);

    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label2, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testOrQueryWithoutChild)
{
    OrQuery *query = new OrQuery("");
    _nullQueryCleaner->visitOrQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testAndNotQueryWithoutRightChildAfterSteal)
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1 = createPhraseQuery();
    QueryPtr queryPtr2;
    QueryPtr queryPtr3 = createStopWordTermQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    delete query;
    delete typeQuery;

}

TEST_F(StopWordsCleanerTest, testAndNotQueryLeftChildWithStopWord)
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1 = createStopWordTermQuery();
    QueryPtr queryPtr2 = createPhraseQuery();
    QueryPtr queryPtr3 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    ASSERT_EQ(ERROR_QUERY_INVALID,
                         _nullQueryCleaner->getErrorCode());
    ASSERT_EQ(string("AndNotQuery or RankQuery's leftQuery " 
                                "is NULL or stop word.")
                         , _nullQueryCleaner->getErrorMsg());
    delete query;
}

TEST_F(StopWordsCleanerTest, testAndNotQueryWithoutLeftChildAfterSteal)
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2 = createPhraseQuery();
    QueryPtr queryPtr3 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    ASSERT_EQ(ERROR_QUERY_INVALID,
                         _nullQueryCleaner->getErrorCode());
    ASSERT_EQ(string("AndNotQuery or RankQuery's leftQuery " 
                                "is NULL or stop word.")
                         , _nullQueryCleaner->getErrorMsg());
    delete query;
}
TEST_F(StopWordsCleanerTest, testAndNotQueryWithMultiLevel)
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11;
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2 = createPhraseQuery();
    QueryPtr queryPtr3 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testAndNotQueryWithoutChild)
{
    AndNotQuery *query = new AndNotQuery("");
    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testRankQueryWithoutRightChildAfterSteal)
{
    string label("testLabelName");
    RankQuery *query = new RankQuery(label);
    QueryPtr queryPtr1 = createPhraseQuery();
    QueryPtr queryPtr2;
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testRankQueryWithoutLeftChildAfterSteal)
{
    RankQuery *query = new RankQuery("");
    QueryPtr queryPtr1;
    QueryPtr queryPtr2 = createPhraseQuery();
    QueryPtr queryPtr3 = createPhraseQuery();

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}
TEST_F(StopWordsCleanerTest, testRankQueryWithMultiLevel)
{
    RankQuery *query = new RankQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11 = createPhraseQuery();
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2;
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    delete query;
    delete typeQuery;
}
TEST_F(StopWordsCleanerTest, testRankQueryWithoutChild)
{
    RankQuery *query = new RankQuery("");
    _nullQueryCleaner->visitRankQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
    delete query;
}

TEST_F(StopWordsCleanerTest, testAndNotRankQuery)
{
    AndNotQuery *query = new AndNotQuery("");
    QueryPtr queryPtr1(new RankQuery(""));
    QueryPtr queryPtr11 = createPhraseQuery();
    QueryPtr queryPtr12;
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2;
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndNotQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    PhraseQuery *typeQuery = dynamic_cast<PhraseQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testTermQueryWithStopWord) 
{
    QueryPtr queryPtr1 = createStopWordTermQuery();
    _nullQueryCleaner->visitTermQuery((TermQuery*)(queryPtr1.get()));
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
}

TEST_F(StopWordsCleanerTest, testTermQueryNormal) 
{
    QueryPtr queryPtr1 = createTermQuery();
    _nullQueryCleaner->visitTermQuery((TermQuery*)(queryPtr1.get()));
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    delete resultQuery;
}

TEST_F(StopWordsCleanerTest, testPhraseQueryNormal)
{
    QueryPtr queryPtr1 = createPhraseQuery();
    _nullQueryCleaner->visitPhraseQuery((PhraseQuery*)(queryPtr1.get()));
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    delete resultQuery;

}
TEST_F(StopWordsCleanerTest, testPhraseQueryRestOneTerm)
{
    QueryPtr queryPtr1 = createRestOneWordPhraseQuery();
    _nullQueryCleaner->visitPhraseQuery((PhraseQuery*)(queryPtr1.get()));
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    TermQuery *termQuery = dynamic_cast<TermQuery *>(resultQuery); 
    ASSERT_TRUE(termQuery);
    delete termQuery;
}

TEST_F(StopWordsCleanerTest, testPhraseQueryWithAllStopWord)
{
    QueryPtr queryPtr1 = createAllStopWordPhraseQuery();
    _nullQueryCleaner->visitPhraseQuery((PhraseQuery*)(queryPtr1.get()));
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(!resultQuery);
}

TEST_F(StopWordsCleanerTest, testAndQueryWithPhraseQueryRestOneTerm)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11 = createRestOneWordPhraseQuery();
    QueryPtr queryPtr12 = createAllStopWordPhraseQuery();
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    QueryPtr queryPtr2(new AndQuery(""));
    QueryPtr queryPtr21 = createAllStopWordPhraseQuery();
    QueryPtr queryPtr22 = createAllStopWordPhraseQuery();
    queryPtr2->addQuery(queryPtr21);
    queryPtr2->addQuery(queryPtr22);
    QueryPtr queryPtr3;

    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    TermQuery *typeQuery = dynamic_cast<TermQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    delete query;
    delete typeQuery;
}

TEST_F(StopWordsCleanerTest, testAndMultiTermQuery)
{
    AndQuery *query = new AndQuery("");
    QueryPtr queryPtr1(new AndQuery(""));
    QueryPtr queryPtr11 = createStopWordTermQuery();
    QueryPtr queryPtr12 = createStopWordTermQuery();
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    string label("testLabelName");
    QueryPtr queryPtr2(new AndQuery(label));
    QueryPtr queryPtr21 = createStopWordTermQuery();
    QueryPtr queryPtr22 = createStopWordTermQuery();
    QueryPtr queryPtr23 = createStopWordTermQuery();
    QueryPtr queryPtr24 = createStopWordTermQuery();
    QueryPtr queryPtr25 = createTermQuery();
    queryPtr2->addQuery(queryPtr21);
    queryPtr2->addQuery(queryPtr22);
    queryPtr2->addQuery(queryPtr23);
    queryPtr2->addQuery(queryPtr24);
    queryPtr2->addQuery(queryPtr25);
    
    QueryPtr queryPtr3;
    
    query->addQuery(queryPtr1);
    query->addQuery(queryPtr2);
    query->addQuery(queryPtr3);

    _nullQueryCleaner->visitAndQuery(query);
    Query *resultQuery = _nullQueryCleaner->stealQuery();
    ASSERT_TRUE(resultQuery);
    TermQuery *typeQuery = dynamic_cast<TermQuery *> (resultQuery);
    ASSERT_TRUE(typeQuery);
    ASSERT_EQ(label, typeQuery->getQueryLabel());
    ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());

    delete query;
    delete typeQuery;

}


TEST_F(StopWordsCleanerTest, testMultiTermQueryOp)
{
    {
        string label("testLabelName");
        MultiTermQuery *query = new MultiTermQuery(label);
        query->addTerm(createNormalTerm());
        query->addTerm(createStopWordTerm());
        _nullQueryCleaner->visitMultiTermQuery(query);
        Query *resultQuery = _nullQueryCleaner->stealQuery();
        ASSERT_TRUE(resultQuery);
        ASSERT_EQ(resultQuery->toString(), "TermQuery@testLabelName:[Term:[default||abc|100|]]");
        TermQuery *typeQuery = dynamic_cast<TermQuery *> (resultQuery);
        ASSERT_TRUE(typeQuery);
        ASSERT_EQ(label, typeQuery->getQueryLabel());
        ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());
        delete query;
        delete typeQuery;
    }

    {
        string label("testLabelName");
        MultiTermQuery *query = new MultiTermQuery(label);
        query->addTerm(createStopWordTerm());
        query->addTerm(createNormalTerm());
        _nullQueryCleaner->visitMultiTermQuery(query);
        Query *resultQuery = _nullQueryCleaner->stealQuery();
        ASSERT_TRUE(resultQuery);
        ASSERT_EQ(resultQuery->toString(), "TermQuery@testLabelName:[Term:[default||abc|100|]]");
        TermQuery *typeQuery = dynamic_cast<TermQuery *> (resultQuery);
        ASSERT_TRUE(typeQuery);
        ASSERT_EQ(label, typeQuery->getQueryLabel());
        ASSERT_EQ(MDL_TERM, typeQuery->getMatchDataLevel());
        delete query;
        delete typeQuery;
    }

    {
        MultiTermQuery *query = new MultiTermQuery("");
        query->addTerm(createStopWordTerm());
        query->addTerm(createStopWordTerm());
        _nullQueryCleaner->visitMultiTermQuery(query);
        Query *resultQuery = _nullQueryCleaner->stealQuery();
        ASSERT_TRUE(!resultQuery);
        delete query;
    }

    {
        string label("testLabelName");
        MultiTermQuery *query = new MultiTermQuery(label);
        query->addTerm(createNormalTerm());
        query->addTerm(createNormalTerm());
        _nullQueryCleaner->visitMultiTermQuery(query);
        Query *resultQuery = _nullQueryCleaner->stealQuery();
        ASSERT_TRUE(resultQuery);
        ASSERT_EQ(resultQuery->toString(), "MultiTermQuery@testLabelName:[Term:[default||abc|100|], Term:[default||abc|100|], ]");
        ASSERT_EQ(label, resultQuery->getQueryLabel());
        ASSERT_EQ(MDL_TERM, resultQuery->getMatchDataLevel());
        delete resultQuery;
        delete query;
    }

    {
        MultiTermQuery *query = new MultiTermQuery("");
        query->addTerm(createNormalTerm());
        query->addTerm(createStopWordTerm());
        query->addTerm(createNormalTerm());
        query->addTerm(createStopWordTerm());
        _nullQueryCleaner->visitMultiTermQuery(query);
        Query *resultQuery = _nullQueryCleaner->stealQuery();
        ASSERT_TRUE(resultQuery);
        ASSERT_EQ(resultQuery->toString(), "MultiTermQuery:[Term:[default||abc|100|], Term:[default||abc|100|], ]");

        delete resultQuery;
        delete query;
    }
}

QueryPtr StopWordsCleanerTest::createAllStopWordPhraseQuery() {
    PhraseQuery *query1 = new PhraseQuery("");
    QueryPtr queryPtr1(query1);
    RequiredFields requiredField;
    query1->addTerm(TermPtr(new Term(Token("stop", 0, "stop", true),
                         "default", requiredField)));
    query1->addTerm(TermPtr(new Term(Token("word", 1, "word", true), 
                         "default", requiredField)));
    return queryPtr1;
}

QueryPtr StopWordsCleanerTest::createRestOneWordPhraseQuery() {
    PhraseQuery *query1 = new PhraseQuery("");
    QueryPtr queryPtr1(query1);
    RequiredFields requiredField;
    query1->addTerm(TermPtr(new Term(Token("stop", 0, "stop", true), 
                         "default", requiredField)));
    query1->addTerm(TermPtr(new Term(Token("abc", 1, "abc", false), 
                         "default", requiredField)));
    return queryPtr1;
}
QueryPtr StopWordsCleanerTest::createPhraseQuery() {
    PhraseQuery *query1 = new PhraseQuery("");
    QueryPtr queryPtr1(query1);
    RequiredFields requiredField;
    query1->addTerm(TermPtr(new Term(Token("def", 0, "def", false),
                         "default", requiredField)));
    query1->addTerm(TermPtr(new Term(Token("abc", 1, "abc", false),
                         "default", requiredField)));
    return queryPtr1;
}

QueryPtr StopWordsCleanerTest::createStopWordTermQuery() {
    RequiredFields requiredField;
    QueryPtr queryPtr1(new TermQuery(Term(Token("stop", 0, "stop", true),
                            "default", requiredField), ""));
    return queryPtr1;
}

QueryPtr StopWordsCleanerTest::createTermQuery() {
    RequiredFields requiredField;
    QueryPtr queryPtr1(new TermQuery(Term(Token("abc", 0, "abc", false), 
                            "default", requiredField), ""));
    return queryPtr1;
}

TermPtr StopWordsCleanerTest::createNormalTerm() {
    RequiredFields requiredField;
    TermPtr term(new Term(Token("abc", 0, "abc", false), 
                            "default", requiredField));
    return term;
}

TermPtr StopWordsCleanerTest::createStopWordTerm() {
    RequiredFields requiredField;
    TermPtr term(new Term(Token("stop", 0, "stop", true), 
                            "default", requiredField));
    return term;
}
END_HA3_NAMESPACE(qrs);

