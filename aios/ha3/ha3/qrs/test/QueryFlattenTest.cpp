#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QueryFlatten.h>
#include <ha3/common/AndQuery.h>
#include <ha3/qrs/QueryFlatten.h>
#include <autil/StringTokenizer.h>
#include <ha3/qrs/test/QueryMaker.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

class QueryFlattenTest : public TESTBASE {
public:
    QueryFlattenTest();
    ~QueryFlattenTest();
public:
    void setUp();
    void tearDown();
protected:
    template <typename QueryType>
    QueryType* createBinaryQuery(const char *words);

    template <typename QueryType>
    QueryType* packageBinaryQuery(common::QueryPtr theFirstChildQueryPtr,
                                  const char *word);
protected:
    QueryFlatten *_queryFlatten;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QueryFlattenTest);


QueryFlattenTest::QueryFlattenTest() {
    _queryFlatten = new QueryFlatten();
}

QueryFlattenTest::~QueryFlattenTest() {
    DELETE_AND_SET_NULL(_queryFlatten);
}

void QueryFlattenTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void QueryFlattenTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryFlattenTest, testSimpleProcess) {
    //flatten two AndQuery
    HA3_LOG(DEBUG, "Begin Test!");

    QueryPtr andQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr originalQueryPtr(QueryMaker::packageBinaryQuery<AndQuery>(andQueryPtr, "C"));
    string label("testLabelName");
    originalQueryPtr->setQueryLabel(label);
    originalQueryPtr->accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B,C"));

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
    ASSERT_EQ(label, flattenQueryPtr->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenAndOrQueryWithoutChange) {
    HA3_LOG(DEBUG, "Begin Test!");

    AndQuery originalQuery("");
    QueryPtr orQueryPtr1(QueryMaker::createBinaryQuery<OrQuery>("A,B"));
    QueryPtr orQueryPtr2(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    originalQuery.addQuery(orQueryPtr1);
    originalQuery.addQuery(orQueryPtr2);
    string label("testLabelName");
    originalQuery.setQueryLabel(label);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    AndQuery expectedQuery("");
    QueryPtr expectedOrQueryPtr1(QueryMaker::createBinaryQuery<OrQuery>("A,B"));
    QueryPtr expectedOrQueryPtr2(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    expectedQuery.addQuery(expectedOrQueryPtr1);
    expectedQuery.addQuery(expectedOrQueryPtr2);

    ASSERT_TRUE(expectedQuery == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenFiveAndQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    AndQuery originalQuery("");
    QueryPtr andQueryPtr1(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr andQueryPtr2(QueryMaker::packageBinaryQuery<AndQuery>(andQueryPtr1, "C"));
    QueryPtr andQueryPtr3(QueryMaker::packageBinaryQuery<AndQuery>(andQueryPtr2, "D"));

    QueryPtr andQueryPtr4(QueryMaker::createBinaryQuery<AndQuery>("E,F"));
    originalQuery.addQuery(andQueryPtr3);
    originalQuery.addQuery(andQueryPtr4);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B,C,D,E,F"));
    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenJustLeft) {
    HA3_LOG(DEBUG, "Begin Test!");

    AndQuery originalQuery("");
    QueryPtr andQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr orQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(orQueryPtr);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr expectedOrQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    expectedQueryPtr->addQuery(expectedOrQueryPtr);

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenJustLeftWithLabel) {
    AndQuery originalQuery("");
    QueryPtr andQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr orQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    string label("testLabelName");
    andQueryPtr->setQueryLabelWithDefaultLevel(label);
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(orQueryPtr);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);
    ASSERT_TRUE(originalQuery == *flattenQueryPtr);
    ASSERT_EQ(label, (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label,
              (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenJustRight) {
    HA3_LOG(DEBUG, "Begin Test!");

    OrQuery originalQuery("");
    QueryPtr andQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr orQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(orQueryPtr);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedAndQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr expectedQueryPtr(QueryMaker::packageBinaryQuery<OrQuery>(expectedAndQueryPtr, "C,D"));

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenJustRightWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");

    OrQuery originalQuery("");
    QueryPtr andQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr orQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("C,D"));
    string label("testLabelName");
    orQueryPtr->setQueryLabelWithDefaultLevel(label);
    originalQuery.addQuery(andQueryPtr);
    originalQuery.addQuery(orQueryPtr);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);
    ASSERT_TRUE(originalQuery == *flattenQueryPtr);
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[1]->getMatchDataLevel());
}

TEST_F(QueryFlattenTest, testFlattenAndNot) {
    HA3_LOG(DEBUG, "Begin Test!");

    AndNotQuery originalQuery("");
    QueryPtr andNotQueryPtr1(QueryMaker::createBinaryQuery<AndNotQuery>("A,B"));
    QueryPtr andNotQueryPtr2(QueryMaker::createBinaryQuery<AndNotQuery>("C,D"));
    originalQuery.addQuery(andNotQueryPtr1);
    originalQuery.addQuery(andNotQueryPtr2);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<AndNotQuery>("A,B"));
    QueryPtr expectedAndNotQueryPtr(QueryMaker::createBinaryQuery<AndNotQuery>("C,D"));
    expectedQueryPtr->addQuery(expectedAndNotQueryPtr);

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenAndNotWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");

    AndNotQuery originalQuery("");
    QueryPtr andNotQueryPtr1(QueryMaker::createBinaryQuery<AndNotQuery>("A,B"));
    QueryPtr andNotQueryPtr2(QueryMaker::createBinaryQuery<AndNotQuery>("C,D"));
    string label("testLabelName");
    andNotQueryPtr1->setQueryLabelWithDefaultLevel(label);
    originalQuery.addQuery(andNotQueryPtr1);
    originalQuery.addQuery(andNotQueryPtr2);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);
    ASSERT_TRUE(originalQuery == *flattenQueryPtr);
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label,
              (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenRank) {
    HA3_LOG(DEBUG, "Begin Test!");

    RankQuery originalQuery("");
    QueryPtr rankQueryPtr1(QueryMaker::createBinaryQuery<RankQuery>("A,B"));
    QueryPtr rankQueryPtr2(QueryMaker::createBinaryQuery<RankQuery>("C,D"));
    originalQuery.addQuery(rankQueryPtr1);
    originalQuery.addQuery(rankQueryPtr2);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<RankQuery>("A,B"));
    QueryPtr expectedRankQueryPtr(QueryMaker::createBinaryQuery<RankQuery>("C,D"));
    expectedQueryPtr->addQuery(expectedRankQueryPtr);

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenRankWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");

    RankQuery originalQuery("");
    QueryPtr rankQueryPtr1(QueryMaker::createBinaryQuery<RankQuery>("A,B"));
    QueryPtr rankQueryPtr2(QueryMaker::createBinaryQuery<RankQuery>("C,D"));
    string label("testLabelName");
    rankQueryPtr1->setQueryLabelWithDefaultLevel(label);
    originalQuery.addQuery(rankQueryPtr1);
    originalQuery.addQuery(rankQueryPtr2);

    originalQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);
    ASSERT_TRUE(originalQuery == *flattenQueryPtr);
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label,
              (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenMultiLevel) {
    HA3_LOG(DEBUG, "Begin Test!");
/*********************
L0        and
          / \
L1     and   or
       / \   / \
L2   and  C  D  E
     / \
    A   B
*********************/
    QueryPtr andQueryPtrL2(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr andQueryPtrL1(new AndQuery(""));
    QueryPtr termQueryPtr(new TermQuery("C", "phrase", RequiredFields(), ""));
    andQueryPtrL1->addQuery(andQueryPtrL2);
    andQueryPtrL1->addQuery(termQueryPtr);
    AndQuery rootQuery(""); // L0
    QueryPtr orQueryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    rootQuery.addQuery(andQueryPtrL1);
    rootQuery.addQuery(orQueryPtrL1);

    rootQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedQueryPtr(QueryMaker::createBinaryQuery<AndQuery>("A,B,C"));
    QueryPtr expectedOrQueryPtr(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    expectedQueryPtr->addQuery(expectedOrQueryPtr);

    ASSERT_TRUE(*expectedQueryPtr == *flattenQueryPtr);
}

TEST_F(QueryFlattenTest, testFlattenMultiLevelWithLabelL1) {
/************************************************
L0           and                       and
          /      \                   /     \
L1    and(lab)    or     =>    and(lab)     or
       /     \    / \           / | \       / \
L2  and       C  D   E         A  B  C     D   E
     / \
    A   B
***************************************************/
    QueryPtr andQueryPtrL2(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    string label("testLabelName");
    QueryPtr andQueryPtrL1(new AndQuery(label));
    QueryPtr termQueryPtr(new TermQuery("C", "phrase", RequiredFields(), ""));
    andQueryPtrL1->addQuery(andQueryPtrL2);
    andQueryPtrL1->addQuery(termQueryPtr);
    AndQuery rootQuery(""); // L0
    QueryPtr orQueryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    rootQuery.addQuery(andQueryPtrL1);
    rootQuery.addQuery(orQueryPtrL1);

    rootQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedAndQryPtrL1(QueryMaker::createBinaryQuery<AndQuery>("A,B,C"));
    QueryPtr expectedOrQryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    AndQuery expectedRootQry("");
    expectedRootQry.addQuery(expectedAndQryPtrL1);
    expectedRootQry.addQuery(expectedOrQryPtrL1);
    ASSERT_TRUE(expectedRootQry == *flattenQueryPtr);
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label,
              (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenMultiLevelWithLabelL2) {
/************************************************
L0           and                       and
          /      \                   /   |  \
L1       and      or     =>    and(lab)  C   or
       /     \    / \           /  \        / \
L2  and(lab)  C  D   E         A    B      D   E
     / \
    A   B
***************************************************/
    string label("testLabelName");
    QueryPtr andQueryPtrL2(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    andQueryPtrL2->setQueryLabelWithDefaultLevel(label);
    QueryPtr andQueryPtrL1(new AndQuery(""));
    QueryPtr termQueryPtr(new TermQuery("C", "phrase", RequiredFields(), ""));
    andQueryPtrL1->addQuery(andQueryPtrL2);
    andQueryPtrL1->addQuery(termQueryPtr);
    AndQuery rootQuery(""); // L0
    QueryPtr orQueryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    rootQuery.addQuery(andQueryPtrL1);
    rootQuery.addQuery(orQueryPtrL1);

    rootQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    QueryPtr expectedAndQryPtrL1(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    QueryPtr expectedTermQryPtrL1(new TermQuery("C", "phrase", RequiredFields(), ""));
    QueryPtr expectedOrQryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    AndQuery expectedRootQry("");
    expectedRootQry.addQuery(expectedAndQryPtrL1);
    expectedRootQry.addQuery(expectedTermQryPtrL1);
    expectedRootQry.addQuery(expectedOrQryPtrL1);

    ASSERT_TRUE(expectedRootQry == *flattenQueryPtr);
    ASSERT_EQ(3, (*flattenQueryPtr->getChildQuery()).size());
    ASSERT_EQ(MDL_NONE, flattenQueryPtr->getMatchDataLevel());
    ASSERT_EQ("", flattenQueryPtr->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label, (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_TERM,
              (*flattenQueryPtr->getChildQuery())[1]->getMatchDataLevel());
    ASSERT_EQ("", (*flattenQueryPtr->getChildQuery())[1]->getQueryLabel());
    ASSERT_EQ(MDL_NONE,
              (*flattenQueryPtr->getChildQuery())[2]->getMatchDataLevel());
    ASSERT_EQ("", (*flattenQueryPtr->getChildQuery())[2]->getQueryLabel());
}

TEST_F(QueryFlattenTest, testFlattenMultiLevelWith2Label2) {
/************************************************
L0           and                       and
          /      \                   /     \
L1    and(lab1)   or     =>    and(lab1)    or
       /     \    / \           /  \        / \
L2  and(lab2) C  D   E    and(lab2) C      D   E
     / \                      / \
    A   B                    A   B
***************************************************/
    string label1("testLabelNameL1");
    string label2("testLabelNameL2");
    QueryPtr andQueryPtrL2(QueryMaker::createBinaryQuery<AndQuery>("A,B"));
    andQueryPtrL2->setQueryLabelWithDefaultLevel(label2);
    QueryPtr andQueryPtrL1(new AndQuery(label1));
    QueryPtr termQueryPtr(new TermQuery("C", "phrase", RequiredFields(), ""));
    andQueryPtrL1->addQuery(andQueryPtrL2);
    andQueryPtrL1->addQuery(termQueryPtr);
    AndQuery rootQuery(""); // L0
    QueryPtr orQueryPtrL1(QueryMaker::createBinaryQuery<OrQuery>("D,E"));
    rootQuery.addQuery(andQueryPtrL1);
    rootQuery.addQuery(orQueryPtrL1);

    rootQuery.accept(_queryFlatten);
    QueryPtr flattenQueryPtr(_queryFlatten->stealQuery());
    ASSERT_TRUE(flattenQueryPtr);

    ASSERT_TRUE(rootQuery == *flattenQueryPtr);
    ASSERT_EQ(MDL_NONE, flattenQueryPtr->getMatchDataLevel());
    ASSERT_EQ("", flattenQueryPtr->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY,
              (*flattenQueryPtr->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label1,
              (*flattenQueryPtr->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_SUB_QUERY,
              (*(*flattenQueryPtr->getChildQuery())[0]
               ->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ(label2,
              (*(*flattenQueryPtr->getChildQuery())[0]
               ->getChildQuery())[0]->getQueryLabel());
    ASSERT_EQ(MDL_TERM,
              (*(*flattenQueryPtr->getChildQuery())[1]
               ->getChildQuery())[0]->getMatchDataLevel());
    ASSERT_EQ("",
              (*(*flattenQueryPtr->getChildQuery())[1]
               ->getChildQuery())[0]->getQueryLabel());
}

END_HA3_NAMESPACE(qrs);
