#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/TransAnd2OrVisitor.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);

class TransAnd2OrVisitorTest : public TESTBASE {

public:
    TransAnd2OrVisitorTest();
    ~TransAnd2OrVisitorTest();
public:
    void setUp();
    void tearDown();

protected:
    void internalQueryTest(const std::string& queryStr, const std::string& expectStr);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, TransAnd2OrVisitorTest);


TransAnd2OrVisitorTest::TransAnd2OrVisitorTest() { 
}

TransAnd2OrVisitorTest::~TransAnd2OrVisitorTest() { 
}

void TransAnd2OrVisitorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TransAnd2OrVisitorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TransAnd2OrVisitorTest, testPhraseQuery){
    string label("testLabelName");
    PhraseQuery phraseQuery(label);
    TermPtr t1(new Term("aaa","default",RequiredFields()));
    TermPtr t2(new Term("bbb","default",RequiredFields()));
    phraseQuery.addTerm(t1);
    phraseQuery.addTerm(t2);
    TransAnd2OrVisitor queryVisitor;
    phraseQuery.accept(&queryVisitor);
    QueryPtr transQuery(queryVisitor.stealQuery());
    QueryPtr expQuery(SearcherTestHelper::createQuery("aaa OR bbb"));
    ASSERT_EQ(*transQuery , *expQuery);
    ASSERT_EQ(label, transQuery->getQueryLabel());
}

TEST_F(TransAnd2OrVisitorTest, testTermQuery){
    internalQueryTest("aaa", "aaa");
}

TEST_F(TransAnd2OrVisitorTest, testAndNotQuery){
    internalQueryTest("aaa ANDNOT bbb", "aaa OR bbb");
}

TEST_F(TransAnd2OrVisitorTest, testNumberQuery){
    internalQueryTest("1", "1");
}

TEST_F(TransAnd2OrVisitorTest, testANDQuery){
    internalQueryTest("aaa AND bbb", "aaa OR bbb");
}

TEST_F(TransAnd2OrVisitorTest, testOrQuery){
    internalQueryTest("aaa OR bbb", "aaa OR bbb");
}

TEST_F(TransAnd2OrVisitorTest, testRankQuery){
    internalQueryTest("aaa RANK bbb", "aaa OR bbb");
}


void TransAnd2OrVisitorTest::internalQueryTest(const string& queryStr,
        const string& expectStr)
{
    QueryPtr rawQuery(SearcherTestHelper::createQuery(queryStr));
    string label("testLabelName");
    rawQuery->setQueryLabel(label);
    if(!rawQuery){
        ASSERT_TRUE(false);
    }
    TransAnd2OrVisitor queryVisitor;
    rawQuery->accept(&queryVisitor);
    QueryPtr transQuery (queryVisitor.stealQuery());

    QueryPtr expQuery(SearcherTestHelper::createQuery(expectStr));
//    cout<< expQuery->toString() << "###" << transQuery->toString()<<endl;
    ASSERT_EQ(*transQuery , *expQuery);
    ASSERT_EQ(label, transQuery->getQueryLabel());
}

END_HA3_NAMESPACE(qrs);

