#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryTermVisitor.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(common);

class QueryTermVisitorTest : public TESTBASE {
public:
    QueryTermVisitorTest();
    ~QueryTermVisitorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalExtractQueryTest(const std::string &queryStr, const std::string &termsStr, QueryTermVisitor::VisitTermType visitType = QueryTermVisitor::VT_RANK_QUERY);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, QueryTermVisitorTest);


QueryTermVisitorTest::QueryTermVisitorTest() { 
}

QueryTermVisitorTest::~QueryTermVisitorTest() { 
}

void QueryTermVisitorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QueryTermVisitorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryTermVisitorTest, testSimpleProcess) { 
    internalExtractQueryTest("aaa", "aaa");
}

TEST_F(QueryTermVisitorTest, testExtractPhraseQuery) { 
    internalExtractQueryTest("\"aaa bbb\"", "aaa,bbb");
}

TEST_F(QueryTermVisitorTest, testExtractAndNotQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    internalExtractQueryTest("\"a aa\" ANDNOT bbb", "a,aa");
}

TEST_F(QueryTermVisitorTest, testExtractAndQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    internalExtractQueryTest("aaa AND bbb", "aaa,bbb");
}

TEST_F(QueryTermVisitorTest, testMultiTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    internalExtractQueryTest("default:'aaa' & 'bbb'", "aaa,bbb");
    internalExtractQueryTest("default:'aaa' | 'bbb'", "aaa,bbb");
}

TEST_F(QueryTermVisitorTest, testExtractComplicatedQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");

    internalExtractQueryTest("(aaa OR bbb) AND (\"c cc\" RANK ddd)", "aaa,bbb,c,cc,ddd");
}

TEST_F(QueryTermVisitorTest, testVisitTermType) {
    HA3_LOG(DEBUG, "Begin Test!");

    internalExtractQueryTest("(\"a aa\" RANK b) OR (c ANDNOT bbb)", "a,aa,c,bbb", QueryTermVisitor::VT_ANDNOT_QUERY);
    internalExtractQueryTest("(\"a aa\" RANK b) OR (c ANDNOT bbb)", "a,aa,b,c", QueryTermVisitor::VT_RANK_QUERY);
    internalExtractQueryTest("(\"a aa\" RANK b) OR (c ANDNOT bbb)", "a,aa,b,c,bbb", QueryTermVisitor::VT_ANDNOT_QUERY | QueryTermVisitor::VT_RANK_QUERY);
    internalExtractQueryTest("(\"a aa\" RANK b) OR (c ANDNOT bbb)", "a,aa,c", QueryTermVisitor::VT_NO_AUX_TERM);
} 

void QueryTermVisitorTest::internalExtractQueryTest(const string &queryStr,
        const string &termsStr, QueryTermVisitor::VisitTermType type) 
{
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));

    QueryTermVisitor termVisitor(type);
    query->accept(&termVisitor);
    const TermVector &terms = termVisitor.getTermVector();

    StringTokenizer st(termsStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ((size_t)st.getNumTokens(), terms.size());

    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        ASSERT_EQ(st[i], terms[i].getWord());
    }
}


END_HA3_NAMESPACE(common);

