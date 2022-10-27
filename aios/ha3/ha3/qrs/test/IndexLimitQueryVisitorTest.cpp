#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/IndexLimitQueryVisitor.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(qrs);

class IndexLimitQueryVisitorTest : public TESTBASE {
public:
    IndexLimitQueryVisitorTest();
    ~IndexLimitQueryVisitorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalQueryTest(const std::string &queryStr, 
                           const std::string &expectStr, 
                           const std::string &expectSubStr);  

protected:
    std::string _defaultIndex;
    std::string _indexName;
    std::string _termName;
protected:

    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, IndexLimitQueryVisitorTest);

IndexLimitQueryVisitorTest::IndexLimitQueryVisitorTest() { 
}

IndexLimitQueryVisitorTest::~IndexLimitQueryVisitorTest() { 
}

void IndexLimitQueryVisitorTest::setUp() {
    _defaultIndex = "default";
    _indexName = "default";
    _termName = "error";
}

void IndexLimitQueryVisitorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}


TEST_F(IndexLimitQueryVisitorTest, testPhraseQuery){
    internalQueryTest("\"aaa bbb\"", "\"default:aaa default:bbb\"", "");
    internalQueryTest("\"aaa aaa bbb\"", "\"default:aaa default:aaa default:bbb\"", "");
    internalQueryTest("\"error bbb\"", "\"default:error default:bbb\"", "\"default:error default:bbb\"");
}

TEST_F(IndexLimitQueryVisitorTest, testTermQuery){
    internalQueryTest("aaa", "default:aaa", "");
    internalQueryTest("default:aaa", "default:aaa", "");
    internalQueryTest("noindex:aaa", "noindex:aaa", "");
    internalQueryTest("default:error", "default:error", "default:error");
}

TEST_F(IndexLimitQueryVisitorTest, testAndNotQuery){
    internalQueryTest("aaa ANDNOT bbb", "((default:aaa) ANDNOT (default:bbb))", "");
    internalQueryTest("aaa ANDNOT error", "((default:aaa) ANDNOT (default:error))","((default:aaa) ANDNOT (default:error))" );
//    internalQueryTest("aaa ANDNOT noindex::bbb", "");
}

TEST_F(IndexLimitQueryVisitorTest, testNumberQuery){
    internalQueryTest("1", "default:1", "");
    internalQueryTest("noindex:1", "noindex:1", "");
}

TEST_F(IndexLimitQueryVisitorTest, testANDQuery){
    internalQueryTest("aaa AND bbb", "(default:aaa AND default:bbb)", "");
    internalQueryTest("aaa AND noindex:bbb", "(default:aaa AND noindex:bbb)", "");
    internalQueryTest("aaa error", "(default:aaa AND default:error)", "(default:aaa AND default:error)");
}

TEST_F(IndexLimitQueryVisitorTest, testOrQuery){
    internalQueryTest("aaa OR bbb", "(default:aaa OR default:bbb)", "");
    internalQueryTest("error OR bbb", "(default:error OR default:bbb)", "(default:error OR default:bbb)");
    internalQueryTest("noindex:aaa OR bbb", "(noindex:aaa OR default:bbb)", "");
}

TEST_F(IndexLimitQueryVisitorTest, testMultiTermQuery){
    internalQueryTest("default:'aaa' | 'bbb'", "\"default:aaa default:bbb\"", "");
    internalQueryTest("default:'error' | 'bbb'", "\"default:error default:bbb\"",
                      "\"default:error default:bbb\"");
    internalQueryTest("default:'aaa' & 'bbb'", "\"default:aaa default:bbb\"", "");
    internalQueryTest("default:'aaa' & 'error'", "\"default:aaa default:error\"",
                      "\"default:aaa default:error\"");
}

TEST_F(IndexLimitQueryVisitorTest, testRankQuery){
    internalQueryTest("aaa RANK bbb", "(default:aaa OR default:bbb)", "");
}

TEST_F(IndexLimitQueryVisitorTest, testComplexQuery){
    internalQueryTest("cc AND (aa OR bb OR \"aaa bbb\") AND phrase:aaaa AND noindex:ccc", "(default:cc AND (default:aa OR default:bb OR \"default:aaa default:bbb\") AND phrase:aaaa AND noindex:ccc)", "");
    internalQueryTest("cc AND (aa OR error \"aaa bbb\") AND phrase:aaaa AND noindex:ccc", "(default:cc AND (default:aa OR default:error) AND \"default:aaa default:bbb\" AND phrase:aaaa AND noindex:ccc)", "(default:aa OR default:error)");
    internalQueryTest("aaa ANDNOT error ccc", "(((default:aaa) ANDNOT (default:error)) AND default:ccc)","((default:aaa) ANDNOT (default:error))" );
}



void IndexLimitQueryVisitorTest::internalQueryTest(const string& queryStr,
        const string& expectStr, const string& expectSubStr) 
{
    QueryPtr query(SearcherTestHelper::createQuery(queryStr, 0, _defaultIndex));
    if(!query){
        ASSERT_TRUE(false);
    }
    IndexLimitQueryVisitor queryVisitor(_indexName, _termName);
    query->accept(&queryVisitor);
    string extractQueryStr = queryVisitor.getQueryStr();
    string extractSubStr = queryVisitor.getSubQueryStr();
    ASSERT_EQ(expectStr, extractQueryStr);
    ASSERT_EQ(expectSubStr, extractSubStr);
}


END_HA3_NAMESPACE(qrs);

