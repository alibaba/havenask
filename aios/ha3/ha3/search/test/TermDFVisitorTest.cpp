#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermDFVisitor.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class TermDFVisitorTest : public TESTBASE {
public:
    TermDFVisitorTest();
    ~TermDFVisitorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalTestDF(const std::string &dfMapStr,
                        const std::string &queryStr,
                        df_t expectDF);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, TermDFVisitorTest);


TermDFVisitorTest::TermDFVisitorTest() { 
}

TermDFVisitorTest::~TermDFVisitorTest() { 
}

void TermDFVisitorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TermDFVisitorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TermDFVisitorTest, testSimpleProcess) { 
    internalTestDF("a:10,b:20,c:30", "a", 10);
    internalTestDF("a:10,b:20,c:30", "a AND a", 10);
    internalTestDF("a:10,b:20,c:30", "c AND b", 20);
    internalTestDF("a:10,b:20,c:30", "c OR b", 50);
    internalTestDF("a:10,b:20,c:30", "a AND (b OR c)", 10);
    internalTestDF("a:10,b:10,c:30", "c AND (b OR a)", 20);
    internalTestDF("a:10,b:20,c:30", "b RANK a RANK b", 20);
    internalTestDF("a:10,b:20,c:30", "a ANDNOT b", 10);
    internalTestDF("a:10,b:20,c:30", "c ANDNOT a", 30);
    internalTestDF("a:30,b:20,c:30,d:15,f:13,g:1,h:3", "(a AND b AND c AND (d OR f OR g)) OR h", 23);
    internalTestDF("a:30,b:20,c:30,d:15,f:13,g:1,h:3", "a ANDNOT g ANDNOT h ANDNOT d", 30);
    internalTestDF("a:10,b:20,c:30", "\"a b c\"", 10);
    internalTestDF("1:20,a:10,c:5", "(1 AND a) OR c", 15);
    internalTestDF("1:20,a:10,c:10", "1", 20);
    internalTestDF("a:10,b:20,c:30", "default:'c' | 'b'", 50);
    internalTestDF("a:10,b:20,c:30", "default:'c' & 'b'", 20);
}

void TermDFVisitorTest::internalTestDF(const string &dfMapStr,
                                       const string &queryStr,
                                       df_t expectDF)
{
    TermDFMap termDFMap = SearcherTestHelper::createTermDFMap(dfMapStr);

    TermDFVisitor visitor(termDFMap);
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
    query->accept(&visitor);
    HA3_LOG(INFO, "query is [%s]", queryStr.c_str());
    assert(expectDF == visitor.getDF());
}

END_HA3_NAMESPACE(search);

