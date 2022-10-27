#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AuxiliaryChainVisitor.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class AuxiliaryChainVisitorTest : public TESTBASE {
public:
    AuxiliaryChainVisitorTest();
    ~AuxiliaryChainVisitorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalAuxChainVisitor(const std::string &queryStr,
                                 const std::string &termDFMapStr,
                                 const std::string &expectQueryStr,
                                 SelectAuxChainType type = SAC_DF_SMALLER);
    void testQueryString(const std::string &queryStr,
                         const std::string &expectResultStr);
protected:
    std::string _auxChainName;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AuxiliaryChainVisitorTest);


AuxiliaryChainVisitorTest::AuxiliaryChainVisitorTest() { 
    _auxChainName = "auxChain";
}

AuxiliaryChainVisitorTest::~AuxiliaryChainVisitorTest() { 
}

void AuxiliaryChainVisitorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AuxiliaryChainVisitorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AuxiliaryChainVisitorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    internalAuxChainVisitor("a AND b AND c", "a:20,b:10,c:7", "a AND b AND c#auxChain", SAC_DF_SMALLER);
    internalAuxChainVisitor("a AND b AND c", "a:20,b:10,c:7", "a#auxChain AND b#auxChain AND c#auxChain", SAC_ALL);
    internalAuxChainVisitor("a AND b", "a:20,b:10", "a#auxChain AND b", SAC_DF_BIGGER);
    internalAuxChainVisitor("a OR b", "a:20,b:10,c:16", "a#auxChain OR b#auxChain");
    internalAuxChainVisitor("a OR b OR c", "a:20,b:10,c:16", "a#auxChain OR b#auxChain OR c#auxChain");
    internalAuxChainVisitor("a RANK b RANK c", "a:20,b:10,c:16", "a#auxChain RANK b RANK c");
    internalAuxChainVisitor("a ANDNOT b ANDNOT c", "a:20,b:10,c:16", "a#auxChain ANDNOT b ANDNOT c");
    internalAuxChainVisitor("a AND (b ANDNOT c ANDNOT d)", "a:20,b:30,c:16,d:10", "a#auxChain AND (b ANDNOT c ANDNOT d)");
    internalAuxChainVisitor("a AND (b ANDNOT c ANDNOT d)", "a:20,b:130,c:16,d:10", "a#auxChain AND (b ANDNOT c ANDNOT d)");
    internalAuxChainVisitor("a AND (b RANK c)", "a:20,b:13,c:16,d:10", "a AND (b#auxChain RANK c)");
    internalAuxChainVisitor("a AND (b RANK c RANK d)", "a:20,b:130,c:16,d:10", "a#auxChain AND (b RANK c RANK d)");
    internalAuxChainVisitor("(a OR b) AND ((b OR c) RANK d)", "a:20,b:13,c:16,d:10", "(a OR b) AND ((b#auxChain OR c#auxChain) RANK d)");
    internalAuxChainVisitor("\"a b c\" AND d", "a:20,b:13,c:16,d:10", "\"a b c\"#auxChain AND d#auxChain", SAC_ALL);
    internalAuxChainVisitor("1", "a:20,b:13,c:16,d:10", "1#auxChain", SAC_ALL);
    internalAuxChainVisitor("a", "a:20,b:13,c:16,d:10", "a#auxChain", SAC_ALL);
    internalAuxChainVisitor("\"a b\"", "a:20,b:13,c:16,d:10", "\"a b\"#auxChain", SAC_ALL);
    internalAuxChainVisitor("1 AND a", "a:20,b:13,c:16,d:10", "1#auxChain AND a#auxChain", SAC_ALL);

    internalAuxChainVisitor("default:'a'&'b'&'c'", "a:20,b:10,c:5", "default:a#auxChain & b & c", SAC_DF_BIGGER);
    internalAuxChainVisitor("default:'a'&'b'", "a:20,b:10", "default:'a'#auxChain & 'b'", SAC_DF_BIGGER);
    internalAuxChainVisitor("default:'a'&'b'&'c'", "a:20,b:10,c:7", "default:'a'&'b'&'c'#auxChain",
                            SAC_DF_SMALLER);
    internalAuxChainVisitor("default:'a'|'b'|'c'", "a:20,b:10,c:16", "default:'a'#auxChain|'b'#auxChain|'c'#auxChain");
    testQueryString("default:a#auxChain|b|c", "MultiTermQuery:[Term:[default||a|100|auxChain], Term:[default||b|100|], Term:[default||c|100|], ]");
    testQueryString("default:a|b#auxChain|c", "MultiTermQuery:[Term:[default||a|100|], Term:[default||b|100|auxChain], Term:[default||c|100|], ]");

}

void AuxiliaryChainVisitorTest::internalAuxChainVisitor(
        const string &queryStr, const string &termDFMapStr,
        const string &expectQueryStr, SelectAuxChainType type)
{
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
    TermDFMap termDFMap = SearcherTestHelper::createTermDFMap(termDFMapStr);
    AuxiliaryChainVisitor visitor(_auxChainName, termDFMap, type);
    query->accept(&visitor);
    unique_ptr<Query> expectQuery(SearcherTestHelper::createQuery(expectQueryStr));
    ASSERT_EQ(*expectQuery, *query)<<queryStr;
}

void AuxiliaryChainVisitorTest::testQueryString(
        const string &queryStr, const string &expectResultStr)
{
    unique_ptr<Query> expectQuery(SearcherTestHelper::createQuery(queryStr));
    ASSERT_EQ(expectQuery->toString(), expectResultStr);
}

END_HA3_NAMESPACE(search);

