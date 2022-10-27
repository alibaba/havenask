#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/queryparser/test/TableInfoCreator.h>
#include <string>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(queryparser);

class SyntaxExprBisonParserTest : public TESTBASE {
public:
    SyntaxExprBisonParserTest();
    ~SyntaxExprBisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    std::string parseFilter(const char *filterString);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, SyntaxExprBisonParserTest);


SyntaxExprBisonParserTest::SyntaxExprBisonParserTest() { 
}

SyntaxExprBisonParserTest::~SyntaxExprBisonParserTest() { 
}

void SyntaxExprBisonParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SyntaxExprBisonParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SyntaxExprBisonParserTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseSyntaxExpr("a > b"));
    SyntaxExpr *syntaxExpr = ctx.stealSyntaxExpr();
    unique_ptr<SyntaxExpr> syntaxExpr_ptr(syntaxExpr);
    ASSERT_TRUE(syntaxExpr);

    string resultString = syntaxExpr->getExprString();
    
    SyntaxExpr *syntaxExpr1 = new AtomicSyntaxExpr("a",
            vt_unknown, ATTRIBUTE_NAME);
    SyntaxExpr *syntaxExpr2 = new AtomicSyntaxExpr("b",
            vt_unknown, ATTRIBUTE_NAME);
    GreaterSyntaxExpr greaterSyntaxExpr(syntaxExpr1, syntaxExpr2);

    string expectString = greaterSyntaxExpr.getExprString();

    ASSERT_EQ(expectString, resultString);
}

TEST_F(SyntaxExprBisonParserTest, testNoRelationalOperator) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = "a";

    ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseSyntaxExpr(actual.c_str()));
    SyntaxExpr* syntaxExpr = ctx.stealSyntaxExpr();
    ASSERT_TRUE(syntaxExpr);
    delete syntaxExpr;
}

#define CHECK_PARSE_HELPER(expect, oriStr) do {                         \
        ASSERT_EQ(string(expect), parseFilter(oriStr));      \
        ASSERT_EQ(string(expect), parseFilter(expect));      \
    } while (0)                                                         \
        
TEST_F(SyntaxExprBisonParserTest, testOperatorPriority) {
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("((a&1)=0)", "a & 1 = 0");
}

TEST_F(SyntaxExprBisonParserTest, testAndFilter) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("(((a+b)<c) AND (d=10))", "a+b<c AND d=10");
    CHECK_PARSE_HELPER("(foo(a) AND foo(b))", "foo(a) AND foo(b)");
}

TEST_F(SyntaxExprBisonParserTest, testHexNumber) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("(((a+0X20)<-0xabf1AF0) AND (d=0x10))", "a + 0X20 < -0xabf1AF0 AND d = 0x10");
}

TEST_F(SyntaxExprBisonParserTest, testNegativeNumber) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("(((a+-0X20)<c) AND (d=-1.3))", "a + -0X20 < c AND d = -1.3");
}

TEST_F(SyntaxExprBisonParserTest, testAndOrFilter) { 
    HA3_LOG(DEBUG, "Begin Test!");
    CHECK_PARSE_HELPER("(((a+b)<c) AND ((d=10) OR ((a-d)>=20)))", "a + b < c AND (d = 10 OR a - d >= 20)");
}

TEST_F(SyntaxExprBisonParserTest, testBitFilter) { 
    HA3_LOG(DEBUG, "Begin Test!");
    CHECK_PARSE_HELPER("(a|(b^(c&d)))", "a | b ^ c & d");
}

TEST_F(SyntaxExprBisonParserTest, testBasicFunction) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("foo(a)", "foo(a)");
    CHECK_PARSE_HELPER("foo(a , b , c , d , e)", "foo(a,b ,c, d , e)");
    CHECK_PARSE_HELPER("foo((a+b) , c)", "foo(a+b,c)");
    CHECK_PARSE_HELPER("foo((a+b) , (c+d))", "foo(a+b,c+d)");
    CHECK_PARSE_HELPER("foo(b , (c+d))", "foo(b,c+d)");
    CHECK_PARSE_HELPER("(a+foo(b , c))", "a + foo(b,c)");
    CHECK_PARSE_HELPER("foo(((a!=d) AND (b>=f)) , c)", "foo(a !=d AND b >= f,c)");
    CHECK_PARSE_HELPER("(foo(a , b)>c)", "foo(a,b)>c");
    CHECK_PARSE_HELPER("((d+foo(a , b))>c)", "d+foo(a,b)>c");
    CHECK_PARSE_HELPER("foo((a>=b) , c)", "foo(a >= b , c)");
    CHECK_PARSE_HELPER("foo(((a!=b) AND (c=d)) , (e>f) , (g+h))", "foo(a != b AND c = d, e > f, g + h)");
    CHECK_PARSE_HELPER("foo(a)", "(foo(a))");
    CHECK_PARSE_HELPER("foo(a)", "((foo(a)))");
}

TEST_F(SyntaxExprBisonParserTest, testConstValue) {
    HA3_LOG(DEBUG, "Begin Test!");
    CHECK_PARSE_HELPER("1", "1");
    CHECK_PARSE_HELPER("1.0", "1.0");
    CHECK_PARSE_HELPER("-1.0", "-1.0");
    CHECK_PARSE_HELPER("-0xaB", "-0xaB");
    CHECK_PARSE_HELPER("-0Xab", "-0Xab");
    CHECK_PARSE_HELPER("\"-0Xab\"", "\"-0Xab\"");
    CHECK_PARSE_HELPER("\"\\\"\"", "\"\\\"\"");
    CHECK_PARSE_HELPER("\"\\\\\"", "\"\\\\\"");
    CHECK_PARSE_HELPER("\"'\"", "\"'\"");
    CHECK_PARSE_HELPER("\"abc\\\"def\\\\h\"", "\"abc\\\"def\\\\h\"");
}

TEST_F(SyntaxExprBisonParserTest, testRelationFunction) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("(foo(a) AND (b=c))", "foo(a) AND b = c");
}

TEST_F(SyntaxExprBisonParserTest, testNestedFunction) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("foo(bar(a))", "foo(bar(a))");
}

TEST_F(SyntaxExprBisonParserTest, testNestedArgsFunction) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CHECK_PARSE_HELPER("foo(d , bar(a , b) , c)", "foo(d,bar(a,b),c)");
}

string SyntaxExprBisonParserTest::parseFilter(const char *filterString) {
    ClauseParserContext ctx;
    ctx.parseSyntaxExpr(filterString);
    SyntaxExpr *syntaxExpr = ctx.stealSyntaxExpr();
    unique_ptr<SyntaxExpr> syntaxExpr_ptr(syntaxExpr);
    assert(syntaxExpr);

    return syntaxExpr->getExprString();
}

END_HA3_NAMESPACE(queryparser);

