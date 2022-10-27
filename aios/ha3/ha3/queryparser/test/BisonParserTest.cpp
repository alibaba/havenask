#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/QueryExprToStringEvaluator.h>

using namespace std;
USE_HA3_NAMESPACE(config);
using namespace build_service::analyzer;
BEGIN_HA3_NAMESPACE(queryparser);

class BisonParserTest : public TESTBASE {
public:
    BisonParserTest();
    ~BisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    std::string parseQuery(const char* query, bool enableMultiTermQuery = true);
protected:
    QueryParser *_queryParser;
    QueryParser *_optimizedQueryParser;
    QueryExprToStringEvaluator _qee;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, BisonParserTest);


BisonParserTest::BisonParserTest() {
}

BisonParserTest::~BisonParserTest() {
}

void BisonParserTest::setUp() {
    _queryParser = new QueryParser("default");
    _optimizedQueryParser = new QueryParser("default", OP_AND, true);
    _qee.reset();
}

void BisonParserTest::tearDown() {
    delete _queryParser;
    _queryParser = NULL;
    delete _optimizedQueryParser;
    _optimizedQueryParser = NULL;
}

TEST_F(BisonParserTest, testCompleteWordsExprNoFieldbit) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:abc");
    string expect = "{WordsExpr: (default)'abc'}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testCompleteWordsExprNoIndexName) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("abc");
    string expect = "{WordsExpr: (default)'abc'}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testPhraseExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:\"abc def\"");
    string expect = "{PhraseExpr: (default)\"abc def\"}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testAndQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:abc AND default:def");
    string expect = "{ANDExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testOrQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:abc "
                               "OR default:def");
    string expect = "{ORExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testRankQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:abc RANK default:def");
    string expect = "{RankExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testAndNotQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:abc ANDNOT default:def");
    string expect = "{ANDNotExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testDefaultQueryExprAND) {
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(OP_AND);
    string actual = parseQuery("default:abc default:def");
    string expect = "{ANDExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testDefaultQueryExprOR) {
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(OP_OR);
    _optimizedQueryParser->setDefaultOP(OP_OR);
    string actual = parseQuery("default:abc default:def");
    string expect = "{ORExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testDefaultQueryExprWithoutIndexName) {
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(OP_AND);
    string actual = parseQuery("abc def");
    string expect = "{ANDExpr: {WordsExpr: (default)'abc'}, {WordsExpr: "
                    "(default)'def'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testQueryExprComplex) {
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(OP_AND);
    string actual = parseQuery("default:abc AND (def AND default:hij"
                               "(klm OR nop) ANDNOT qrs)");
    string expect = "{ANDExpr: {WordsExpr: (default)'abc'}, "
                    "{ANDExpr: {ANDExpr: {WordsExpr: (default)'def'}, "
                    "{WordsExpr: (default)'hij'}}, "
                    "{ANDNotExpr: {ORExpr: {WordsExpr: (default)'klm'}, "
                    "{WordsExpr: (default)'nop'}}, "
                    "{WordsExpr: (default)'qrs'}}}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testQueryExprComplexWithLabel) {
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(OP_AND);
    {
        string actual = parseQuery("(default:abc AND (def AND default:hij"
                " (klm OR nop) ANDNOT qrs))@label");
        string expect = "{ANDExpr@label: {WordsExpr: (default)'abc'}, "
                        "{ANDExpr: {ANDExpr: {WordsExpr: (default)'def'}, "
                        "{WordsExpr: (default)'hij'}}, "
                        "{ANDNotExpr: {ORExpr: {WordsExpr: (default)'klm'}, "
                        "{WordsExpr: (default)'nop'}}, "
                        "{WordsExpr: (default)'qrs'}}}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("((abc)@lab1 AND ((def)@lab2 AND default:hij"
                " (klm OR nop)@lab3 ANDNOT qrs)@lab4)@lab5");
        string expect = "{ANDExpr@lab5: {WordsExpr@lab1: (default)'abc'}, "
                        "{ANDExpr@lab4: {ANDExpr: "
                        "{WordsExpr@lab2: (default)'def'}, "
                        "{WordsExpr: (default)'hij'}}, "
                        "{ANDNotExpr: {ORExpr@lab3: {WordsExpr: (default)'klm'}, "
                        "{WordsExpr: (default)'nop'}}, "
                        "{WordsExpr: (default)'qrs'}}}}";
        ASSERT_EQ(expect, actual);
    }
}

TEST_F(BisonParserTest, testNumberTermExprLessThan) {
   HA3_LOG(DEBUG, "Begin Test!");
   string actual = parseQuery("price: < 100");
   string expect = "{NumberExpr: (price)[-9223372036854775808, 100)}";
   ASSERT_EQ(expect, actual);

}

TEST_F(BisonParserTest, testNumberTermExprLessThanOrEqualTo) {
   HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price: <= 100");
    string expect = "{NumberExpr: (price)[-9223372036854775808, 100]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprGreaterThan) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price: > 100");
    string expect = "{NumberExpr: (price)(100, 9223372036854775807]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprGreaterThanOrEqualTo) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price: >= 100");
    string expect = "{NumberExpr: (price)[100, 9223372036854775807]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprEqual) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price: 1300");
    string expect = "{WordsExpr: (price)'1300'}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprRangeFalseTrue) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price:(100, 2000]");
    string expect = "{NumberExpr: (price)(100, 2000]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprRangeTrueFalse) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price:[100, 2000)");
    string expect = "{NumberExpr: (price)[100, 2000)}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprRangeTrueTrue) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price:[100, 2000]");
    string expect = "{NumberExpr: (price)[100, 2000]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprRangeTrueTrue2) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("price:[2000, 2000]");
    string expect = "{NumberExpr: (price)[2000, 2000]}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testNumberTermExprWithNullReturn) {
   HA3_LOG(DEBUG, "Begin Test!");
   ParserContext *ctx = _queryParser->parseExpr("default:<73>15636901200008461249766493>");
   assert(ctx);
   std::unique_ptr<ParserContext> ctxPtr(ctx);   
   assert(ParserContext::SYNTAX_ERROR == ctx->getStatus());
}

TEST_F(BisonParserTest, testTwoPhraseTermExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:\"aaa bbb\" AND default:\"ccc ddd\"");
    string expect = "{ANDExpr: {PhraseExpr: (default)\"aaa bbb\"}, "
                    "{PhraseExpr: (default)\"ccc ddd\"}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testTwoQuotedTermExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' OR default:'bbb'");
    string expect = "{ORExpr: {WordsExpr: (default)'aaa'}, {WordsExpr: (default)'bbb'}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testAndTermQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' & 'bbb'", false);
    string expect = "{ANDExpr: {WordsExpr: (default)'aaa'}, {WordsExpr: (default)'bbb'}}";
    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testAndTermQueryExpr1) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' RANK default:'bbb' OR default:'ccc' AND default:'ddd' ANDNOT default:'eee' | 'fff' & 'ggg'");
    string expect = "{RankExpr: {WordsExpr: (default)'aaa'}, {ORExpr: {WordsExpr: (default)'bbb'}, {ANDExpr: {WordsExpr: (default)'ccc'}, {ANDNotExpr: {WordsExpr: (default)'ddd'}, {ORExpr: {WordsExpr: (default)'eee'}, {ANDExpr: {WordsExpr: (default)'fff'}, {WordsExpr: (default)'ggg'}}}}}}}";

    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testAndTermQueryExpr2) {
    HA3_LOG(DEBUG, "Begin Test!");
    string actual = parseQuery("default:'aaa' RANK (default:'bbb' OR default:'ccc') AND default:'ddd' ANDNOT default:'eee' | 'fff' & 'ggg'");
    string expect = "{RankExpr: {WordsExpr: (default)'aaa'}, {ANDExpr: {ORExpr: {WordsExpr: (default)'bbb'}, {WordsExpr: (default)'ccc'}}, {ANDNotExpr: {WordsExpr: (default)'ddd'}, {ORExpr: {WordsExpr: (default)'eee'}, {ANDExpr: {WordsExpr: (default)'fff'}, {WordsExpr: (default)'ggg'}}}}}}";
    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testOrTermQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' | 'bbb'", false);
    string expect = "{ORExpr: {WordsExpr: (default)'aaa'}, {WordsExpr: (default)'bbb'}}";

    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testOrTermQueryExpr1) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' | 'bbb' & 'www'");
    string expect = "{ORExpr: {WordsExpr: (default)'aaa'}, {ANDExpr: {WordsExpr: (default)'bbb'}, {WordsExpr: (default)'www'}}}";

    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testOrTermQueryExpr2) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'aaa' | 'bbb'", false);
    string expect = "{ORExpr: {WordsExpr: (default)'aaa'}, {WordsExpr: (default)'bbb'}}";

    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testOrTermQueryExpr3) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string actual = parseQuery("price:'11'|'12'|'13'", true);
        string expect = "{ORExpr: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:'11'|'12'|'13'", false);
        string expect = "{ORExpr: {ORExpr: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}}, {WordsExpr: (price)'13'}}";

        ASSERT_EQ(expect, actual);
    }
}

TEST_F(BisonParserTest, testMultiTermQueryExpr) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string actual = parseQuery("price:'11'|('12'|'13')", true);
        string expect = "{ORExpr: {WordsExpr: (price)'12'}, "
                    "{WordsExpr: (price)'13'}, {WordsExpr: (price)'11'}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:('11'|'12')|'13'", true);
        string expect = "{ORExpr: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:('11'|'12')|('13'|'14')", true);
        string expect = "{ORExpr: "
                        "{ORExpr: {WordsExpr: (price)'11'}, {WordsExpr: (price)'12'}}, "
                        "{ORExpr: {WordsExpr: (price)'13'}, {WordsExpr: (price)'14'}}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:('10'|'11'|'12')&'13'&'14'", true);
        string expect = "{ANDExpr: {ANDExpr: {ORExpr: {WordsExpr: (price)'10'}, "
                    "{WordsExpr: (price)'11'}, {WordsExpr: (price)'12'}}, "
                    "{WordsExpr: (price)'13'}}, {WordsExpr: (price)'14'}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:('11'|'12')&('13'|'14')&('15'|'16')", true);
        string expect = "{ANDExpr: {ANDExpr: "
                        "{ORExpr: {WordsExpr: (price)'11'}, {WordsExpr: (price)'12'}}, "
                        "{ORExpr: {WordsExpr: (price)'13'}, {WordsExpr: (price)'14'}}}, "
                        "{ORExpr: {WordsExpr: (price)'15'}, {WordsExpr: (price)'16'}}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:('11'&'12')|('13'&'14')|('15'&'16')", true);
        string expect = "{ORExpr: {ORExpr: "
                        "{ANDExpr: {WordsExpr: (price)'11'}, {WordsExpr: (price)'12'}}, "
                        "{ANDExpr: {WordsExpr: (price)'13'}, {WordsExpr: (price)'14'}}}, "
                        "{ANDExpr: {WordsExpr: (price)'15'}, {WordsExpr: (price)'16'}}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:'11'|''", true);
        string expect = "{ORExpr: {WordsExpr: (price)'11'}, "
                        "{WordsExpr: (price)''}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:''|'11'|''", true);
        string expect = "{ORExpr: {WordsExpr: (price)''}, "
                        "{WordsExpr: (price)'11'}, {WordsExpr: (price)''}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:'11'&''", true);
        string expect = "{ANDExpr: {WordsExpr: (price)'11'}, "
                        "{WordsExpr: (price)''}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("price:'11'&''&''", true);
        string expect = "{ANDExpr: {WordsExpr: (price)'11'}, "
                        "{WordsExpr: (price)''}, {WordsExpr: (price)''}}";

        ASSERT_EQ(expect, actual);
    }
}

TEST_F(BisonParserTest, testOrTermQueryExpr4) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string actual = parseQuery("name:'aaa' | 'bbb' | 'ccc'", true);
        string expect = "{ORExpr: {WordsExpr: (name)'aaa'}, {WordsExpr: (name)'bbb'}, "
                        "{WordsExpr: (name)'ccc'}}";
        ASSERT_EQ(expect, actual);
    }
}

TEST_F(BisonParserTest, testOrTermQueryExpr5) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("name:'aaa' | 'bbb' & 'ccc' | 'ddd'");
    string expect = "{ORExpr: {ORExpr: {WordsExpr: (name)'aaa'}, {ANDExpr: "
                    "{WordsExpr: (name)'bbb'}, {WordsExpr: (name)'ccc'}}}, "
                    "{WordsExpr: (name)'ddd'}}";

    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testparentheses) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'abc' & ('def' | 'ghi')");
    string expect = "{ANDExpr: {WordsExpr: (default)'abc'}, {ORExpr: {WordsExpr: (default)'def'}, {WordsExpr: (default)'ghi'}}}";
    ASSERT_EQ(expect, actual);
}
TEST_F(BisonParserTest, testparentheses1) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'abc'&'qqq' & ('def' | 'ghi')");

    string expect ="{ANDExpr: {ANDExpr: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'qqq'}}, {ORExpr: {WordsExpr: (default)'def'}, {WordsExpr: (default)'ghi'}}}";
    ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testparentheses2) {
    HA3_LOG(DEBUG, "Begin Test!");

    string actual = parseQuery("default:'abc'&'qqq'|'mmm'&'nnn' & ('def' | 'ghi')");
    string expect = "{ORExpr: {ANDExpr: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'qqq'}}, {ANDExpr: {ANDExpr: {WordsExpr: (default)'mmm'}, {WordsExpr: (default)'nnn'}}, {ORExpr: {WordsExpr: (default)'def'}, {WordsExpr: (default)'ghi'}}}}";
        ASSERT_EQ(expect, actual);
}

TEST_F(BisonParserTest, testWeakAnd) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string actual = parseQuery("index1:(a||b||c)^3");
        string expect = "3{WEAKANDExpr: {WordsExpr: (index1)'a'}, "
                        "{WordsExpr: (index1)'b'}, {WordsExpr: (index1)'c'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("index1:(a||b|c)^3");
        string expect = "{ORExpr: 1{WEAKANDExpr: {WordsExpr: (index1)'a'}, "
                        "{WordsExpr: (index1)'b'}}, {WordsExpr: (index1)'c'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("index1:(c|a||b)^3");
        string expect = "{ORExpr: {WordsExpr: (index1)'c'}, "
                        "1{WEAKANDExpr: {WordsExpr: (index1)'a'}, {WordsExpr: (index1)'b'}}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("index1:(a||(b|c))^3");
        string expect = "{ORExpr: {WordsExpr: (index1)'a'}, "
                        "{ORExpr: {WordsExpr: (index1)'b'}, {WordsExpr: (index1)'c'}}}";
        ASSERT_EQ(expect, actual);
    }
    // &
    {
        string actual = parseQuery("index1:(a||b&c)^3");
        string expect = "{ANDExpr: 1{WEAKANDExpr: {WordsExpr: (index1)'a'}, "
                        "{WordsExpr: (index1)'b'}}, {WordsExpr: (index1)'c'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("index1:(c&a||b)^3");
        string expect = "{ANDExpr: {WordsExpr: (index1)'c'}, "
                        "1{WEAKANDExpr: {WordsExpr: (index1)'a'}, {WordsExpr: (index1)'b'}}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("index1:(a||(b&c))^3");
        string expect = "{ORExpr: {WordsExpr: (index1)'a'}, "
                        "{ANDExpr: {WordsExpr: (index1)'b'}, {WordsExpr: (index1)'c'}}}";
        ASSERT_EQ(expect, actual);
    }
    // AND
    {
        string actual = parseQuery("index1:(a||b)^2 AND index2:c");
        string expect = "{ANDExpr: 2{WEAKANDExpr: {WordsExpr: (index1)'a'}, "
                        "{WordsExpr: (index1)'b'}}, {WordsExpr: (index2)'c'}}";
        ASSERT_EQ(expect, actual);
    }
    // ERROR
    {
        ParserContext *ctx = _optimizedQueryParser->parseExpr("(index1:a || index2:b)^2");
        ASSERT_TRUE(ctx);
        std::unique_ptr<ParserContext> ctxPtr(ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());
    }
    {
        ParserContext *ctx = _optimizedQueryParser->parseExpr("(index1:a || index2:b)");
        ASSERT_TRUE(ctx);
        std::unique_ptr<ParserContext> ctxPtr(ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());
    }
}


TEST_F(BisonParserTest, testparentheses3) {
    HA3_LOG(DEBUG, "Begin Test!");


    ParserContext *ctx = _queryParser->parseExpr("default:'abc'&default:'qqq'");
    ASSERT_TRUE(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());

}

TEST_F(BisonParserTest, testparentheses4) {
    HA3_LOG(DEBUG, "Begin Test!");


    ParserContext *ctx = _queryParser->parseExpr("'abc'&'qqq'");
    ASSERT_TRUE(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());

}

TEST_F(BisonParserTest, testparentheses5) {
    HA3_LOG(DEBUG, "Begin Test!");


    ParserContext *ctx = _queryParser->parseExpr("'abc'&default:'qqq'");
    ASSERT_TRUE(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());

}

TEST_F(BisonParserTest, testparentheses6) {
    HA3_LOG(DEBUG, "Begin Test!");


    ParserContext *ctx = _queryParser->parseExpr("default:'qqq'&(default:'aaa'|'bbb')");
    ASSERT_TRUE(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());

}

TEST_F(BisonParserTest, testParseLabel) {
    {
        string query = "(abc)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{WordsExpr@label: (default)'abc'}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(abc)";
        string actual = parseQuery(query.c_str());
        string expect = "{WordsExpr: (default)'abc'}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "((default):abc)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{WordsExpr@label: (default)'abc'}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "([default]:abc)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{WordsExpr@label: (default)'abc'}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(abc def)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{ANDExpr@label: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'def'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(abc OR def)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{ORExpr@label: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'def'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(abc ANDNOT def)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{ANDNotExpr@label: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'def'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(abc RANK def)@label";
        string actual = parseQuery(query.c_str());
        string expect = "{RankExpr@label: {WordsExpr: (default)'abc'}, {WordsExpr: (default)'def'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query = "(default:\"abc def\")@label";
        string actual = parseQuery(query.c_str());
        string expect = "{PhraseExpr@label: (default)\"abc def\"}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:11&12&13)@label", true);
        string expect = "{ANDExpr@label: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:11|12|13)@label", true);
        string expect = "{ORExpr@label: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:(11||12||13))@label", true);
        string expect = "1{WEAKANDExpr@label: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:(11||12||13)^2)@label", true);
        string expect = "2{WEAKANDExpr@label: {WordsExpr: (price)'11'}, "
                    "{WordsExpr: (price)'12'}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:11|12|13)@label", false);
        string expect ="{ORExpr@label: {ORExpr: {WordsExpr: (price)'11'}, "
                       "{WordsExpr: (price)'12'}}, {WordsExpr: (price)'13'}}";

        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:11&12&13)@label", false);
        string expect ="{ANDExpr@label: {ANDExpr: {WordsExpr: (price)'11'}, "
                       "{WordsExpr: (price)'12'}}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:(11||12||13))@label", false);
        string expect ="{ORExpr@label: {ORExpr: {WordsExpr: (price)'11'}, "
                       "{WordsExpr: (price)'12'}}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string actual = parseQuery("(price:(11||12||13)^2)@label", false);
        string expect ="{ORExpr@label: {ORExpr: {WordsExpr: (price)'11'}, "
                       "{WordsExpr: (price)'12'}}, {WordsExpr: (price)'13'}}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query("price:(11||12||13)@label^2");
        ParserContext *ctx = _queryParser->parseExpr(query.c_str());
        ASSERT_TRUE(ctx);
        std::unique_ptr<ParserContext> ctxPtr(ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());
    }
    {
        string actual = parseQuery("(price: < 100)@label");
        string expect = "{NumberExpr@label: (price)[-9223372036854775808, 100)}";
        ASSERT_EQ(expect, actual);
    }
    {
        string query("(abc)@123");
        ParserContext *ctx = _queryParser->parseExpr(query.c_str());
        ASSERT_TRUE(ctx);
        std::unique_ptr<ParserContext> ctxPtr(ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());
    }
    {
        string query("(abc)@123label");
        ParserContext *ctx = _queryParser->parseExpr(query.c_str());
        ASSERT_TRUE(ctx);
        std::unique_ptr<ParserContext> ctxPtr(ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());
    }
}

string BisonParserTest::parseQuery(const char* query,
                                   bool enableMultiTermQuery)
{
    ParserContext *ctx = enableMultiTermQuery
                         ? _optimizedQueryParser->parseExpr(query)
                         : _queryParser->parseExpr(query);
    assert(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    assert(ParserContext::OK == ctx->getStatus());
    vector<QueryExpr*> queryExprs = ctx->stealQueryExprs();
    assert(size_t(1) == queryExprs.size());
    assert(queryExprs[0]);
    std::unique_ptr<QueryExpr> ptr(queryExprs[0]);
    queryExprs[0]->evaluate(&_qee);
    return _qee.stealString();
}

END_HA3_NAMESPACE(queryparser);
