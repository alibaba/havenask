#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/common/Query.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/config/QueryInfo.h>
#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <string>
#include <memory>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(queryparser);

class QueryParserTest : public TESTBASE {
public:
    QueryParserTest();
    ~QueryParserTest();
public:
    void setUp();
    void tearDown();
protected:
    QueryParser *_queryParser;
    QueryParser *_optimizedQueryParser;
    std::vector<common::Query*> _querys;
    common::Query *_expectQuery;
    ParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};


HA3_LOG_SETUP(queryparser, QueryParserTest);


QueryParserTest::QueryParserTest() {
    _expectQuery = NULL;
    _ctx = NULL;
}

QueryParserTest::~QueryParserTest() {
}

void QueryParserTest::setUp() {
    _queryParser = new QueryParser("default");
    _optimizedQueryParser = new QueryParser("default", OP_AND, true);
}

void QueryParserTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_queryParser);
    DELETE_AND_SET_NULL(_optimizedQueryParser);

    for (size_t i = 0; i < _querys.size(); ++i) {
        DELETE_AND_SET_NULL(_querys[i]);
    }
    _querys.clear();

    delete _expectQuery;
    _expectQuery = NULL;
    delete _ctx;
    _ctx = NULL;
}

TEST_F(QueryParserTest, testParseTermQueryComplete) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:abc");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    _expectQuery = new TermQuery("abc", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseTermQueryWithBoost) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _ctx = _queryParser->parse("default:abc^300");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new TermQuery("abc", "default", requiredFields, "", 300);
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }
    {
        setUp();
        _ctx = _queryParser->parse("default:abc^200^100");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
        ASSERT_EQ(string("syntax error, unexpected '^', expecting end of file or ';'"),
                             _ctx->getStatusMsg());
    }

}

TEST_F(QueryParserTest, testParseTermQueryWithSecondaryChain) {
    HA3_LOG(DEBUG, "Begin Test!");

    {
        _ctx = _queryParser->parse("default:abc#secondaryChain");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new TermQuery("abc", "default", requiredFields, "",
                100, "secondaryChain");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("default:'#'#secondaryChain");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new TermQuery("#", "default", requiredFields, "",
                100, "secondaryChain");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("default:abc#secondaryChain#secondaryChain2");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
        ASSERT_EQ(string("syntax error, unexpected '#', expecting end of file or ';'"),
                             _ctx->getStatusMsg());
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("default:abc#secondaryChain^100#secondaryChain2");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
        ASSERT_EQ(string("syntax error, unexpected '#', expecting end of file or ';'"),
                             _ctx->getStatusMsg());
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("default:abc#secondaryChain^300");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new TermQuery("abc", "default", requiredFields, "",
                300, "secondaryChain");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("default:abc^300#secondaryChain");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new TermQuery("abc", "default", requiredFields, "",
                300, "secondaryChain");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
    }
}


TEST_F(QueryParserTest, testParseTermQueryWithNumberHeading) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:3C");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("3C", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseTermQueryWithWidthAlpha) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:ＣＤＭＡ");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("ＣＤＭＡ", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testMultiTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:ＣＤＭＡ");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("ＣＤＭＡ", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseTermQueryNoFieldBit) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:abc");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    _expectQuery = new TermQuery("abc", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseBinaryQueryWithFields) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _ctx = _optimizedQueryParser->parse("index(f1,f2, f3):(abc & def & hij)");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"MultiTermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        TermPtr term1(new Term("abc", "index", requiredFields));
        TermPtr term2(new Term("def", "index", requiredFields));
        TermPtr term3(new Term("hij", "index", requiredFields));
        MultiTermQuery *resultQuery = new MultiTermQuery("");
        resultQuery->addTerm(term1);
        resultQuery->addTerm(term2);
        resultQuery->addTerm(term3);
        resultQuery->setOPExpr(OP_AND);
        _expectQuery = resultQuery;
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _optimizedQueryParser->parse("index[f1,f2, f3]:(abc | def)");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"MultiTermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        requiredFields.isRequiredAnd = false;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        TermPtr term1(new Term("abc", "index", requiredFields));
        TermPtr term2(new Term("def", "index", requiredFields));
        MultiTermQuery *resultQuery = new MultiTermQuery("");
        resultQuery->addTerm(term1);
        resultQuery->addTerm(term2);
        resultQuery->setOPExpr(OP_OR);
        _expectQuery = resultQuery;
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }
    {
        setUp();
        _ctx = _optimizedQueryParser->parse("index[f1,f2, f3]:(123 | def)");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"MultiTermQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        requiredFields.isRequiredAnd = false;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        TermPtr term1(new Term("123", "index", requiredFields));
        TermPtr term2(new Term("def", "index", requiredFields));
        MultiTermQuery *resultQuery = new MultiTermQuery("");
        resultQuery->addTerm(term1);
        resultQuery->addTerm(term2);
        resultQuery->setOPExpr(OP_OR);
        _expectQuery = resultQuery;
        ASSERT_EQ(*_expectQuery, *_querys[0]);
    }
}

TEST_F(QueryParserTest, testParseTermQueryWithFields) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _ctx = _queryParser->parse("index(f1,f2, f3):(abc)");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

        RequiredFields requiredFields;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        _expectQuery = new TermQuery("abc", "index", requiredFields, "");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        delete _expectQuery;
        requiredFields.fields.clear();
        requiredFields.fields.push_back("f1");
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        _expectQuery = new TermQuery("abc", "index", requiredFields, "");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("(f1,f2, f3) :abc");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

        RequiredFields requiredFields;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        _expectQuery = new TermQuery("abc", "default", requiredFields, "");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("index[ f1 , f2, f3] : abc");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

        RequiredFields requiredFields;
        requiredFields.fields.push_back("f2");
        requiredFields.fields.push_back("f3");
        requiredFields.fields.push_back("f1");
        requiredFields.isRequiredAnd = false;
        _expectQuery = new TermQuery("abc", "index", requiredFields, "");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
    }
}

TEST_F(QueryParserTest, testParseTermQueryWithFieldsFailed) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _ctx = _queryParser->parse("index( f1 , f2, f3] : abc");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("index[ f1 , f2, f3) : abc");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
        tearDown();
    }

    {
        setUp();
        _ctx = _queryParser->parse("index( f1 ; f2, f3) : abc");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
    }
}

TEST_F(QueryParserTest, testParseTermQueryDefaultIndex) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("abc");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("abc", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseTermQueryDefaultIndexWithDot) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("'a.bc'");
    ASSERT_TRUE(_ctx);
    HA3_LOG(DEBUG, "parsing |'a.bc'|, status msg(%s)",
        _ctx->getStatusMsg().c_str());
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("a.bc", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseTermQueryWithColon) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("default:'a:bc'");
    ASSERT_TRUE(_ctx);
    HA3_LOG(DEBUG, "parsing |default:'a.bc'|, status msg(%s)",
        _ctx->getStatusMsg().c_str());
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    _expectQuery = new TermQuery("a:bc", "default", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseNumberTermQuerySingleNumber) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:34^200#secondaryChain");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);

    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    _expectQuery = new TermQuery("34", "price", requiredFields, "",
                                   200, "secondaryChain");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseNumberTermQueryTrueFalse) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price(f2, f1, f3):[34,37)");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"NumberQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    requiredFields.fields.push_back("f2");
    requiredFields.fields.push_back("f3");
    requiredFields.fields.push_back("f1");

    _expectQuery = new NumberQuery(34, true, 37, false, "price", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseNumberTermQueryFalseTrue) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:(134,337]");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"NumberQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    _expectQuery = new NumberQuery(134, false, 337, true,
                                   "price", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}
TEST_F(QueryParserTest, testParseOpenNumberQuery)
{
    _ctx = _queryParser->parse("price:[34,)");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
}

TEST_F(QueryParserTest, testParseNumberTermQueryTrueTrue) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price[f1]:[34,37]");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(1), _querys.size());
    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"NumberQuery", _querys[0]->getQueryName());

    RequiredFields requiredFields;
    requiredFields.fields.push_back("f1");
    requiredFields.isRequiredAnd = false;
    _expectQuery = new NumberQuery(34, true, 37, true,
                                   "price", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
}

TEST_F(QueryParserTest, testParseNumberTermQueryInvalidRange) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:[200,37]");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
    ASSERT_EQ(string("Invalid Range: [200, 37]."),
                         _ctx->getStatusMsg());
}

TEST_F(QueryParserTest, testParseNumberTermQueryInvalidRangeFalseTrue) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:(137,137]");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
    ASSERT_EQ(string("Invalid Range: (137, 137]."),
                         _ctx->getStatusMsg());
}

TEST_F(QueryParserTest, testParseNumberTermQueryInvalidRangeTrueFalse) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:[37,37)");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
    ASSERT_EQ(string("Invalid Range: [37, 37)."),
                         _ctx->getStatusMsg());
}

TEST_F(QueryParserTest, testParseNumberTermQueryInvalidRangeFalseFalse) {
    HA3_LOG(DEBUG, "Begin Test!");
    _ctx = _queryParser->parse("price:(237,237)");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
    ASSERT_EQ(string("Invalid Range: (237, 237)."),
                         _ctx->getStatusMsg());
}

TEST_F(QueryParserTest, testParseExprErrorBug69946) {
     HA3_LOG(DEBUG, "Begin Test!");
     _ctx = _queryParser->parse("phrase:\xc8\xb9");
     ASSERT_TRUE(_ctx);
     ASSERT_EQ(ParserContext::SYNTAX_ERROR, _ctx->getStatus());
     ASSERT_EQ(string("syntax error, unexpected end of file"),
                         _ctx->getStatusMsg());
}

TEST_F(QueryParserTest, testMultiQuerys) {
    _ctx = _queryParser->parse("phrase:'133'; '133'#second AND phrase:';';\"a\";\"b\"");
    ASSERT_TRUE(_ctx);
    ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
    _querys = _ctx->stealQuerys();
    ASSERT_EQ(size_t(4), _querys.size());

    ASSERT_TRUE(_querys[0]);
    ASSERT_EQ((string)"TermQuery", _querys[0]->getQueryName());
    RequiredFields requiredFields;
    _expectQuery = new TermQuery("133", "phrase", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *_querys[0]);
    DELETE_AND_SET_NULL(_expectQuery);

    ASSERT_TRUE(_querys[1]);
    ASSERT_EQ((string)"AndQuery", _querys[1]->getQueryName());

    const std::vector<QueryPtr>* childQuery = _querys[1]->getChildQuery();
    ASSERT_EQ(size_t(2), childQuery->size());

    _expectQuery = new TermQuery("133", "default", requiredFields, "", DEFAULT_BOOST_VALUE, "second");
    ASSERT_EQ(*_expectQuery, *(*childQuery)[0]);
    DELETE_AND_SET_NULL(_expectQuery);

    _expectQuery = new TermQuery(";", "phrase", requiredFields, "");
    ASSERT_EQ(*_expectQuery, *(*childQuery)[1]);
    DELETE_AND_SET_NULL(_expectQuery);

    ASSERT_TRUE(_querys[2]);
    ASSERT_EQ((string)"PhraseQuery", _querys[2]->getQueryName());
    PhraseQuery *phraseQuery = new PhraseQuery("");
    phraseQuery->addTerm(TermPtr(new Term("a", "default", requiredFields)));
    _expectQuery = phraseQuery;
    ASSERT_EQ(*_expectQuery, *_querys[2]);
    DELETE_AND_SET_NULL(_expectQuery);

    ASSERT_TRUE(_querys[3]);
    ASSERT_EQ((string)"PhraseQuery", _querys[3]->getQueryName());
    phraseQuery = new PhraseQuery("");
    phraseQuery->addTerm(TermPtr(new Term("b", "default", requiredFields)));
    _expectQuery = phraseQuery;
    ASSERT_EQ(*_expectQuery, *_querys[3]);
    DELETE_AND_SET_NULL(_expectQuery);
}

TEST_F(QueryParserTest, test64bitNumberTerm) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        _ctx = _queryParser->parse("price:(1340000000000,3370000000000]");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"NumberQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new NumberQuery(1340000000000LL, false, 3370000000000LL, true,
                                       "price", requiredFields, "");
	NumberQuery* query = dynamic_cast<NumberQuery*>(_querys[0]);
	NumberTerm term = query->getTerm();
	std::string expectedWord = "[1340000000001,3370000000000]";
	ASSERT_EQ(expectedWord, term.getWord());
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        DELETE_AND_SET_NULL(_expectQuery);
        for (size_t i = 0; i < _querys.size(); ++i) {
            DELETE_AND_SET_NULL(_querys[i]);
        }
        _querys.clear();
        delete _ctx;
        _ctx = NULL;
    }
    {
        _ctx = _queryParser->parse("price:(,3370000000001)");
        ASSERT_TRUE(_ctx);
        ASSERT_EQ(ParserContext::OK, _ctx->getStatus());
        _querys = _ctx->stealQuerys();
        ASSERT_EQ(size_t(1), _querys.size());
        ASSERT_TRUE(_querys[0]);
        ASSERT_EQ((string)"NumberQuery", _querys[0]->getQueryName());
        RequiredFields requiredFields;
        _expectQuery = new NumberQuery(std::numeric_limits<int64_t>::min(), true,
                3370000000000LL, true, "price", requiredFields, "");
        ASSERT_EQ(*_expectQuery, *_querys[0]);
        DELETE_AND_SET_NULL(_expectQuery);
        for (size_t i = 0; i < _querys.size(); ++i) {
            DELETE_AND_SET_NULL(_querys[i]);
        }
        _querys.clear();
        delete _ctx;
        _ctx = NULL;
    }
}

TEST_F(QueryParserTest, testParseWithLabel) {
#define ASSERT_SINGLE_QUERY(queryStr, label, queryType, ...)    \
    {                                                   \
        auto queryParser = new QueryParser("default", OP_AND, true);    \
        auto ctx = queryParser->parse(queryStr);        \
        ASSERT_TRUE(ctx);                               \
        ASSERT_EQ(ParserContext::OK, ctx->getStatus()); \
        auto querys = ctx->stealQuerys();               \
        ASSERT_EQ(size_t(1), querys.size());            \
        auto result = querys[0];                        \
        ASSERT_TRUE(result);                            \
        ASSERT_EQ(""#queryType, result->getQueryName());\
        auto expectQuery = new queryType(__VA_ARGS__);  \
        ASSERT_EQ(*expectQuery, *result);               \
        ASSERT_EQ(label, result->getQueryLabel());      \
        delete expectQuery;                             \
        delete result;                                  \
        delete ctx;                                     \
        delete queryParser;                             \
    }
    {
        Term tt("abc", "default", RequiredFields());
        ASSERT_SINGLE_QUERY("(abc)@labTerm", "labTerm", TermQuery, tt, "labTerm");
    }
    {
        RequiredFields requiredFields;
        TermPtr term1(new Term("123", "index", requiredFields));
        TermPtr term2(new Term("def", "index", requiredFields));
        MultiTermQuery multiQuery("labMultiTerm", OP_OR);
        multiQuery.addTerm(term1);
        multiQuery.addTerm(term2);
        ASSERT_SINGLE_QUERY("(index:123|def)@labMultiTerm", "labMultiTerm",
                            MultiTermQuery, multiQuery);
    }
    {
        NumberTerm nn(134, false, 337, true, "price", RequiredFields());
        ASSERT_SINGLE_QUERY("(price:(134,337])@labNumber", "labNumber",
                            NumberQuery, nn, "labNumber");
    }
    {
        PhraseQuery phraseQuery("labPhrase");
        TermPtr ptt(new Term("abc", "default", RequiredFields()));
        phraseQuery.addTerm(ptt);
        ASSERT_SINGLE_QUERY("(\"abc\")@labPhrase", "labPhrase", PhraseQuery,
                            phraseQuery);
    }
#undef ASSERT_SINGLE_QUERY

#define ASSERT_BINARY_QUERY(queryStr, label, queryType)      \
    {                                                        \
        auto queryParser = new QueryParser("default");       \
        auto ctx = queryParser->parse(queryStr);             \
        ASSERT_TRUE(ctx);                                    \
        ASSERT_EQ(ParserContext::OK, ctx->getStatus());      \
        auto querys = ctx->stealQuerys();                    \
        ASSERT_EQ(size_t(1), querys.size());                 \
        auto result = querys[0];                             \
        ASSERT_TRUE(result);                                    \
        ASSERT_EQ(""#queryType, result->getQueryName());        \
        RequiredFields requiredFields;                          \
        auto expectQuery = new queryType(label);                        \
        QueryPtr q1(new TermQuery("abc", "default", requiredFields, "")); \
        QueryPtr q2(new TermQuery("def", "default", requiredFields, "")); \
        expectQuery->addQuery(q1);                                      \
        expectQuery->addQuery(q2);                                      \
        ASSERT_EQ(*expectQuery, *result);                               \
        ASSERT_EQ(label, result->getQueryLabel());                      \
        delete expectQuery;                                             \
        delete result;                                                  \
        delete ctx;                                                     \
        delete queryParser;                                             \
    }

    ASSERT_BINARY_QUERY("(abc def)@labAnd1", "labAnd1", AndQuery);
    ASSERT_BINARY_QUERY("(abc AND def)@labAnd2", "labAnd2", AndQuery);
    ASSERT_BINARY_QUERY("(abc OR def)@labOr", "labOr", OrQuery);
    ASSERT_BINARY_QUERY("(abc RANK def)@labRank", "labRank", RankQuery);
    ASSERT_BINARY_QUERY("(abc ANDNOT def)@labAndNot", "labAndNot", AndNotQuery);
#undef ASSERT_BINARY_QUERY
}

TEST_F(QueryParserTest, testParseWithLabelFail) {
#define ASSERT_SINGLE_QUERY(queryStr, label)            \
    {                                                   \
        auto queryParser = new QueryParser("default", OP_AND, true); \
        auto ctx = queryParser->parse(queryStr);        \
        ASSERT_TRUE(ctx);                               \
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus()); \
        delete ctx;                                     \
        delete queryParser;                             \
    }

    ASSERT_SINGLE_QUERY("abc@labTerm", "labTerm");
    ASSERT_SINGLE_QUERY("index:123|def @labMultiTerm", "labMultiTerm");
    ASSERT_SINGLE_QUERY("price:(134,337]@labNumber", "labNumber");
    ASSERT_SINGLE_QUERY("\"abc\"@labPhrase", "labPhrase");

#undef ASSERT_SINGLE_QUERY

#define ASSERT_BINARY_QUERY(queryStr, label, queryType)      \
    {                                                        \
        auto queryParser = new QueryParser("default");       \
        auto ctx = queryParser->parse(queryStr);             \
        ASSERT_TRUE(ctx);                                    \
        ASSERT_EQ(ParserContext::SYNTAX_ERROR, ctx->getStatus());      \
        delete ctx;                                                     \
        delete queryParser;                                             \
    }

    ASSERT_BINARY_QUERY("abc def @labAnd1", "labAnd1", AndQuery);
    ASSERT_BINARY_QUERY("abc AND def @labAnd2", "labAnd2", AndQuery);
    ASSERT_BINARY_QUERY("abc OR def @labOr", "labOr", OrQuery);
    ASSERT_BINARY_QUERY("abc RANK def @labRank", "labRank", RankQuery);
    ASSERT_BINARY_QUERY("abc ANDNOT def @labAndNot", "labAndNot", AndNotQuery);
#undef ASSERT_BINARY_QUERY
}

END_HA3_NAMESPACE(queryparser);
