#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/QueryExprToStringEvaluator.h>
#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <autil/TimeUtility.h>

using namespace std;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
using namespace build_service::analyzer;
BEGIN_HA3_NAMESPACE(queryparser);

class BisonParserPerfTest : public TESTBASE {
public:
    BisonParserPerfTest();
    ~BisonParserPerfTest();
public:
    void setUp();
    void tearDown();
protected:
    std::string parseQuery(const char* query);
protected:
    config::TableInfo *_tableInfo;
    build_service::analyzer::AnalyzerFactory *_analyzerFactory;
    QueryParser *_queryParser;
    QueryExprToStringEvaluator _qee;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, BisonParserPerfTest);


BisonParserPerfTest::BisonParserPerfTest() { 
    _tableInfo = new TableInfo("");
    IndexInfos *indexInfos = new IndexInfos("");
    IndexInfo *indexInfo = new IndexInfo("");
    indexInfo->setIndexName("default");
    indexInfo->addFieldBit("title", 100);
    indexInfo->addFieldBit("body", 50);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("SimpleAnalyzer");
    indexInfos->addIndexInfo(indexInfo);
    _tableInfo->setIndexInfos(indexInfos);
    _analyzerFactory = new AnalyzerFactory();
    AnalyzerFactoryInit::initFactory(*_analyzerFactory);
}

BisonParserPerfTest::~BisonParserPerfTest() { 
    delete _tableInfo;
    delete _analyzerFactory;
}

void BisonParserPerfTest::setUp() { 
    _queryParser = new QueryParser(_tableInfo, _analyzerFactory, "default");
    _qee.reset();
}

void BisonParserPerfTest::tearDown() { 
    delete _queryParser;
}

TEST_F(BisonParserPerfTest, testQueryExprComplex) { 
    HA3_LOG(DEBUG, "Begin Test!");
    _queryParser->setDefaultOP(QueryParser::OP_AND);
    string query = "default.title:abc AND (def AND default.body:\u674e\u91d1"
                   "\u8f89(klm OR nop) ANDNOT qrs)";
    const int32_t CYCLE_COUNT = 10000;
    const int32_t TIME_COST_PER_QUERY = 70;
    const int32_t DELTA_COST_PER_QUERY = 20;

//    const char *filename = "~/svn/prof.log";
//    ProfilerStart(filename);
    uint64_t startTime = TimeUtility::currentTime();
    for (int i = 0; i < CYCLE_COUNT; i++) {
        (void)parseQuery(query.c_str());
                         
    }
//    ProfilerStop();
    uint64_t endTime = TimeUtility::currentTime();
    ASSERT_TRUE_DOUBLES_EQUAL(CYCLE_COUNT * TIME_COST_PER_QUERY, 
                                 endTime - startTime,
                                 CYCLE_COUNT * DELTA_COST_PER_QUERY);
    HA3_LOG(DEBUG, "Scanner performance time cost: (%lu us)", endTime - startTime);

    string actual = parseQuery(query.c_str());
    string expect = "{ANDExpr: {WordsExpr: (default.1)'abc'}, "
                    "{ANDExpr: {ANDExpr: {WordsExpr: (default.0)'def'}, "
                    "{WordsExpr: (default.2)'\u674e\u91d1\u8f89'}}, "
                    "{ANDNotExpr: {ORExpr: {WordsExpr: (default.0)'klm'}, "
                    "{WordsExpr: (default.0)'nop'}}, "
                    "{WordsExpr: (default.0)'qrs'}}}}";
    ASSERT_EQ(expect, actual);
}

string BisonParserPerfTest::parseQuery(const char* query) { 
    ParserContext *ctx = _queryParser->parseExpr(query);
    ASSERT_TRUE(ctx);
    std::unique_ptr<ParserContext> ctxPtr(ctx);

    ASSERT_EQ(ParserContext::OK, ctx->getStatus());
    QueryExpr *queryExpr = ctx->stealQueryExpr();
    ASSERT_TRUE(queryExpr);
    std::unique_ptr<QueryExpr> ptr(queryExpr);
    queryExpr->evaluate(&_qee);
    return _qee.stealString();    
}

END_HA3_NAMESPACE(queryparser);

