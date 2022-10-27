#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <memory>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

class AggregateBisonParserTest : public TESTBASE {
public:
    AggregateBisonParserTest();
    ~AggregateBisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    void testRange(const std::string &query, const std::vector<std::string> &rangeVec);
protected:
    ClauseParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, AggregateBisonParserTest);


AggregateBisonParserTest::AggregateBisonParserTest() { 
}

AggregateBisonParserTest::~AggregateBisonParserTest() { 
}

void AggregateBisonParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _ctx = new ClauseParserContext;
}

void AggregateBisonParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _ctx;
}

TEST_F(AggregateBisonParserTest, testMultiGroupKey) {
    HA3_LOG(DEBUG, "Begin Test!");

    bool ret = _ctx->parseAggClause("group_key:[company_id,id],agg_fun:sum(id)");
    ASSERT_TRUE(ret);

    AggregateDescription *aggDes = _ctx->stealAggDescription();
    unique_ptr<AggregateDescription> aggDes_ptr(aggDes);
    ASSERT_TRUE(aggDes);
    SyntaxExpr *keyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(keyExpr);
    auto multiSyntaxExpr = dynamic_cast<MultiSyntaxExpr*>(keyExpr);
    ASSERT_TRUE(multiSyntaxExpr);
    ASSERT_EQ(multiSyntaxExpr->getExprString(), "[company_id, id]");
    ASSERT_EQ(multiSyntaxExpr->getExprResultType(), vt_uint64);
}

TEST_F(AggregateBisonParserTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    bool ret = _ctx->parseAggClause("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
            ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    ASSERT_TRUE(ret);

    AggregateDescription *aggDes = _ctx->stealAggDescription();
    unique_ptr<AggregateDescription> aggDes_ptr(aggDes);
    ASSERT_TRUE(aggDes);
    SyntaxExpr *keyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(keyExpr);
    AtomicSyntaxExpr *atomicKeyExpr = dynamic_cast<AtomicSyntaxExpr*>(keyExpr);
    ASSERT_TRUE(atomicKeyExpr);
    ASSERT_EQ(string("company_id"), keyExpr->getExprString());
    const vector<AggFunDescription*>& aggFuns = aggDes->getAggFunDescriptions();
    ASSERT_EQ(size_t(4), aggFuns.size());
    ASSERT_TRUE(aggFuns[0]);
    ASSERT_EQ(string(AGGREGATE_FUNCTION_SUM), aggFuns[0]->getFunctionName());
    ASSERT_EQ(string("id"), aggFuns[0]->getSyntaxExpr()->getExprString());
    ASSERT_TRUE(aggFuns[1]);
    ASSERT_EQ(string(AGGREGATE_FUNCTION_COUNT), aggFuns[1]->getFunctionName());
    ASSERT_TRUE(NULL == aggFuns[1]->getSyntaxExpr());
    ASSERT_TRUE(aggFuns[2]);
    ASSERT_EQ(string(AGGREGATE_FUNCTION_MIN), aggFuns[2]->getFunctionName());
    ASSERT_EQ(string("id"), aggFuns[2]->getSyntaxExpr()->getExprString());
    ASSERT_TRUE(aggFuns[3]);
    ASSERT_EQ(string(AGGREGATE_FUNCTION_MAX), aggFuns[3]->getFunctionName());
    ASSERT_EQ(string("((id*3)+2)"), aggFuns[3]->getSyntaxExpr()->getExprString());

    const vector<string> &ranges = aggDes->getRange();
    ASSERT_EQ(size_t(3), ranges.size());
    ASSERT_EQ(string("1"), ranges[0]);
    ASSERT_EQ(string("4"), ranges[1]);
    ASSERT_EQ(string("10"), ranges[2]);

    ASSERT_EQ(uint32_t(2), aggDes->getSampleStep());
    ASSERT_EQ(uint32_t(10), aggDes->getAggThreshold());

    ASSERT_EQ(string("(price1>price2)"), aggDes->getFilterClause()->getRootSyntaxExpr()->getExprString());
    ASSERT_EQ(10u, aggDes->getMaxGroupCount());
}

TEST_F(AggregateBisonParserTest, testBug213671) { 
    HA3_LOG(DEBUG, "Begin Test!");
    bool ret = _ctx->parseAggClause("group_key:company_id,agg_fun:count(),agg_fun:sum(company_id)");
    ASSERT_TRUE(ret);
    AggregateDescription *aggDes = _ctx->stealAggDescription();
    ASSERT_TRUE(aggDes);
    delete aggDes;
}

TEST_F(AggregateBisonParserTest, testFloatRange) {


    vector<string> rangeVec;
    rangeVec.push_back("1");
    rangeVec.push_back("4.4");
    rangeVec.push_back("10.1");
    testRange("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
              ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1", rangeVec);
}

TEST_F(AggregateBisonParserTest, testNegativeFloatRange) {
    vector<string> rangeVec;
    rangeVec.push_back("-10.1");
    rangeVec.push_back("-5.5");
    rangeVec.push_back("-3");
    rangeVec.push_back("4.4");
    rangeVec.push_back("10.1");
    rangeVec.push_back("15");
    testRange("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
              ",agg_sampler_threshold:10,agg_sampler_step:2,range:-10.1~-5.5~-3~4.4~10.1~15,agg_filter:price1>price2,max_group:-1", rangeVec);
}

TEST_F(AggregateBisonParserTest, testNegativeIntRange) {
    vector<string> rangeVec;
    rangeVec.push_back("-10");
    rangeVec.push_back("-5.5");
    rangeVec.push_back("-3");
    rangeVec.push_back("4");
    rangeVec.push_back("10.1");
    rangeVec.push_back("15");
    testRange("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
              ",agg_sampler_threshold:10,agg_sampler_step:2,range:-10~-5.5~-3~4~10.1~15,agg_filter:price1>price2,max_group:-1", rangeVec);
}
void   AggregateBisonParserTest::testRange(const string &query, const vector<string> &rangeVec){
    bool ret = _ctx->parseAggClause(query.c_str());
    ASSERT_TRUE(ret);

    AggregateDescription *aggDes = _ctx->stealAggDescription();
    std::unique_ptr<AggregateDescription> aggDes_ptr(aggDes);
    ASSERT_TRUE(aggDes);
    SyntaxExpr *keyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(keyExpr);
    AtomicSyntaxExpr *atomicKeyExpr = dynamic_cast<AtomicSyntaxExpr*>(keyExpr);
    ASSERT_TRUE(atomicKeyExpr);
    ASSERT_EQ(ATTRIBUTE_NAME, atomicKeyExpr->getAtomicSyntaxExprType());
    const vector<string> &ranges = aggDes->getRange();
    ASSERT_EQ(rangeVec.size(), ranges.size());
    vector<string>::const_iterator it1 = ranges.begin();
    for (vector<string>::const_iterator it = rangeVec.begin();
         it != rangeVec.end() && it1 != ranges.end(); ++it, ++it1) {
        ASSERT_EQ(*it, *it1);
    }
}


TEST_F(AggregateBisonParserTest, testInvalidAggClause) {
    vector<string> invalidClauseVec;
    invalidClauseVec.push_back("group_key:,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum()#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count(a)#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min()#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max()"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:1.1,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2.2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:1.1");
    invalidClauseVec.push_back("group_key1:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fu:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:Sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#counT()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#miN(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#mAx(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",Agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_steP:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,rangE:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter=price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,maxgroup:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)##max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun::sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");
    invalidClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1~a,agg_filter:price1>price2,max_group:-1");

    invalidClauseVec.push_back("group_key:[id, company_id:,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4.4~10.1,agg_filter:price1>price2,max_group:-1");

    for (size_t i = 0; i < invalidClauseVec.size(); ++i) {
        bool ret = _ctx->parseAggClause(invalidClauseVec[i].c_str());
        ASSERT_TRUE(!ret)<<invalidClauseVec[i];
    }
}

TEST_F(AggregateBisonParserTest, testSpecialAggClause) {
    vector<string> specialClauseVec;
    specialClauseVec.push_back("group_key:agg_fun,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",group_key:company_id,agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(group_key)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(min)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(sum)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(count)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(max)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(agg_fun)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(agg_sampler_threshold)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(agg_sampler_step)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(range)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(agg_filter)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(max_group)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:10");
    specialClauseVec.push_back("group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:-2");
    specialClauseVec.push_back("max_group:-2,group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2");
    specialClauseVec.push_back("agg_filter:price1>price2,max_group:-2,group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10");
    specialClauseVec.push_back("range:1~4~10,agg_filter:price1>price2,max_group:-2,group_key:company_id,agg_fun:sum(id)#count()#min(id)#max(id*3+2)"
                               ",agg_sampler_threshold:10,agg_sampler_step:2");
    specialClauseVec.push_back("agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:-2,group_key:company_id,"
                               "agg_fun:sum(id)#count()#min(id)#max(id*3+2),agg_sampler_threshold:10");
    specialClauseVec.push_back("agg_sampler_threshold:10,agg_sampler_step:2,range:1~4~10,agg_filter:price1>price2,max_group:-2,group_key:company_id,"
                               "agg_fun:sum(id)#count()#min(id)#max(id*3+2)");
    specialClauseVec.push_back("agg_fun:sum(id)#count()#min(id)#max(id*3+2),agg_sampler_threshold:10,agg_sampler_step:2,"
                               "range:1~4~10,agg_filter:price1>price2,max_group:-2,group_key:company_id");
    specialClauseVec.push_back("agg_fun:sum(id)#count()#min(id)#max(id*3+2),group_key:company_id");

    for (size_t i = 0; i < specialClauseVec.size(); ++i) {
        bool ret = _ctx->parseAggClause(specialClauseVec[i].c_str());
        ASSERT_TRUE(ret)<<specialClauseVec[i];
    }
}

END_HA3_NAMESPACE(queryparser);

