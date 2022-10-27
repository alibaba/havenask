#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);

class DistinctBisonParserTest : public TESTBASE {
public:
    DistinctBisonParserTest();
    ~DistinctBisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    ClauseParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, DistinctBisonParserTest);


DistinctBisonParserTest::DistinctBisonParserTest() { 
}

DistinctBisonParserTest::~DistinctBisonParserTest() { 
}

void DistinctBisonParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _ctx = new ClauseParserContext;
}

void DistinctBisonParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _ctx;
}

TEST_F(DistinctBisonParserTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    bool ret = _ctx->parseDistClause("dist_count:2,dist_key:dist_key(dist_times),dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123");
    ASSERT_TRUE(ret);
    DistinctDescription *distDescription = _ctx->stealDistDescription();
    unique_ptr<DistinctDescription> distDescription_ptr(distDescription);
    ASSERT_TRUE(distDescription);
    SyntaxExpr *keyExpr = distDescription->getRootSyntaxExpr();
    ASSERT_TRUE(keyExpr);
    ASSERT_EQ(string("dist_key(dist_times)"), keyExpr->getExprString());

    ASSERT_EQ(int32_t(2), distDescription->getDistinctCount());
    ASSERT_EQ(int32_t(3), distDescription->getDistinctTimes());
    ASSERT_TRUE(distDescription->getReservedFlag());
    FilterClause *filterClause = distDescription->getFilterClause();
    ASSERT_TRUE(filterClause);
    ASSERT_EQ(string("(company_id=3)"), filterClause->getOriginalString());

    const vector<double>& grade = distDescription->getGradeThresholds();
    ASSERT_EQ(size_t(4), grade.size());
    ASSERT_DOUBLE_EQ(double(1), grade[0]);
    ASSERT_DOUBLE_EQ(double(2.3), grade[1]);
    ASSERT_DOUBLE_EQ(double(41.3), grade[2]);
    ASSERT_DOUBLE_EQ(double(123), grade[3]);

    ASSERT_EQ(string("lib123"), distDescription->getModuleName());
}

TEST_F(DistinctBisonParserTest, testInvalidDistinctClause) {
    vector<string> invalidClause;
    invalidClause.push_back(string("dist_key:,dist_count:2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id+,dist_count:2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:-2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:a,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2.2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:-3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:a,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1.1,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1,reserved:1,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1,reserved:false,dist_filter:company_id=3+,grade:1|23|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1,reserved:false,dist_filter:,grade:1|23|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1,reserved:false,dist_filter:company_id=3,grade:1|2~3|123|41.3,dbmodule:lib123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:1,reserved:false,dist_filter:company_id=3,grade:1|23|123|41.3,dbmodule:123"));
    invalidClause.push_back(string("dist_key:id,dist_count:2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3|a,dbmodule:lib123"));

    for (size_t i = 0; i < invalidClause.size(); ++i) {
        ClauseParserContext ctx;
        ASSERT_TRUE(!ctx.parseDistClause(invalidClause[i].c_str()));
    }
}

TEST_F(DistinctBisonParserTest, testSpecialDistinctClause) {
    vector<string> specialClause;
    specialClause.push_back(string("dist_key:dist_count(a,b,dist_count, dist_times, reserved, dist_filter, grade, dbmodule),"
                                   "dist_count:2,dist_times:3,reserved:true,dist_filter:company_id=3,grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("dist_key:dist_count(a,b,dist_count, dist_times, reserved, dist_filter, grade, dbmodule),"
                                   "dist_count:2,dist_times:3,reserved:true,dist_filter:company_id=3 AND dist_count=2,grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("dist_count:2,dist_key:dist_count(),"
                                   "dist_times:3,reserved:true,dist_filter:company_id=3 AND dist_count=2,grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("dist_times:3,dist_count:2,dist_key:dist_count(),"
                                   "reserved:true,dist_filter:company_id=3 AND dist_count=2,grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("reserved:true,dist_times:3,dist_count:2,dist_key:dist_count(),"
                                   "dist_filter:company_id=3 AND dist_count=2,grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("dist_filter:company_id=3 AND dist_count=2,reserved:true,dist_times:3,dist_count:2,dist_key:dist_count(),"
                                   "grade:1|2.3|123|41.3,dbmodule:lib123"));
    specialClause.push_back(string("grade:1|2.3|123|41.3,dist_filter:company_id=3 AND dist_count=2,reserved:true,dist_times:3,dist_count:2,"
                                   "dist_key:dist_count(),dbmodule:lib123"));
    specialClause.push_back(string("dbmodule:lib123,grade:1|2.3|123|41.3,dist_filter:company_id=3 AND dist_count=2,reserved:true,dist_times:3,dist_count:2,"
                                   "dist_key:dist_count()"));

    for (size_t i = 0; i < specialClause.size(); ++i) {
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseDistClause(specialClause[i].c_str()))<<specialClause[i];
    }
}

END_HA3_NAMESPACE(queryparser);

