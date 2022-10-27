#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/common/DistinctClause.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class DistinctClauseTest : public TESTBASE {
public:
    DistinctClauseTest();
    ~DistinctClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, DistinctClauseTest);


static int sTestCount = 0;

DistinctClauseTest::DistinctClauseTest() { 
}

DistinctClauseTest::~DistinctClauseTest() { 
}

void DistinctClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void DistinctClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DistinctClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    //prepare DistinctClause
    string originalStr1 = "dist_key:type,distinct_count:1, distinct_times:1, dist_filter:type<2";
    string originalStr2 = "dist_key:type,dist_count:2, dist_times:2,reserved:false";
    string originalString = "distinct=" + originalStr1 + ";none_dist;" + originalStr2;

    SyntaxExpr *expr = new AtomicSyntaxExpr("type", vt_int32, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("type", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("2", vt_unknown, INTEGER_VALUE);
    LessSyntaxExpr *checkFilterSyntaxExpr = new LessSyntaxExpr(leftExpr, rightExpr);
    FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
    filterClause->setOriginalString("type<2");
    DistinctClause distinctClause;
    distinctClause.setOriginalString(originalString);

    DistinctDescription* distinctDescription = 
        new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
                                originalStr1, 1, 1, 200, false,
                                true, expr, filterClause);
    vector<double> gradeThresholds;
    gradeThresholds.push_back(1.0);
    gradeThresholds.push_back(2.0);
    distinctDescription->setGradeThresholds(gradeThresholds);

    distinctClause.addDistinctDescription(distinctDescription);
    distinctClause.addDistinctDescription(NULL);
    
    SyntaxExpr *expr2 = new AtomicSyntaxExpr("type", vt_int32, ATTRIBUTE_NAME);
    DistinctDescription* distinctDescription2 = 
        new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
                                originalStr2, 2, 2, 100, true,
                                false, expr2, NULL);
    distinctClause.addDistinctDescription(distinctDescription2);

    DistinctClause distinctClause2;
    autil::DataBuffer buffer;
    buffer.write(distinctClause);
    buffer.read(distinctClause2);

    ASSERT_EQ(originalString, distinctClause2.getOriginalString());
    const vector<DistinctDescription*> &distDescs = 
        distinctClause2.getDistinctDescriptions();
    ASSERT_EQ((size_t)3, distDescs.size());
    
    DistinctDescription* distDesc = distDescs[0];
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(string(DEFAULT_DISTINCT_MODULE_NAME), 
                         distDesc->getModuleName());
    ASSERT_EQ(1, distDesc->getDistinctCount());
    ASSERT_EQ(1, distDesc->getDistinctTimes());
    ASSERT_EQ(200, distDesc->getMaxItemCount());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    ASSERT_TRUE(distDesc->getUpdateTotalHitFlag());
    ASSERT_EQ(originalStr1, distDesc->getOriginalString());
    ASSERT_TRUE(distDesc->getRootSyntaxExpr());
    ASSERT_TRUE(*expr == distDesc->getRootSyntaxExpr());
    FilterClause *filterClause2 = distDesc->getFilterClause();
    ASSERT_TRUE(filterClause2);
    ASSERT_EQ(string("type<2"), filterClause2->getOriginalString());
    ASSERT_TRUE(*checkFilterSyntaxExpr == (filterClause2->getRootSyntaxExpr()));
    vector<double> gradeThresholds2 = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)2, gradeThresholds2.size());
    ASSERT_EQ(1.0, gradeThresholds2[0]);
    ASSERT_EQ(2.0, gradeThresholds2[1]);

    distDesc = distDescs[1];
    ASSERT_TRUE(!distDesc);

    distDesc = distDescs[2];
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(string(DEFAULT_DISTINCT_MODULE_NAME), 
                         distDesc->getModuleName());
    ASSERT_EQ(2, distDesc->getDistinctCount());
    ASSERT_EQ(2, distDesc->getDistinctTimes());
    ASSERT_EQ(100, distDesc->getMaxItemCount());
    ASSERT_TRUE(distDesc->getReservedFlag());
    ASSERT_TRUE(!distDesc->getUpdateTotalHitFlag());
    ASSERT_EQ(originalStr2, distDesc->getOriginalString());
    ASSERT_TRUE(distDesc->getRootSyntaxExpr());
    ASSERT_TRUE(*expr2 == distDesc->getRootSyntaxExpr());
    filterClause2 = distDesc->getFilterClause();
    ASSERT_TRUE(!filterClause2);
    gradeThresholds2 = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)0, gradeThresholds2.size());

    sTestCount++;    
}

TEST_F(DistinctClauseTest, testToString) {
    {
        string requestStr = "distinct=dist_key:company_id,dist_count:1,dist_times:1,reserved:false,max_item_count:100,update_total_hit:true,grade:1.1|2.2|3.3,dist_filter:filter(id) > 0;";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
        ASSERT_TRUE(requestPtr);
        DistinctClause* distinctClause 
            = requestPtr->getDistinctClause();
        ASSERT_EQ(string("[moduleName:,distkey:company_id,distincttimes:1,distinctcount:1,maxitemcount:100,reservedflag:false,updatetotalhitflag:true,filter:(filter(id)>0),grades:1.1|2.2|3.3|]"), distinctClause->toString());
    }
    {
        string requestStr = "distinct=none_dist;dist_key:company_id,dist_count:1,dist_times:1,reserved:false,max_item_count:100,update_total_hit:true,grade:1.1|2.2|3.3,dist_filter:filter(id) > 0;";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
        ASSERT_TRUE(requestPtr);
        DistinctClause* distinctClause 
            = requestPtr->getDistinctClause();
        ASSERT_EQ(string("[none_dist][moduleName:,distkey:company_id,distincttimes:1,distinctcount:1,maxitemcount:100,reservedflag:false,updatetotalhitflag:true,filter:(filter(id)>0),grades:1.1|2.2|3.3|]"), distinctClause->toString());
    }
    {
        string requestStr = "distinct=dist_key:company_id,dist_count:1,dist_times:1,reserved:false,max_item_count:100,update_total_hit:true,grade:1.1|2.2|3.3,dist_filter:filter(id) > 0;none_dist;";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
        ASSERT_TRUE(requestPtr);
        DistinctClause* distinctClause 
            = requestPtr->getDistinctClause();
        ASSERT_EQ(string("[moduleName:,distkey:company_id,distincttimes:1,distinctcount:1,maxitemcount:100,reservedflag:false,updatetotalhitflag:true,filter:(filter(id)>0),grades:1.1|2.2|3.3|][none_dist]"), distinctClause->toString());
    }
    {
        string requestStr = "distinct=dist_key:company_id,dist_count:1,dist_times:1,reserved:false,max_item_count:100,update_total_hit:true,grade:1.1|2.2|3.3,dist_filter:filter(id) > 0;dist_key:nid,dist_count:1,dist_times:1;";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
        ASSERT_TRUE(requestPtr);
        DistinctClause* distinctClause 
            = requestPtr->getDistinctClause();
        ASSERT_EQ(string("[moduleName:,distkey:company_id,distincttimes:1,distinctcount:1,maxitemcount:100,reservedflag:false,updatetotalhitflag:true,filter:(filter(id)>0),grades:1.1|2.2|3.3|][moduleName:,distkey:nid,distincttimes:1,distinctcount:1,maxitemcount:0,reservedflag:true,updatetotalhitflag:false,grades:]"), distinctClause->toString());
    }
}

END_HA3_NAMESPACE(common);

