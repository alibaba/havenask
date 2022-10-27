#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/common/DistinctDescription.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <string>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);

class DistinctDescriptionTest : public TESTBASE {
public:
    DistinctDescriptionTest();
    ~DistinctDescriptionTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, DistinctDescriptionTest);


DistinctDescriptionTest::DistinctDescriptionTest() { 
}

DistinctDescriptionTest::~DistinctDescriptionTest() { 
}

void DistinctDescriptionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void DistinctDescriptionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DistinctDescriptionTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    //prepare DistinctDescription
    string originalString = "distinct=dist_key:type,"
                            "distinct_count:1, distinct_times:1, "
                            "dist_filter:type<2";
    SyntaxExpr *expr = new AtomicSyntaxExpr("type", 
            vt_int32, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("type", 
            vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("2", 
            vt_unknown, INTEGER_VALUE);
    LessSyntaxExpr *checkFilterSyntaxExpr = 
        new LessSyntaxExpr(leftExpr, rightExpr);
    FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
    filterClause->setOriginalString("type<2");
    DistinctDescription distinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            originalString, 1, 1, 200, false, true, expr, filterClause);
    vector<double> gradeThresholds;
    gradeThresholds.push_back(1.0);
    gradeThresholds.push_back(2.0);
    distinctDescription.setGradeThresholds(gradeThresholds);
    DistinctDescription distinctDescription2;
    autil::DataBuffer buffer;
    buffer.write(distinctDescription);
    buffer.read(distinctDescription2);

    ASSERT_EQ(string(DEFAULT_DISTINCT_MODULE_NAME), 
                         distinctDescription2.getModuleName());
    ASSERT_EQ(1, distinctDescription2.getDistinctCount());
    ASSERT_EQ(1, distinctDescription2.getDistinctTimes());
    ASSERT_EQ(200, distinctDescription2.getMaxItemCount());
    ASSERT_TRUE(!distinctDescription2.getReservedFlag());
    ASSERT_TRUE(distinctDescription2.getUpdateTotalHitFlag());
    ASSERT_EQ(originalString, 
                         distinctDescription2.getOriginalString());
    ASSERT_TRUE(distinctDescription2.getRootSyntaxExpr());
    ASSERT_TRUE(*expr == distinctDescription2.getRootSyntaxExpr());
    FilterClause *filterClause2 = distinctDescription2.getFilterClause();
    ASSERT_TRUE(filterClause2);
    ASSERT_EQ(string("type<2"), 
                         filterClause2->getOriginalString());
    bool ret = *checkFilterSyntaxExpr == filterClause2->getRootSyntaxExpr();
    ASSERT_TRUE(ret);
    vector<double> gradeThresholds2 = distinctDescription2.getGradeThresholds();
    ASSERT_EQ((size_t)2, gradeThresholds2.size());
    ASSERT_EQ(1.0, gradeThresholds2[0]);
    ASSERT_EQ(2.0, gradeThresholds2[1]);
}

END_HA3_NAMESPACE(common);

