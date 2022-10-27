#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/common/AggregateClause.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <autil/legacy/any.h>
#include <string>


using namespace autil::legacy::json;
using namespace std;

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class AggregateClauseTest : public TESTBASE {
public:
    AggregateClauseTest();
    ~AggregateClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AggregateClauseTest);


AggregateClauseTest::AggregateClauseTest() { 
}

AggregateClauseTest::~AggregateClauseTest() { 
}

void AggregateClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AggregateClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AggregateClauseTest, testToString) {
    string requestStr = "aggregate=group_key:company_id,agg_fun:count()#min(id),agg_sampler_threshold:5,agg_sampler_step:2,max_group:1000;group_key:func(id),agg_fun:sum(id)#max(id)#min(id),range:1~2~3,agg_filter:id>10";
    RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    AggregateClause* aggClause = requestPtr->getAggregateClause();
    ASSERT_EQ(string("[group_key:company_id,agg_fun:count()#min(id)#,ranges:,"
                                "maxgroupcount:1000,step:2,thredshold:5][group_key:func(id),agg_fun:sum(id)#max(id)#min(id)#,"
                                "ranges:1|2|3|,maxgroupcount:1000,step:0,thredshold:0,filter:(id>10)]"), 
                         aggClause->toString());    
}

END_HA3_NAMESPACE(common);
