#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryTerminator.h>
#include <autil/TimeUtility.h>

using namespace autil;

BEGIN_HA3_NAMESPACE(common);

class QueryTerminatorTest : public TESTBASE {
public:
    QueryTerminatorTest();
    ~QueryTerminatorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, QueryTerminatorTest);


QueryTerminatorTest::QueryTerminatorTest() { 
}

QueryTerminatorTest::~QueryTerminatorTest() { 
}

void QueryTerminatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QueryTerminatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryTerminatorTest, testTerminate) { 
    HA3_LOG(DEBUG, "Begin Test!");
    QueryTerminator terminator;
    ASSERT_TRUE(!terminator.isTerminated());

    terminator.setTerminated(true);
    ASSERT_TRUE(terminator.isTerminated());

    terminator.setTerminated(false);
    ASSERT_TRUE(!terminator.isTerminated());
}

END_HA3_NAMESPACE(common);

