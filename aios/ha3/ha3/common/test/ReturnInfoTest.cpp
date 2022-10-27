#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ReturnInfo.h>

BEGIN_HA3_NAMESPACE(common);

class ReturnInfoTest : public TESTBASE {
public:
    ReturnInfoTest();
    ~ReturnInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ReturnInfoTest);


ReturnInfoTest::ReturnInfoTest() { 
}

ReturnInfoTest::~ReturnInfoTest() { 
}

void ReturnInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ReturnInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ReturnInfoTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ReturnInfo ret;
    ASSERT_TRUE(ret);

    ret.code = ERROR_ARPC;
    ASSERT_TRUE(!ret);
}

END_HA3_NAMESPACE(common);

