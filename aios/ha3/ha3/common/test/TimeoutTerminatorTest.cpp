#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/TimeoutTerminator.h>

using namespace autil;

BEGIN_HA3_NAMESPACE(common);

class TimeoutTerminatorTest : public TESTBASE {
public:
    TimeoutTerminatorTest();
    ~TimeoutTerminatorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, TimeoutTerminatorTest);


TimeoutTerminatorTest::TimeoutTerminatorTest() { 
}

TimeoutTerminatorTest::~TimeoutTerminatorTest() { 
}

void TimeoutTerminatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TimeoutTerminatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TimeoutTerminatorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    int64_t timeout = 100000; // 100ms
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_EQ(startTime, terminator.getStartTime());
    ASSERT_EQ(startTime+timeout, terminator.getExpireTime());
    
    int32_t checkStep = 8;
    terminator.init(checkStep);

    ASSERT_TRUE(!terminator.checkTimeout());
    ASSERT_TRUE(!terminator.isTerminated());
    usleep(110000); // sleep 110ms
    
    for (int32_t i = 0; i < checkStep - 1; ++i) {
        ASSERT_TRUE(!terminator.checkTimeout());
        ASSERT_TRUE(!terminator.isTerminated()); // not checked
    }
    ASSERT_TRUE(terminator.checkTimeout());
    ASSERT_TRUE(terminator.isTerminated()); // after check
    ASSERT_EQ(checkStep + 1, terminator.getCheckTimes());
}

TEST_F(TimeoutTerminatorTest, testCheckTimes) { 
    HA3_LOG(DEBUG, "Begin Test!");

    int64_t timeout = 10000000; // 10s, never timeout
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_TRUE(!terminator.isTerminated());

    int32_t checkStep = 16;
    terminator.init(checkStep);

    uint32_t expectCheckTimes = 100;
    for (size_t i = 0; i < expectCheckTimes; ++i) {
        ASSERT_TRUE(!terminator.checkTimeout());
        ASSERT_TRUE(!terminator.isTerminated());
    }
    ASSERT_EQ(int32_t(100), terminator.getCheckTimes());
}

TEST_F(TimeoutTerminatorTest, testCheckNotTimeoutButTerminated) {
    int64_t timeout = 10000000; // 10s, never timeout
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    int32_t checkStep = 4;
    terminator.init(checkStep);

    terminator.setTerminated(true);

    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(terminator.checkTimeout());
        ASSERT_TRUE(terminator.isTerminated());
    }
}

TEST_F(TimeoutTerminatorTest, testCheckRestrictTimeout) { 
    HA3_LOG(DEBUG, "Begin Test!");

    int64_t timeout = 100000; // 100ms
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_TRUE(!terminator.checkRestrictorTimeout());
    ASSERT_TRUE(!terminator.isTerminated());
    usleep(110000); // sleep 110ms
    int32_t checkStep = TimeoutTerminator::RESTRICTOR_CHECK_MASK + 1;
    for (int32_t i = 0; i < checkStep - 1 ; ++i) {
        ASSERT_TRUE(!terminator.checkRestrictorTimeout());
        ASSERT_TRUE(!terminator.isTerminated()); // not checked
    }
    ASSERT_TRUE(terminator.checkRestrictorTimeout());
    ASSERT_TRUE(terminator.isTerminated()); // after check
    ASSERT_EQ(checkStep + 1, terminator.getRestrictorCheckTimes());
}

END_HA3_NAMESPACE(common);

