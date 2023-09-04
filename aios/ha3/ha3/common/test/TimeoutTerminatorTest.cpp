#include "ha3/common/TimeoutTerminator.h"

#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/TimeUtility.h"
#include "unittest/unittest.h"

using namespace autil;

namespace isearch {
namespace common {

class TimeoutTerminatorTest : public TESTBASE {
public:
    TimeoutTerminatorTest();
    ~TimeoutTerminatorTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, TimeoutTerminatorTest);

TimeoutTerminatorTest::TimeoutTerminatorTest() {}

TimeoutTerminatorTest::~TimeoutTerminatorTest() {}

void TimeoutTerminatorTest::setUp() {}

void TimeoutTerminatorTest::tearDown() {}

TEST_F(TimeoutTerminatorTest, testSimpleProcess) {

    int64_t timeout = 100000; // 100ms
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_EQ(startTime, terminator.getStartTime());
    ASSERT_EQ(startTime + timeout, terminator.getExpireTime());

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

    int64_t timeout = 10000000; // 10s, never timeout
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_TRUE(!terminator.isTerminated());

    int32_t checkStep = TimeoutTerminator::DEFAULT_RESTRICTOR_CHECK_MASK + 1;
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

    int64_t timeout = 100000; // 100ms
    int64_t startTime = TimeUtility::currentTime();
    TimeoutTerminator terminator(timeout, startTime);

    ASSERT_TRUE(!terminator.checkRestrictTimeout());
    ASSERT_TRUE(!terminator.isTerminated());
    usleep(110000); // sleep 110ms
    int32_t checkStep = TimeoutTerminator::DEFAULT_RESTRICTOR_CHECK_MASK + 1;
    ;
    for (int32_t i = 0; i < checkStep - 1; ++i) {
        ASSERT_TRUE(!terminator.checkRestrictTimeout());
        ASSERT_TRUE(!terminator.isTerminated()); // not checked
    }
    ASSERT_TRUE(terminator.checkRestrictTimeout());
    ASSERT_TRUE(terminator.isTerminated()); // after check
    ASSERT_EQ(checkStep + 1, terminator.getRestrictorCheckTimes());
}

} // namespace common
} // namespace isearch
