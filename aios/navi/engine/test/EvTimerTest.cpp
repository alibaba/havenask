#include "unittest/unittest.h"
#include "navi/engine/EvTimer.h"
#include "navi/util/CommonUtil.h"

using namespace std;
using namespace testing;

namespace navi {

class EvTimerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void EvTimerTest::setUp() {
}

void EvTimerTest::tearDown() {
}

atomic64_t startCount;
atomic64_t stopCount;
atomic64_t callbackCount;
static void callback(void *data, bool isTimeout) {
    atomic_inc(&callbackCount);
}

TEST_F(EvTimerTest, testSimple) {
    atomic_set(&startCount, 0);
    atomic_set(&stopCount, 0);
    atomic_set(&callbackCount, 0);
    EvTimer evTimer("navi_timer");
    ASSERT_TRUE(evTimer.start());
    std::vector<autil::ThreadPtr> threads;
    size_t loopCount = 1000;
    size_t threadCount = 10;
    auto func = std::bind([&]() {
        std::vector<Timer *> timers;
        for (size_t i = 0; i < loopCount; i++) {
            auto timeoutSecond = 0.001 + 0.0001 * ((CommonUtil::random64() % 100) - 50);
            auto timer = evTimer.addTimer(timeoutSecond, callback, nullptr);
            ASSERT_TRUE(timer);
            atomic_inc(&startCount);
            if (CommonUtil::random64() % 2) {
                timers.push_back(timer);
            } else {
                usleep(CommonUtil::random64() % 1020);
                evTimer.updateTimer(timer, timeoutSecond / 2);
                usleep(CommonUtil::random64() % 1020);
                evTimer.stopTimer(timer);
                atomic_inc(&stopCount);
            }
            usleep(CommonUtil::random64() % 10);
        }
        for (auto timer : timers) {
            evTimer.stopTimer(timer);
            atomic_inc(&stopCount);
        }
    });
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(func, "timer_test_thread_" + std::to_string(i)));
    }
    threads.clear();
    evTimer.stop();
    EXPECT_EQ(threadCount * loopCount, atomic_read(&startCount));
    EXPECT_EQ(threadCount * loopCount, atomic_read(&stopCount));
    EXPECT_EQ(threadCount * loopCount, atomic_read(&callbackCount));
}

TEST_F(EvTimerTest, testStop) {
    atomic_set(&callbackCount, 0);
    EvTimer evTimer("navi_timer");
    ASSERT_TRUE(evTimer.start());
    std::vector<Timer *> timers;
    for (size_t i = 0; i < 10; i++) {
        auto timer = evTimer.addTimer(1, callback, nullptr);
        ASSERT_TRUE(timer);
        timers.push_back(timer);
    }
    for (auto timer : timers) {
        evTimer.updateTimer(timer, 0.1);
    }
    while (10 != atomic_read(&callbackCount)) {
        usleep(1000);
    }
    for (auto timer : timers) {
        evTimer.stopTimer(timer);
    }
    evTimer.stop();
}

}
