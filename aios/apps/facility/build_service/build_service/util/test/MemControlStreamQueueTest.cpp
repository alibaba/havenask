#include "build_service/util/MemControlStreamQueue.h"

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <stdlib.h>
#include <unistd.h>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace build_service { namespace util {

class MemControlStreamQueueTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

public:
    static void pushThread(MemControlStreamQueue<int32_t>* queue, int32_t count);
    static void popThread(MemControlStreamQueue<int32_t>* queue, int32_t count);
};

void MemControlStreamQueueTest::setUp() {}

void MemControlStreamQueueTest::tearDown() {}

TEST_F(MemControlStreamQueueTest, testSimple)
{
    MemControlStreamQueue<int32_t> queue(1000, 1000);
    for (int32_t i = 0; i < 10; ++i) {
        queue.push(i, i);
    }
    for (int32_t i = 0; i < 10; ++i) {
        int32_t value;
        ASSERT_TRUE(queue.pop(value));
        ASSERT_EQ(i, value);
    }
}

TEST_F(MemControlStreamQueueTest, testMultiThread)
{
    MemControlStreamQueue<int32_t> queue(1000, 1000);
    ThreadPtr pushThreadPtr = Thread::createThread(std::bind(&MemControlStreamQueueTest::pushThread, &queue, 1000));
    ThreadPtr popThreadPtr = Thread::createThread(std::bind(&MemControlStreamQueueTest::popThread, &queue, 1000));
    pushThreadPtr->join();
    popThreadPtr->join();
}

void MemControlStreamQueueTest::pushThread(MemControlStreamQueue<int32_t>* queue, int32_t count)
{
    for (int32_t i = 0; i < count; ++i) {
        usleep(rand() % 1000);
        queue->push(i, 100);
    }
    queue->setFinish();
}

void MemControlStreamQueueTest::popThread(MemControlStreamQueue<int32_t>* queue, int32_t count)
{
    for (int32_t i = 0; i < count; ++i) {
        usleep(rand() % 1000);
        int32_t value;
        ASSERT_TRUE(queue->pop(value));
        auto sz = queue->size();
        ASSERT_TRUE(sz <= 10);
        ASSERT_EQ(i, value);
    }
}

}} // namespace build_service::util
