#include "build_service/util/StreamQueue.h"

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

class StreamQueueTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

public:
    static void pushThread(StreamQueue<int32_t>* queue, int32_t count);
    static void popThread(StreamQueue<int32_t>* queue, int32_t count);
};

void StreamQueueTest::setUp() {}

void StreamQueueTest::tearDown() {}

TEST_F(StreamQueueTest, testSimple)
{
    StreamQueue<int32_t> queue;
    for (int32_t i = 0; i < 10; ++i) {
        queue.push(i);
    }
    for (int32_t i = 0; i < 10; ++i) {
        int32_t value;
        ASSERT_TRUE(queue.pop(value));
        ASSERT_EQ(i, value);
    }
}

TEST_F(StreamQueueTest, testMultiThread)
{
    StreamQueue<int32_t> queue;
    ThreadPtr pushThreadPtr = Thread::createThread(std::bind(&StreamQueueTest::pushThread, &queue, 100));
    ThreadPtr popThreadPtr = Thread::createThread(std::bind(&StreamQueueTest::popThread, &queue, 100));
    pushThreadPtr->join();
    popThreadPtr->join();
}

void StreamQueueTest::pushThread(StreamQueue<int32_t>* queue, int32_t count)
{
    for (int32_t i = 0; i < count; ++i) {
        usleep(rand() % 100000);
        queue->push(i);
    }
    queue->setFinish();
}

void StreamQueueTest::popThread(StreamQueue<int32_t>* queue, int32_t count)
{
    for (int32_t i = 0; i < 10; ++i) {
        usleep(rand() % 100000);
        int32_t value;
        ASSERT_TRUE(queue->pop(value));
        ASSERT_EQ(i, value);
    }
}

}} // namespace build_service::util
