#include "indexlib/util/memory_control/WaitMemoryQuotaController.h"

#include "autil/Log.h"
#include "autil/Thread.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class WaitMemoryQuotaControllerTest : public INDEXLIB_TESTBASE
{
public:
    WaitMemoryQuotaControllerTest();
    ~WaitMemoryQuotaControllerTest();

    DECLARE_CLASS_NAME(WaitMemoryQuotaControllerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSimpleProcess2();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(WaitMemoryQuotaControllerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(WaitMemoryQuotaControllerTest, TestSimpleProcess2);
AUTIL_LOG_SETUP(indexlib.util, WaitMemoryQuotaControllerTest);

WaitMemoryQuotaControllerTest::WaitMemoryQuotaControllerTest() {}

WaitMemoryQuotaControllerTest::~WaitMemoryQuotaControllerTest() {}

void WaitMemoryQuotaControllerTest::CaseSetUp() {}

void WaitMemoryQuotaControllerTest::CaseTearDown() {}

void WaitMemoryQuotaControllerTest::TestSimpleProcess()
{
    std::vector<autil::ThreadPtr> threads;
    WaitMemoryQuotaController memController(1);

    int32_t sum = 0;
    for (int32_t threadIdx = 0; threadIdx < 4; ++threadIdx) {
        threads.emplace_back(autil::Thread::createThread([&memController, &sum]() {
            for (int i = 0; i < 25; ++i) {
                memController.Allocate(1);
                sum += 1;
                memController.Free(1);
            }
        }));
    }
    for (const autil::ThreadPtr& thread : threads) {
        thread->join();
    }
    ASSERT_EQ(100, sum);
}

void WaitMemoryQuotaControllerTest::TestSimpleProcess2()
{
    std::vector<autil::ThreadPtr> threads;
    int64_t quota = 4;
    WaitMemoryQuotaController memController(quota);

    std::atomic<int32_t> sum = 0;
    for (int32_t threadIdx = 0; threadIdx < quota + 3; ++threadIdx) {
        threads.emplace_back(autil::Thread::createThread([&memController, &sum, &quota]() {
            for (int i = 0; i < 100; ++i) {
                memController.Allocate(1);
                // use EXPECT_ instead ASSERT_ to avoid dead-lock
                // use _LT instead of LE for fetch_add() return old value
                EXPECT_LT(sum.fetch_add(1), quota);
                usleep(2000);
                sum.fetch_sub(1);
                memController.Free(1);
            }
        }));
    }
    for (const autil::ThreadPtr& thread : threads) {
        thread->join();
    }
    ASSERT_EQ(0, sum);
}

}} // namespace indexlib::util
