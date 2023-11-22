#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

#include <algorithm>
#include <random>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
namespace indexlib { namespace util {

class BlockMemoryQuotaControllerTest : public INDEXLIB_TESTBASE
{
public:
    BlockMemoryQuotaControllerTest();
    ~BlockMemoryQuotaControllerTest();

    DECLARE_CLASS_NAME(BlockMemoryQuotaControllerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAllocAndFree();
    void TestMultiThreadAllocAndFree();
    void TestReserve();
    void TestShrink();

private:
    void Write()
    {
        while (!_isRun) {
            usleep(0);
        }
        DoWrite();
    }
    void Read(int* status)
    {
        while (!_isRun) {
            usleep(0);
        }
        DoRead(status);
    }
    void DoMultiThreadTest(size_t readThreadNum, int64_t duration);
    void DoWrite();
    void DoRead(int* status);
    bool IsFinished() { return _isFinish; }
    void FillAllocSizeVector(std::vector<int64_t>& vec, size_t size, int64_t total);

private:
    int64_t _totalMemory;
    MemoryQuotaControllerPtr _mc;
    PartitionMemoryQuotaControllerPtr _pmc;
    BlockMemoryQuotaControllerPtr _bmc;
    atomic_bool _isFinish;
    atomic_bool _isRun;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestAllocAndFree);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestMultiThreadAllocAndFree);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestReserve);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestShrink);

AUTIL_LOG_SETUP(indexlib.util, BlockMemoryQuotaControllerTest);

BlockMemoryQuotaControllerTest::BlockMemoryQuotaControllerTest() {}

BlockMemoryQuotaControllerTest::~BlockMemoryQuotaControllerTest() {}

void BlockMemoryQuotaControllerTest::CaseSetUp()
{
    _totalMemory = 1024 * 1024 * 1024;
    _mc.reset(new MemoryQuotaController(_totalMemory));
    _pmc.reset(new PartitionMemoryQuotaController(_mc));
}

void BlockMemoryQuotaControllerTest::CaseTearDown()
{
    _bmc.reset();
    _pmc.reset();
    _mc.reset();
}

inline void BlockMemoryQuotaControllerTest::DoMultiThreadTest(size_t readThreadNum, int64_t duration)
{
    std::vector<int> status(readThreadNum, 0);
    _isFinish = false;
    _isRun = false;

    std::vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < readThreadNum; ++i) {
        autil::ThreadPtr thread =
            autil::Thread::createThread(std::bind(&BlockMemoryQuotaControllerTest::Read, this, &status[i]));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(std::bind(&BlockMemoryQuotaControllerTest::Write, this));

    _isRun = true;
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < duration) {
        sleep(1);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
    _isFinish = true;

    for (size_t i = 0; i < readThreadNum; ++i) {
        readThreads[i].reset();
    }
    writeThread.reset();
    for (size_t i = 0; i < readThreadNum; ++i) {
        ASSERT_EQ((int)0, status[i]);
    }
}
void BlockMemoryQuotaControllerTest::FillAllocSizeVector(vector<int64_t>& vec, size_t size, int64_t total)
{
    vec.clear();
    vec.reserve(size);
    random_device rd;
    mt19937 mt(rd());
    for (size_t i = 1; i < size; ++i) {
        int64_t x = mt() % total;
        total -= x;
        vec.push_back(x);
    }
    vec.push_back(total);
    shuffle(vec.begin(), vec.end(), mt);
}

void BlockMemoryQuotaControllerTest::DoWrite()
{
    size_t count = 10;
    vector<int64_t> allocs;
    while (!IsFinished()) {
        FillAllocSizeVector(allocs, count, _totalMemory / 10);
        for (auto a : allocs) {
            _bmc->Reserve(a);
        }
        _bmc->ShrinkToFit();
    }
}

void BlockMemoryQuotaControllerTest::DoRead(int* status)
{
    size_t allocCount = 10;
    vector<int64_t> allocs;
    vector<int64_t> frees;
    while (!IsFinished()) {
        FillAllocSizeVector(allocs, allocCount, _totalMemory / 10);
        FillAllocSizeVector(frees, allocCount, _totalMemory / 10);
        for (auto a : allocs) {
            _bmc->Allocate(a);
        }
        _bmc->Free(0);
        for (auto f : frees) {
            _bmc->Free(f);
        }
        return;
    }
}

void BlockMemoryQuotaControllerTest::TestAllocAndFree()
{
    // Simple
    _bmc.reset(new BlockMemoryQuotaController(_pmc));
    _bmc->Allocate(324);
    ASSERT_TRUE(_bmc->GetFreeQuota() <= _totalMemory - 324);
    ASSERT_EQ(_bmc->GetFreeQuota(), _mc->GetFreeQuota());
    _bmc->Free(324);
    _bmc.reset();
    ASSERT_EQ(0, _pmc->GetUsedQuota());

    _bmc.reset(new BlockMemoryQuotaController(_pmc));
    vector<int64_t> allocs = {10, 100, 10000, 1000000};
    vector<int64_t> frees = {5, 5, 50, 50, 5000, 5000, 500000, 500000};
    for (auto a : allocs) {
        _bmc->Allocate(a);
    }
    _bmc->Free(0);
    for (auto f : frees) {
        _bmc->Free(f);
    }
    _bmc->Free(0);
    ASSERT_EQ(0, _pmc->GetUsedQuota());
}

void BlockMemoryQuotaControllerTest::TestMultiThreadAllocAndFree()
{
    _bmc.reset(new BlockMemoryQuotaController(_pmc));
    DoMultiThreadTest(9, 3);
    _bmc.reset();
    ASSERT_EQ(0, _pmc->GetUsedQuota());
}

void BlockMemoryQuotaControllerTest::TestReserve()
{
    _bmc.reset(new BlockMemoryQuotaController(_pmc));
    _bmc->Reserve(24);
    int64_t freeQuota = _bmc->GetFreeQuota();
    ASSERT_TRUE(freeQuota <= _totalMemory - 24);
    _bmc->Allocate(10);
    ASSERT_EQ(freeQuota, _bmc->GetFreeQuota());
    _bmc->Allocate(10);
    ASSERT_EQ(freeQuota, _bmc->GetFreeQuota());
    _bmc->Free(20);
    _bmc.reset();
    ASSERT_EQ(0, _pmc->GetUsedQuota());
}

void BlockMemoryQuotaControllerTest::TestShrink()
{
    int64_t blockSize = BlockMemoryQuotaController::BLOCK_SIZE;

    _bmc.reset(new BlockMemoryQuotaController(_pmc));
    _bmc->Reserve(blockSize + 1234);
    _bmc->Allocate(100);
    ASSERT_TRUE(_bmc->GetUsedQuota() >= blockSize + 1234);
    _bmc->ShrinkToFit();
    ASSERT_EQ(blockSize, _pmc->GetUsedQuota());
    ASSERT_EQ(_totalMemory - blockSize, _bmc->GetFreeQuota());
    _bmc->Free(100);

    _bmc->Reserve(3 * blockSize);
    ASSERT_EQ(3 * blockSize, _bmc->GetUsedQuota());
    _bmc->ShrinkToFit();
    ASSERT_EQ(0, _bmc->GetUsedQuota());

    _bmc.reset();
}
}} // namespace indexlib::util
