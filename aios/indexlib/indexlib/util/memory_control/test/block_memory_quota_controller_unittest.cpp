#include "indexlib/util/memory_control/test/block_memory_quota_controller_unittest.h"
#include <autil/ThreadPool.h>
#include <random>
#include <algorithm>

using namespace std;
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BlockMemoryQuotaControllerTest);

BlockMemoryQuotaControllerTest::BlockMemoryQuotaControllerTest()
{
}

BlockMemoryQuotaControllerTest::~BlockMemoryQuotaControllerTest()
{
}

void BlockMemoryQuotaControllerTest::CaseSetUp()
{
    mTotalMemory = 1024 * 1024 * 1024;
    mMc.reset(new MemoryQuotaController(mTotalMemory));
    mPmc.reset(new PartitionMemoryQuotaController(mMc));
}

void BlockMemoryQuotaControllerTest::CaseTearDown()
{
    mBmc.reset();
    mPmc.reset();
    mMc.reset();
}

void BlockMemoryQuotaControllerTest::FillAllocSizeVector(
        vector<int64_t>& vec, size_t size, int64_t total)
{
    vec.clear();
    vec.reserve(size);
    random_device rd;
    mt19937 mt(rd());
    for (size_t i = 1 ; i < size; ++i)
    {
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
    while (!IsFinished())
    {
        FillAllocSizeVector(allocs, count, mTotalMemory / 10);
        for (auto a : allocs)
        {
            mBmc->Reserve(a);
        }
        mBmc->ShrinkToFit();
    }
}

void BlockMemoryQuotaControllerTest::DoRead(int* status)
{
    size_t allocCount = 10;
    vector<int64_t> allocs;
    vector<int64_t> frees;
    while (!IsFinished())
    {
        FillAllocSizeVector(allocs, allocCount, mTotalMemory / 10);
        FillAllocSizeVector(frees, allocCount, mTotalMemory / 10);
        for(auto a : allocs)
        {
            mBmc->Allocate(a);
        }
        mBmc->Free(0);
        for(auto f : frees)
        {
            mBmc->Free(f);
        }
        return ;
    }
}

void BlockMemoryQuotaControllerTest::TestAllocAndFree()
{

    // Simple
    mBmc.reset(new BlockMemoryQuotaController(mPmc));
    mBmc->Allocate(324);
    ASSERT_TRUE(mBmc->GetFreeQuota() <= mTotalMemory - 324);
    ASSERT_EQ(mBmc->GetFreeQuota(), mMc->GetFreeQuota());
    mBmc->Free(324);
    mBmc.reset();
    ASSERT_EQ(0, mPmc->GetUsedQuota());

    mBmc.reset(new BlockMemoryQuotaController(mPmc));
    vector<int64_t> allocs = {10, 100, 10000, 1000000};
    vector<int64_t> frees  = {5, 5, 50, 50, 5000, 5000, 500000, 500000};
    for (auto a : allocs)
    {
        mBmc->Allocate(a);
    }
    mBmc->Free(0);
    for (auto f : frees)
    {
        mBmc->Free(f);
    }
    mBmc->Free(0);
    ASSERT_EQ(0, mPmc->GetUsedQuota());
}


void BlockMemoryQuotaControllerTest::TestMultiThreadAllocAndFree()
{
    mBmc.reset(new BlockMemoryQuotaController(mPmc));
    DoMultiThreadTest(9, 5);
    mBmc.reset();
    ASSERT_EQ(0, mPmc->GetUsedQuota());
}

void BlockMemoryQuotaControllerTest::TestReserve()
{
    mBmc.reset(new BlockMemoryQuotaController(mPmc));
    mBmc->Reserve(24);
    int64_t freeQuota = mBmc->GetFreeQuota();
    ASSERT_TRUE(freeQuota <= mTotalMemory - 24);
    mBmc->Allocate(10);
    ASSERT_EQ(freeQuota, mBmc->GetFreeQuota());
    mBmc->Allocate(10);
    ASSERT_EQ(freeQuota, mBmc->GetFreeQuota());
    mBmc->Free(20);
    mBmc.reset();
    ASSERT_EQ(0, mPmc->GetUsedQuota());
}

void BlockMemoryQuotaControllerTest::TestShrink()
{
    int64_t blockSize = BlockMemoryQuotaController::BLOCK_SIZE;

    mBmc.reset(new BlockMemoryQuotaController(mPmc));
    mBmc->Reserve(blockSize + 1234);
    mBmc->Allocate(100);
    ASSERT_TRUE(mBmc->GetUsedQuota() >= blockSize + 1234);
    mBmc->ShrinkToFit();
    ASSERT_EQ(blockSize, mPmc->GetUsedQuota());
    ASSERT_EQ(mTotalMemory - blockSize, mBmc->GetFreeQuota());
    mBmc->Free(100);

    mBmc->Reserve(3 * blockSize);
    ASSERT_EQ(3 * blockSize, mBmc->GetUsedQuota());
    mBmc->ShrinkToFit();
    ASSERT_EQ(0, mBmc->GetUsedQuota());

    mBmc.reset();
}

IE_NAMESPACE_END(comon);

