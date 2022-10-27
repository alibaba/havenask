#include "indexlib/common/perf_test/radix_tree_perftest.h"
#include <autil/TimeUtility.h>

using namespace autil;
using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RadixTreePerfTest);

RadixTreePerfTest::RadixTreePerfTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
}

RadixTreePerfTest::~RadixTreePerfTest()
{
    delete mByteSlicePool;
}

void RadixTreePerfTest::CaseSetUp()
{
}

void RadixTreePerfTest::CaseTearDown()
{
    mByteSlicePool->release();
}

void RadixTreePerfTest::TestAppendData()
{
    //100000000
    //RadixTree(16, 16, x, x); 5s
    //RadixTree(8, 8, x, x); 6s
    //RadixTree(32, 32, x, x); 4s
    //RadixTree(64, 64, x, x); 3s
    //RadixTree(128, 128, x, x); 3s
    //RadixTree(16, 16*1024, x, x); 3s
    //RadixTree(128, 16*1024, x, x); 2s
    void* buffer = mByteSlicePool->allocate(sizeof(RadixTree));
    RadixTree* tree = new (buffer) RadixTree(128, 16*1024, mByteSlicePool, sizeof(uint32_t));
    for (uint32_t i = 0; i < 100000000; i++)
    {
        Append(tree, i);
    }
}

IE_NAMESPACE_END(common);

