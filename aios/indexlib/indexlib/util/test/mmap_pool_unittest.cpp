#include "indexlib/util/test/mmap_pool_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MmapPoolTest);

MmapPoolTest::MmapPoolTest()
{
}

MmapPoolTest::~MmapPoolTest()
{
}

void MmapPoolTest::CaseSetUp()
{
}

void MmapPoolTest::CaseTearDown()
{
}

void MmapPoolTest::TestSimpleProcess()
{
    MmapPool pool;
    uint8_t* buffer = (uint8_t*) pool.allocate(1024);
    memset(buffer, 0, 1024);
    pool.deallocate(buffer, 1024);
}

IE_NAMESPACE_END(util);

