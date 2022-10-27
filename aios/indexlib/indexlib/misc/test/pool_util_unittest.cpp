#include "indexlib/misc/test/pool_util_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, PoolUtilTest);

PoolUtilTest::PoolUtilTest()
{
}

PoolUtilTest::~PoolUtilTest()
{
}

void PoolUtilTest::CaseSetUp()
{
}

void PoolUtilTest::CaseTearDown()
{
}

void PoolUtilTest::TestSimpleProcess()
{
    auto pool = new autil::mem_pool::Pool(1024);
    char* buffer = (char*)pool->allocate(4);
    ASSERT_FALSE(nullptr == buffer);
    IE_POOL_COMPATIBLE_DELETE_CLASS(pool, buffer);
    ASSERT_TRUE(nullptr == buffer);
    delete pool;
}

IE_NAMESPACE_END(misc);
