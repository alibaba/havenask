#include <autil/mem_pool/pool_allocator.h>

#include "indexlib/util/test/simple_pool_unittest.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SimplePoolTest);

SimplePoolTest::SimplePoolTest()
{
}

SimplePoolTest::~SimplePoolTest()
{
}

void SimplePoolTest::CaseSetUp()
{
}

void SimplePoolTest::CaseTearDown()
{
}

void SimplePoolTest::TestSimpleProcess()
{ 
    SimplePool pool;
    ASSERT_EQ((size_t)0, pool.getUsedBytes());
    void* p = pool.allocate(100);
    ASSERT_TRUE(p);
    ASSERT_EQ((size_t)100, pool.getUsedBytes());
    ASSERT_EQ((size_t)100, pool.getPeakOfUsedBytes());
    pool.deallocate(p, 100);
    ASSERT_EQ((size_t)0, pool.getUsedBytes());
    ASSERT_EQ((size_t)100, pool.getPeakOfUsedBytes());
}

void SimplePoolTest::TestAbnormalCase()
{
    SimplePool pool;
    void* p = pool.allocate(0);
    ASSERT_TRUE(!p);
    pool.deallocate(NULL, 10);
}

void SimplePoolTest::TestPoolUtil()
{
    unique_ptr<SimplePool> pool(new SimplePool());
    {
        SimplePool* p = IE_POOL_COMPATIBLE_NEW_CLASS(pool.get(), SimplePool);
        ASSERT_TRUE(p);
        ASSERT_EQ(sizeof(SimplePool), pool->getUsedBytes());
        IE_POOL_COMPATIBLE_DELETE_CLASS(pool.get(), p);
        ASSERT_EQ((size_t)0, pool->getUsedBytes());
    }
    {
        SimplePool* nullPool = NULL;
        SimplePool* p = IE_POOL_COMPATIBLE_NEW_CLASS(nullPool, SimplePool);
        ASSERT_TRUE(p);
        IE_POOL_COMPATIBLE_DELETE_CLASS(nullPool, p);
    }
    {
        int32_t* p = IE_POOL_COMPATIBLE_NEW_VECTOR(pool.get(), int32_t, 100);
        ASSERT_TRUE(p);
        ASSERT_EQ(sizeof(int32_t) * 100, pool->getUsedBytes());
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool.get(), p, 100);
        ASSERT_EQ((size_t)0, pool->getUsedBytes());
    }
    {
        SimplePool* nullPool = NULL;
        int32_t* p = IE_POOL_COMPATIBLE_NEW_VECTOR(nullPool, int32_t, 100);
        ASSERT_TRUE(p);
        IE_POOL_COMPATIBLE_DELETE_VECTOR(nullPool, p, 100);
    }
}

void SimplePoolTest::TestSimplePoolAllocator()
{
    SimplePool pool;
    pool_allocator<int64_t> allocator(&pool);
    {
        vector<int64_t, pool_allocator<int64_t> > vec(allocator);
        vec.reserve(100);
        ASSERT_EQ((size_t)100 * sizeof(int64_t), pool.getUsedBytes());
        ASSERT_EQ((size_t)100 * sizeof(int64_t), pool.getPeakOfUsedBytes());
        vec.reserve(200);
        ASSERT_EQ((size_t)200 * sizeof(int64_t), pool.getUsedBytes());
        ASSERT_EQ((size_t)300 * sizeof(int64_t), pool.getPeakOfUsedBytes());
        vector<int64_t, pool_allocator<int64_t> >(allocator).swap(vec);
        ASSERT_EQ((size_t)0, pool.getUsedBytes());
    }
    ASSERT_EQ((size_t)0, pool.getUsedBytes());
}

IE_NAMESPACE_END(util);

