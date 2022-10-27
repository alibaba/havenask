#include "indexlib/util/test/mmap_vector_unittest.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MMapVectorTest);

MMapVectorTest::MMapVectorTest()
{
}

MMapVectorTest::~MMapVectorTest()
{
}

void MMapVectorTest::CaseSetUp()
{
}

void MMapVectorTest::CaseTearDown()
{
}

void MMapVectorTest::TestSimpleProcess()
{
    MMapAllocatorPtr allocator(new MMapAllocator);
    MMapVector<uint32_t> vec;

    uint32_t capacity = 100;
    vec.Init(allocator, capacity);
    for (size_t i = 0; i < capacity; i++)
    {
        vec.PushBack(i);
        ASSERT_EQ(i, vec[i]);
        ASSERT_EQ(i + 1, vec.Size());
        ASSERT_EQ((i+1)*sizeof(uint32_t), vec.GetUsedBytes());
    }
}

void MMapVectorTest::TestPushBack()
{
    MMapAllocatorPtr allocator(new MMapAllocator);
    MMapVector<uint32_t> vec;
    vec.Init(allocator, 1);

    vec.PushBack(0);
    ASSERT_EQ((uint32_t)0, vec[0]);
    ASSERT_THROW(vec.PushBack(1), UnSupportedException);
}

void MMapVectorTest::TestEqual()
{
    MMapAllocatorPtr allocator(new MMapAllocator);
    MMapVector<uint32_t> vec1;
    MMapVector<uint32_t> vec2;

    vec1.Init(allocator, 10);
    vec2.Init(allocator, 20);

    for (size_t i = 0; i < 10; i++)
    {
        vec1.PushBack(i);
        vec2.PushBack(i);
    }
    
    ASSERT_EQ(vec1, vec2);
    vec1[0] = 20;
    ASSERT_TRUE(vec1 != vec2);
}
IE_NAMESPACE_END(util);

