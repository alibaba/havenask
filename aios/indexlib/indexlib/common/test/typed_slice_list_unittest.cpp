#include "indexlib/common/test/typed_slice_list_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, TypedSliceListTest);

TypedSliceListTest::TypedSliceListTest()
{
}

TypedSliceListTest::~TypedSliceListTest()
{
}

void TypedSliceListTest::CaseSetUp()
{
}

void TypedSliceListTest::CaseTearDown()
{
}

void TypedSliceListTest::TestPushBackTypedValue()
{    
    TypedSliceList<uint32_t> accessor(2, 8, &mByteSlicePool);
    ASSERT_TRUE(accessor.mData != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)10, *(uint32_t*)accessor.mData->Search(0));
    ASSERT_EQ((uint32_t)20, *(uint32_t*)accessor.mData->Search(1));
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor.mData->Search(2));
}

void TypedSliceListTest::TestOperatorBracket()
{
    TypedSliceList<uint32_t> accessor(2, 8, &mByteSlicePool);
    ASSERT_TRUE(accessor.mData != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor.mData->Search(2));
    accessor[2] = 333;
    ASSERT_EQ((uint32_t)333, *(uint32_t*)accessor.mData->Search(2));
}

void TypedSliceListTest::TestUpdate()
{
    TypedSliceList<uint32_t> accessor(2, 8, &mByteSlicePool);
    ASSERT_TRUE(accessor.mData != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor.mData->Search(2));
    accessor.Update(2, 333);
    ASSERT_EQ((uint32_t)333, *(uint32_t*)accessor.mData->Search(2));
}

void TypedSliceListTest::TestRead()
{
    TypedSliceList<uint32_t> accessor(2, 8, &mByteSlicePool);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);

    uint32_t value;
    accessor.Read(0, value);
    ASSERT_EQ((uint32_t)10, value); 

    accessor.Read(1, value);
    ASSERT_EQ((uint32_t)20, value);

    accessor.Read(2, value);
    ASSERT_EQ((uint32_t)30, value);
}

IE_NAMESPACE_END(common);

