#include "indexlib/index/common/TypedSliceList.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib::index {

class TypedSliceListTest : public INDEXLIB_TESTBASE
{
public:
    TypedSliceListTest();
    ~TypedSliceListTest();

    DECLARE_CLASS_NAME(TypedSliceListTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestPushBackTypedValue();
    void TestUpdate();
    void TestRead();
    void TestOperatorBracket();

private:
    autil::mem_pool::Pool _byteSlicePool;
};
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestPushBackTypedValue);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestUpdate);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(TypedSliceListTest, TestOperatorBracket);

TypedSliceListTest::TypedSliceListTest() {}

TypedSliceListTest::~TypedSliceListTest() {}

void TypedSliceListTest::CaseSetUp() {}

void TypedSliceListTest::CaseTearDown() {}

void TypedSliceListTest::TestPushBackTypedValue()
{
    TypedSliceList<uint32_t> accessor(2, 8, &_byteSlicePool);
    ASSERT_TRUE(accessor._data != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)10, *(uint32_t*)accessor._data->Search(0));
    ASSERT_EQ((uint32_t)20, *(uint32_t*)accessor._data->Search(1));
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor._data->Search(2));
}

void TypedSliceListTest::TestOperatorBracket()
{
    TypedSliceList<uint32_t> accessor(2, 8, &_byteSlicePool);
    ASSERT_TRUE(accessor._data != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor._data->Search(2));
    accessor[2] = 333;
    ASSERT_EQ((uint32_t)333, *(uint32_t*)accessor._data->Search(2));
}

void TypedSliceListTest::TestUpdate()
{
    TypedSliceList<uint32_t> accessor(2, 8, &_byteSlicePool);
    ASSERT_TRUE(accessor._data != NULL);
    accessor.PushBack(10);
    accessor.PushBack(20);
    accessor.PushBack(30);
    ASSERT_EQ((uint32_t)30, *(uint32_t*)accessor._data->Search(2));
    accessor.Update(2, 333);
    ASSERT_EQ((uint32_t)333, *(uint32_t*)accessor._data->Search(2));
}

void TypedSliceListTest::TestRead()
{
    TypedSliceList<uint32_t> accessor(2, 8, &_byteSlicePool);
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
} // namespace indexlib::index
