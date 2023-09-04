
#include "indexlib/index/common/ShortBuffer.h"

#include "indexlib/index/common/AtomicValueTyped.h"
#include "indexlib/index/common/MultiValue.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {
template <typename T>
class SimpleMultiValue : public MultiValue
{
public:
    SimpleMultiValue(size_t count)
    {
        for (size_t i = 0; i < count; ++i) {
            AtomicValue* value = new AtomicValueTyped<T>();
            value->SetLocation(i);
            value->SetOffset(i * sizeof(T));
            AddAtomicValue(value);
        }
    }
};

class ShortBufferTest : public INDEXLIB_TESTBASE
{
public:
    ShortBufferTest();
    ~ShortBufferTest();

    DECLARE_CLASS_NAME(ShortBufferTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestTwoRow();
    void TestMaxCapacity();
    void TestInvalidPushBack();
    void TestSizeChange();
    void TestClear();
    void TestDisorderPushBack();
    void TestMultiRowWithDifferentType();

private:
    void CheckShortBuffer(uint32_t* expectShortBuffer, size_t rowNum, size_t colNum, ShortBuffer* shortBuffer = NULL,
                          uint8_t* capacity = NULL);

    void CheckEqual(uint32_t* expectShortBuffer, size_t rowNum, size_t colNum, ShortBuffer* shortBuffer);
};

INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestTwoRow);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestMaxCapacity);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestInvalidPushBack);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestSizeChange);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestClear);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestDisorderPushBack);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestMultiRowWithDifferentType);

ShortBufferTest::ShortBufferTest() {}

ShortBufferTest::~ShortBufferTest() {}

void ShortBufferTest::CaseSetUp() {}

void ShortBufferTest::CaseTearDown() {}

void ShortBufferTest::TestSimpleProcess()
{
    SimpleMultiValue<uint32_t> multiValue(1);

    ShortBuffer shortBuffer;
    shortBuffer.Init((const MultiValue*)(&multiValue));

    ASSERT_EQ((uint8_t)0, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    ASSERT_TRUE(shortBuffer.PushBack(0, (uint32_t)100));
    shortBuffer.EndPushBack();
    ASSERT_EQ((uint8_t)2, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestTwoRow()
{
    uint32_t expectShortBuffer[] = {100, 101, 102, 200, 201, 202};
    CheckShortBuffer((uint32_t*)expectShortBuffer, 2, 3);
}

void ShortBufferTest::TestMaxCapacity()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(8);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    uint32_t expectShortBuffer[128 * 8];
    for (size_t c = 0; c < 128; ++c) {
        for (size_t r = 0; r < 8; ++r) {
            expectShortBuffer[c + r * 128] = random();
        }
    }
    CheckShortBuffer((uint32_t*)expectShortBuffer, 8, 128, &shortBuffer);

    shortBuffer.Clear();

    uint8_t capcity = 128;
    CheckShortBuffer((uint32_t*)expectShortBuffer, 8, 128, &shortBuffer, &capcity);
}

void ShortBufferTest::TestInvalidPushBack()
{
    {
        // push same row repeatedly
        ShortBuffer shortBuffer;
        SimpleMultiValue<uint32_t> multiValue(2);
        shortBuffer.Init(&multiValue);

        ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));
        shortBuffer.EndPushBack();
        ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(0, 200));
    }
    {
        // push when shortBuffer is full and hit max capacity
        ShortBuffer shortBuffer;
        SimpleMultiValue<uint32_t> multiValue(1);
        shortBuffer.Init((const MultiValue*)(&multiValue));

        for (size_t col = 0; col < 128; ++col) {
            ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(0, col));
            shortBuffer.EndPushBack();
        }
        ASSERT_TRUE(!shortBuffer.PushBack<uint32_t>(0, 128));
    }
}

void ShortBufferTest::TestSizeChange()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(2);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(1, 200));
    shortBuffer.EndPushBack();
    ASSERT_EQ((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestDisorderPushBack()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(3);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    ASSERT_EQ((uint8_t)0, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(2, 100));

    ASSERT_EQ((uint8_t)2, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));

    ASSERT_EQ((uint8_t)2, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    ASSERT_TRUE(shortBuffer.PushBack<uint32_t>(1, 100));
    shortBuffer.EndPushBack();
    ASSERT_EQ((uint8_t)2, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestClear()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(1);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    shortBuffer.PushBack<uint32_t>(0, 100);
    shortBuffer.EndPushBack();
    ASSERT_EQ(1, shortBuffer.Size());
    ASSERT_EQ(2, shortBuffer.Capacity());

    shortBuffer.Clear();
    ASSERT_EQ(0, shortBuffer.Size());
    ASSERT_EQ(2, shortBuffer.Capacity());
}

void ShortBufferTest::TestMultiRowWithDifferentType()
{
#define NUMBER_TYPE_HELPER_FOR_TEST(macro)                                                                             \
    macro(uint8_t);                                                                                                    \
    macro(int8_t);                                                                                                     \
    macro(uint16_t);                                                                                                   \
    macro(int16_t);                                                                                                    \
    macro(uint32_t);                                                                                                   \
    macro(int32_t);

    MultiValue multiValue;
    size_t offset = 0;
    size_t pos = 0;

#define ADD_VALUE_HELPER_FOR_TEST(type)                                                                                \
    {                                                                                                                  \
        AtomicValue* value = new AtomicValueTyped<type>();                                                             \
        value->SetLocation(pos++);                                                                                     \
        value->SetOffset(offset);                                                                                      \
        offset += sizeof(type);                                                                                        \
        multiValue.AddAtomicValue(value);                                                                              \
    }

    NUMBER_TYPE_HELPER_FOR_TEST(ADD_VALUE_HELPER_FOR_TEST);

    ShortBuffer shortBuffer;
    shortBuffer.Init(&multiValue);

    ASSERT_EQ((uint8_t)0, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer.Size());

    for (size_t i = 0; i < 128; ++i) {
        pos = 0;
#define TEST_PUSH_BACK_HELPER(type)                                                                                    \
    ASSERT_TRUE(shortBuffer.PushBack<type>(pos++, std::numeric_limits<type>::max() - i));

        NUMBER_TYPE_HELPER_FOR_TEST(TEST_PUSH_BACK_HELPER);
        shortBuffer.EndPushBack();
    }

    ASSERT_EQ((uint8_t)128, shortBuffer.Capacity());
    ASSERT_EQ((uint8_t)128, shortBuffer.Size());

    for (size_t i = 0; i < 128; ++i) {
        pos = 0;
#define ASSERT_GET_VALUE_HELPER(type)                                                                                  \
    ASSERT_EQ((type)(std::numeric_limits<type>::max() - i), shortBuffer.GetRowTyped<type>(pos++)[i]);

        NUMBER_TYPE_HELPER_FOR_TEST(ASSERT_GET_VALUE_HELPER);
    }
}

void ShortBufferTest::CheckShortBuffer(uint32_t* expectShortBuffer, size_t rowNum, size_t colNum,
                                       ShortBuffer* shortBuffer, uint8_t* capacity)
{
    std::unique_ptr<ShortBuffer> shortBufferPtr;
    SimpleMultiValue<uint32_t> multiValue(rowNum);
    if (!shortBuffer) {
        shortBuffer = new ShortBuffer;
        shortBufferPtr.reset(shortBuffer);
        shortBufferPtr->Init((const MultiValue*)(&multiValue));
    }
    uint8_t curCapacity = capacity == NULL ? 0 : *capacity;
    ASSERT_EQ(curCapacity, shortBuffer->Capacity());
    ASSERT_EQ((uint8_t)0, shortBuffer->Size());
    ASSERT_EQ(rowNum, shortBuffer->GetRowCount());

    for (size_t c = 0; c < colNum; ++c) {
        for (size_t r = 0; r < rowNum; ++r) {
            ASSERT_TRUE(shortBuffer->PushBack(r, expectShortBuffer[c + r * colNum]));
        }
        shortBuffer->EndPushBack();
        ASSERT_EQ((uint8_t)(c + 1), shortBuffer->Size());
        uint8_t curCapacity = capacity == NULL ? shortBuffer->AllocatePlan(c) : *capacity;
        ASSERT_EQ(curCapacity, shortBuffer->Capacity());
    }

    CheckEqual(expectShortBuffer, rowNum, colNum, shortBuffer);
}

void ShortBufferTest::CheckEqual(uint32_t* expectShortBuffer, size_t rowNum, size_t colNum, ShortBuffer* shortBuffer)
{
    ASSERT_EQ(rowNum, (size_t)shortBuffer->GetRowCount());
    ASSERT_EQ(colNum, (size_t)shortBuffer->Size());
    for (size_t r = 0; r < rowNum; ++r) {
        uint32_t* row = shortBuffer->GetRowTyped<uint32_t>(r);
        for (size_t c = 0; c < colNum; ++c) {
            ASSERT_EQ(expectShortBuffer[c + r * colNum], row[c]);
            uint32_t* row = shortBuffer->GetRowTyped<uint32_t>(r);
            ASSERT_EQ(expectShortBuffer[c + r * colNum], row[c]);
        }
    }
}

} // namespace indexlib::index
