#include "indexlib/common/test/short_buffer_unittest.h"
#include "indexlib/common/atomic_value_typed.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShortBufferTest);

ShortBufferTest::ShortBufferTest()
{
}

ShortBufferTest::~ShortBufferTest()
{
}

void ShortBufferTest::CaseSetUp()
{
}

void ShortBufferTest::CaseTearDown()
{
}

void ShortBufferTest::TestSimpleProcess()
{
    SimpleMultiValue<uint32_t> multiValue(1);

    ShortBuffer shortBuffer;
    shortBuffer.Init((const MultiValue*)(&multiValue));

    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack(0, (uint32_t)100));
    shortBuffer.EndPushBack();
    INDEXLIB_TEST_EQUAL((uint8_t)2, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestTwoRow()
{
    uint32_t expectShortBuffer[] = {
        100, 101, 102,
        200, 201, 202
    };
    CheckShortBuffer((uint32_t*)expectShortBuffer, 2, 3);
}

void ShortBufferTest::TestMaxCapacity()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(8);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    uint32_t expectShortBuffer[128 * 8];
    for (size_t c = 0; c < 128; ++c)
    {
        for (size_t r = 0; r < 8; ++r)
        {
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

        INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));
        shortBuffer.EndPushBack();
        INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(0, 200));
    }
    {
        // push when shortBuffer is full and hit max capacity
        ShortBuffer shortBuffer;
        SimpleMultiValue<uint32_t> multiValue(1);
        shortBuffer.Init((const MultiValue*)(&multiValue));

        for (size_t col = 0; col < 128; ++col)
        {
            INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(0, col));
            shortBuffer.EndPushBack();
        }
        INDEXLIB_TEST_TRUE(!shortBuffer.PushBack<uint32_t>(0, 128));
    }
}

void ShortBufferTest::TestSizeChange()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(2);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(1, 200));
    shortBuffer.EndPushBack();
    INDEXLIB_TEST_EQUAL((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestDisorderPushBack()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(3);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(2, 100));

    INDEXLIB_TEST_EQUAL((uint8_t)2, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(0, 100));

    INDEXLIB_TEST_EQUAL((uint8_t)2, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    INDEXLIB_TEST_TRUE(shortBuffer.PushBack<uint32_t>(1, 100));
    shortBuffer.EndPushBack();
    INDEXLIB_TEST_EQUAL((uint8_t)2, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)1, shortBuffer.Size());
}

void ShortBufferTest::TestClear()
{
    ShortBuffer shortBuffer;
    SimpleMultiValue<uint32_t> multiValue(1);
    shortBuffer.Init((const MultiValue*)(&multiValue));

    shortBuffer.PushBack<uint32_t>(0, 100);
    shortBuffer.EndPushBack();
    INDEXLIB_TEST_EQUAL(1, shortBuffer.Size());
    INDEXLIB_TEST_EQUAL(2, shortBuffer.Capacity());

    shortBuffer.Clear();
    INDEXLIB_TEST_EQUAL(0, shortBuffer.Size());
    INDEXLIB_TEST_EQUAL(2, shortBuffer.Capacity());
}

void ShortBufferTest::TestMultiRowWithDifferentType()
{
#define NUMBER_TYPE_HELPER_FOR_TEST(macro)      \
    macro(uint8_t);                             \
    macro(int8_t);                              \
    macro(uint16_t);                            \
    macro(int16_t);                             \
    macro(uint32_t);                            \
    macro(int32_t);

    MultiValue multiValue;
    size_t offset = 0;
    size_t pos = 0;

#define ADD_VALUE_HELPER_FOR_TEST(type)                         \
    {                                                           \
        AtomicValue* value = new AtomicValueTyped<type>();      \
        value->SetLocation(pos++);                              \
        value->SetOffset(offset);                               \
        offset += sizeof(type);                                 \
        multiValue.AddAtomicValue(value);                       \
    }

    NUMBER_TYPE_HELPER_FOR_TEST(ADD_VALUE_HELPER_FOR_TEST);

    ShortBuffer shortBuffer;
    shortBuffer.Init(&multiValue);

    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer.Size());

    for (size_t i = 0; i < 128; ++i)
    {
        pos = 0;
#define TEST_PUSH_BACK_HELPER(type)                                     \
        INDEXLIB_TEST_TRUE(shortBuffer.PushBack<type>(pos++, numeric_limits<type>::max() - i));

        NUMBER_TYPE_HELPER_FOR_TEST(TEST_PUSH_BACK_HELPER);
        shortBuffer.EndPushBack();
    }

    INDEXLIB_TEST_EQUAL((uint8_t)128, shortBuffer.Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)128, shortBuffer.Size());

    for (size_t i = 0; i < 128; ++i)
    {
        pos = 0;
#define ASSERT_GET_VALUE_HELPER(type)                                \
        INDEXLIB_TEST_EQUAL((type)(numeric_limits<type>::max() - i),    \
                            shortBuffer.GetRowTyped<type>(pos++)[i]);

        NUMBER_TYPE_HELPER_FOR_TEST(ASSERT_GET_VALUE_HELPER);
    }
}

void ShortBufferTest::CheckShortBuffer(uint32_t *expectShortBuffer, size_t rowNum, 
                             size_t colNum, ShortBuffer* shortBuffer, uint8_t* capacity)
{
    unique_ptr<ShortBuffer> shortBufferPtr;
    SimpleMultiValue<uint32_t> multiValue(rowNum);
    if (!shortBuffer)
    {
        shortBuffer = new ShortBuffer;
        shortBufferPtr.reset(shortBuffer);
        shortBufferPtr->Init((const MultiValue*)(&multiValue));
    }
    uint8_t curCapacity = capacity == NULL ? 0 : *capacity;
    INDEXLIB_TEST_EQUAL(curCapacity, shortBuffer->Capacity());
    INDEXLIB_TEST_EQUAL((uint8_t)0, shortBuffer->Size());
    INDEXLIB_TEST_EQUAL(rowNum, shortBuffer->GetRowCount());

    for (size_t c = 0; c < colNum; ++c)
    {
        for (size_t r = 0; r < rowNum; ++r)
        {
            INDEXLIB_TEST_TRUE(shortBuffer->PushBack(r, expectShortBuffer[c + r * colNum]));
        }
        shortBuffer->EndPushBack();
        INDEXLIB_TEST_EQUAL((uint8_t)(c + 1), shortBuffer->Size());
        uint8_t curCapacity = capacity == NULL ? shortBuffer->AllocatePlan(c) : *capacity;
        INDEXLIB_TEST_EQUAL(curCapacity, shortBuffer->Capacity());
    }

    CheckEqual(expectShortBuffer, rowNum, colNum, shortBuffer);
}

void ShortBufferTest::CheckEqual(uint32_t* expectShortBuffer, size_t rowNum, size_t colNum,
                            ShortBuffer* shortBuffer)
{
    INDEXLIB_TEST_EQUAL(rowNum, (size_t)shortBuffer->GetRowCount());
    INDEXLIB_TEST_EQUAL(colNum, (size_t)shortBuffer->Size());
    for (size_t r = 0; r < rowNum; ++r)
    {
        uint32_t* row = shortBuffer->GetRowTyped<uint32_t>(r);
        for (size_t c = 0; c < colNum; ++c)
        {
            INDEXLIB_TEST_EQUAL(expectShortBuffer[c + r * colNum], row[c]);
            uint32_t* row = shortBuffer->GetRowTyped<uint32_t>(r);
            INDEXLIB_TEST_EQUAL(expectShortBuffer[c + r * colNum], row[c]);
        }
    }
}


IE_NAMESPACE_END(common);

