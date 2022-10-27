#ifndef __INDEXLIB_SHORTBUFFERTEST_H
#define __INDEXLIB_SHORTBUFFERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/short_buffer.h"
#include "indexlib/common/multi_value.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
class SimpleMultiValue : public MultiValue
{
public:
    SimpleMultiValue(size_t count)
    {
        for (size_t i = 0; i < count; ++i)
        {
            AtomicValue* value = new AtomicValueTyped<T>();
            value->SetLocation(i);
            value->SetOffset(i * sizeof(T));
            AddAtomicValue(value);
        }
    }
};

class ShortBufferTest : public INDEXLIB_TESTBASE {
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
    void CheckShortBuffer(uint32_t* expectShortBuffer, size_t rowNum, 
                     size_t colNum, ShortBuffer* shortBuffer = NULL,
                     uint8_t* capacity = NULL);

    void CheckEqual(uint32_t* expectShortBuffer, size_t rowNum, 
                    size_t colNum, ShortBuffer* shortBuffer);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestTwoRow);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestMaxCapacity);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestInvalidPushBack);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestSizeChange);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestClear);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestDisorderPushBack);
INDEXLIB_UNIT_TEST_CASE(ShortBufferTest, TestMultiRowWithDifferentType);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHORTBUFFERTEST_H
