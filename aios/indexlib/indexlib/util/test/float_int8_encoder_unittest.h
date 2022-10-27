#ifndef __INDEXLIB_FLOATINT8ENCODERTEST_H
#define __INDEXLIB_FLOATINT8ENCODERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/float_int8_encoder.h"

IE_NAMESPACE_BEGIN(util);

class FloatInt8EncoderTest : public INDEXLIB_TESTBASE
{
public:
    FloatInt8EncoderTest();
    ~FloatInt8EncoderTest();

    DECLARE_CLASS_NAME(FloatInt8EncoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestEncodeSingleValue();
    void TestCompare();
    void TestEncodeZero();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FloatInt8EncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(FloatInt8EncoderTest, TestEncodeSingleValue);
INDEXLIB_UNIT_TEST_CASE(FloatInt8EncoderTest, TestCompare);
INDEXLIB_UNIT_TEST_CASE(FloatInt8EncoderTest, TestEncodeZero);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_FLOATINT8ENCODERTEST_H
