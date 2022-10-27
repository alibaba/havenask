#ifndef __INDEXLIB_FP16ENCODERTEST_H
#define __INDEXLIB_FP16ENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/fp16_encoder.h"

IE_NAMESPACE_BEGIN(util);

class Fp16EncoderTest : public INDEXLIB_TESTBASE
{
public:
    Fp16EncoderTest();
    ~Fp16EncoderTest();

    DECLARE_CLASS_NAME(Fp16EncoderTest);
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

INDEXLIB_UNIT_TEST_CASE(Fp16EncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(Fp16EncoderTest, TestEncodeSingleValue);
INDEXLIB_UNIT_TEST_CASE(Fp16EncoderTest, TestCompare);
INDEXLIB_UNIT_TEST_CASE(Fp16EncoderTest, TestEncodeZero);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_FP16ENCODERTEST_H
