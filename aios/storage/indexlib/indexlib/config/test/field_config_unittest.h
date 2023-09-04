#ifndef __INDEXLIB_FIELDCONFIGTEST_H
#define __INDEXLIB_FIELDCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class FieldConfigTest : public INDEXLIB_TESTBASE
{
public:
    FieldConfigTest();
    ~FieldConfigTest();

public:
    void TestJsonize();
    void TestJsonizeU32OffsetThreshold();
    void TestAssertEqual();
    void TestCheckUniqEncode();
    void TestCheckBlockFpEncode();
    void TestCheckFp16Encode();
    void TestCheckFloatInt8Encode();
    void TestCheckFixedLengthMultiValue();
    void TestCheckSupportNull();
    void TestCheck();
    void TestSupportSort();
    void TestUserDefinedParam();
    void TestUserDefinedParamV2();
    void TestRewriteFieldType();
    void TestCheckFieldType();
    void TestSeparator();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestJsonizeU32OffsetThreshold);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestAssertEqual);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckUniqEncode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckBlockFpEncode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFp16Encode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFloatInt8Encode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFixedLengthMultiValue);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestSupportSort);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestUserDefinedParam);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestUserDefinedParamV2);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestRewriteFieldType);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFieldType);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckSupportNull);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestSeparator);
}} // namespace indexlib::config

#endif //__INDEXLIB_FIELDCONFIGTEST_H
