#ifndef __INDEXLIB_FIELDCONFIGTEST_H
#define __INDEXLIB_FIELDCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/impl/field_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class FieldConfigTest : public INDEXLIB_TESTBASE {
public:
    FieldConfigTest();
    ~FieldConfigTest();
public:
    void SetUp();
    void TearDown();
    void TestJsonize();
    void TestJsonizeU32OffsetThreshold();
    void TestAssertEqual();
    void TestIsAttributeUpdatable();
    void TestCheckUniqEncode();
    void TestCheckBlockFpEncode();
    void TestCheckFp16Encode();
    void TestCheckFloatInt8Encode();
    void TestCheckDefaultAttributeValue();
    void TestCheckFixedLengthMultiValue();    
    void TestCheck();
    void TestSupportSort();
    void TestUserDefinedParam();
    void TestRewriteFieldType();
    void TestCheckFieldType();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestJsonizeU32OffsetThreshold);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestAssertEqual);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestIsAttributeUpdatable);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckUniqEncode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckBlockFpEncode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFp16Encode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFloatInt8Encode);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckDefaultAttributeValue);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFixedLengthMultiValue);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestSupportSort);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestUserDefinedParam);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestRewriteFieldType);
INDEXLIB_UNIT_TEST_CASE(FieldConfigTest, TestCheckFieldType);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELDCONFIGTEST_H
