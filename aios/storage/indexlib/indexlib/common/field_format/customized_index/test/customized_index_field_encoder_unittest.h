#pragma once

#include "indexlib/common/field_format/customized_index/customized_index_field_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class CustomizedIndexFieldEncoderTest : public INDEXLIB_TESTBASE
{
public:
    CustomizedIndexFieldEncoderTest();
    ~CustomizedIndexFieldEncoderTest();

    DECLARE_CLASS_NAME(CustomizedIndexFieldEncoderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestEncodeDouble();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexFieldEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexFieldEncoderTest, TestEncodeDouble);
}} // namespace indexlib::common
