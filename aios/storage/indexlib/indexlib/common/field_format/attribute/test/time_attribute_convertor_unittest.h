#pragma once

#include "indexlib/common/field_format/attribute/time_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class TimeAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    TimeAttributeConvertorTest();
    ~TimeAttributeConvertorTest();

    DECLARE_CLASS_NAME(TimeAttributeConvertorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSimple();
    void TestEncode(const std::string& value, uint32_t expectedValue);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeAttributeConvertorTest, TestCaseForSimple);
}} // namespace indexlib::common
