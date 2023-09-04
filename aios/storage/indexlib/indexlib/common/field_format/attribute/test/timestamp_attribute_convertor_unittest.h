#ifndef __INDEXLIB_TIMESTAMPATTRIBUTECONVERTORTEST_H
#define __INDEXLIB_TIMESTAMPATTRIBUTECONVERTORTEST_H

#include "indexlib/common/field_format/attribute/timestamp_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class TimestampAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    TimestampAttributeConvertorTest();
    ~TimestampAttributeConvertorTest();

    DECLARE_CLASS_NAME(TimestampAttributeConvertorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSimple();

private:
    void TestEncode(const std::string& value, uint64_t expectedValue);
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimestampAttributeConvertorTest, TestCaseForSimple);
}} // namespace indexlib::common

#endif //__INDEXLIB_TIMESTAMPATTRIBUTECONVERTORTEST_H
