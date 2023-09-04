#ifndef __INDEXLIB_DATEATTRIBUTECONVERTORTEST_H
#define __INDEXLIB_DATEATTRIBUTECONVERTORTEST_H

#include "indexlib/common/field_format/attribute/date_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class DateAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    DateAttributeConvertorTest();
    ~DateAttributeConvertorTest();

    DECLARE_CLASS_NAME(DateAttributeConvertorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSimple();
    void TestEncode(const std::string& value, uint32_t expectedValue);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateAttributeConvertorTest, TestCaseForSimple);
}} // namespace indexlib::common

#endif //__INDEXLIB_DATEATTRIBUTECONVERTORTEST_H
