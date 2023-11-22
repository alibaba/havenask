#pragma once

#include "autil/ConstString.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class AttributeReferenceTest : public INDEXLIB_TESTBASE
{
public:
    AttributeReferenceTest();
    ~AttributeReferenceTest();

    DECLARE_CLASS_NAME(AttributeReferenceTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSetAndGetCountedMultiValueAttr();
    void TestSingleFloat();
    void TestSetAndGetCompactMultiValue();
    void TestReferenceOffset();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestSetAndGetCountedMultiValueAttr);
INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestSingleFloat);
INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestSetAndGetCompactMultiValue);
INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestReferenceOffset);

}} // namespace indexlib::common
