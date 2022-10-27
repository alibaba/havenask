#ifndef __INDEXLIB_ATTRIBUTEREFERENCETEST_H
#define __INDEXLIB_ATTRIBUTEREFERENCETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(common);

class AttributeReferenceTest : public INDEXLIB_TESTBASE {
public:
    AttributeReferenceTest();
    ~AttributeReferenceTest();

    DECLARE_CLASS_NAME(AttributeReferenceTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSetAndGetCountedMultiValueAttr();
    void TestSingleFloat();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestSetAndGetCountedMultiValueAttr);
INDEXLIB_UNIT_TEST_CASE(AttributeReferenceTest, TestSingleFloat);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTEREFERENCETEST_H
