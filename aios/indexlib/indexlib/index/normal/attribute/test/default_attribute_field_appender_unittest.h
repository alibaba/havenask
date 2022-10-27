#ifndef __INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H
#define __INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/format/default_attribute_field_appender.h"

IE_NAMESPACE_BEGIN(index);

class DefaultAttributeFieldAppenderTest : public INDEXLIB_TESTBASE
{
public:
    DefaultAttributeFieldAppenderTest();
    ~DefaultAttributeFieldAppenderTest();

    DECLARE_CLASS_NAME(DefaultAttributeFieldAppenderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DefaultAttributeFieldAppenderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H
