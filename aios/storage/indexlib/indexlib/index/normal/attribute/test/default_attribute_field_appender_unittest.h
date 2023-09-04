#ifndef __INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H
#define __INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/default_attribute_field_appender.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

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
}} // namespace indexlib::index

#endif //__INDEXLIB_DEFAULTATTRIBUTEFIELDAPPENDERTEST_H
