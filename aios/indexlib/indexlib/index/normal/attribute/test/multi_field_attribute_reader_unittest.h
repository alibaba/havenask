#ifndef __INDEXLIB_MULTIFIELDATTRIBUTEREADERTEST_H
#define __INDEXLIB_MULTIFIELDATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class MultiFieldAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    MultiFieldAttributeReaderTest();
    ~MultiFieldAttributeReaderTest();

    DECLARE_CLASS_NAME(MultiFieldAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLazyLoad();

private:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiFieldAttributeReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MultiFieldAttributeReaderTest, TestLazyLoad);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTIFIELDATTRIBUTEREADERTEST_H
