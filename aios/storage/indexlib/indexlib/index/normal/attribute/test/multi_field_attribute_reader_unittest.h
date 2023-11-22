#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

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
}} // namespace indexlib::index
