#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/single_value_data_iterator.h"
#include "indexlib/index/normal/attribute/accessor/var_num_data_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributeDataIteratorTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDataIteratorTest();
    ~AttributeDataIteratorTest();

    DECLARE_CLASS_NAME(AttributeDataIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSingleValue();
    void TestMultiValue();

private:
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionDataPtr mPartitionData;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDataIteratorTest, TestSingleValue);
INDEXLIB_UNIT_TEST_CASE(AttributeDataIteratorTest, TestMultiValue);
}} // namespace indexlib::index
