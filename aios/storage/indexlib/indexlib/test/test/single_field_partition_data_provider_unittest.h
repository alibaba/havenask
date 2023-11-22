#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace test {

class SingleFieldPartitionDataProviderTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    SingleFieldPartitionDataProviderTest();
    ~SingleFieldPartitionDataProviderTest();

    DECLARE_CLASS_NAME(SingleFieldPartitionDataProviderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreatePkIndex();
    void TestCreateIndex();

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, SingleFieldPartitionDataProviderTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SingleFieldPartitionDataProviderTest, TestCreatePkIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SingleFieldPartitionDataProviderTest, TestCreateIndex);
}} // namespace indexlib::test
