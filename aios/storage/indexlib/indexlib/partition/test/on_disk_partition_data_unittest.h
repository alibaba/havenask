#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnDiskPartitionDataTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskPartitionDataTest();
    ~OnDiskPartitionDataTest();

    DECLARE_CLASS_NAME(OnDiskPartitionDataTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetPartitionMeta();
    void TestClone();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnDiskPartitionDataTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OnDiskPartitionDataTest, TestGetPartitionMeta);
INDEXLIB_UNIT_TEST_CASE(OnDiskPartitionDataTest, TestClone);
}} // namespace indexlib::partition
