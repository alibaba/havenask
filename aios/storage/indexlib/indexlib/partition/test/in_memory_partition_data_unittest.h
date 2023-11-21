#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class InMemoryPartitionDataTest : public INDEXLIB_TESTBASE
{
public:
    InMemoryPartitionDataTest();
    ~InMemoryPartitionDataTest();

    DECLARE_CLASS_NAME(InMemoryPartitionDataTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestAddSegmentForSub();
    void TestCreateNewSegmentData();
    void TestClone();

private:
    InMemorySegmentCreator mCreator;
    config::IndexPartitionSchemaPtr mSchema;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemoryPartitionDataTest, TestAddSegmentForSub);
INDEXLIB_UNIT_TEST_CASE(InMemoryPartitionDataTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(InMemoryPartitionDataTest, TestCreateNewSegmentData);
}} // namespace indexlib::partition
