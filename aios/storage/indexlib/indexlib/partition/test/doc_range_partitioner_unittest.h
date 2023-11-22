#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/partition/doc_range_partitioner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class DocRangePartitionerTest : public INDEXLIB_TESTBASE
{
public:
    DocRangePartitionerTest();
    ~DocRangePartitionerTest();

    DECLARE_CLASS_NAME(DocRangePartitionerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBatch();

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocRangePartitionerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocRangePartitionerTest, TestBatch);
}} // namespace indexlib::partition
