#ifndef __INDEXLIB_DUMPSEGMENTCONTAINERTEST_H
#define __INDEXLIB_DUMPSEGMENTCONTAINERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {
DECLARE_REFERENCE_CLASS(partition, PartitionData);

class DumpSegmentContainerTest : public INDEXLIB_TESTBASE
{
public:
    DumpSegmentContainerTest();
    ~DumpSegmentContainerTest();

    DECLARE_CLASS_NAME(DumpSegmentContainerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetMaxDumpingSegmentExpandMemUse();
    void TestGetOneValidSegmentItemToDump();

private:
    NormalSegmentDumpItemPtr CreateSegmentDumpItem(config::IndexPartitionOptions& options,
                                                   config::IndexPartitionSchemaPtr& schema,
                                                   index_base::PartitionDataPtr& partitionData);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    index_base::PartitionDataPtr mPartitionData;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DumpSegmentContainerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentContainerTest, TestGetMaxDumpingSegmentExpandMemUse);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentContainerTest, TestGetOneValidSegmentItemToDump);
}} // namespace indexlib::partition

#endif //__INDEXLIB_DUMPSEGMENTCONTAINERTEST_H
