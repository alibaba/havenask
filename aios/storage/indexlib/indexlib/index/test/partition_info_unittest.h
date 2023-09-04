#ifndef __INDEXLIB_PARTITIONINFOTEST_H
#define __INDEXLIB_PARTITIONINFOTEST_H
#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PartitionInfoTest : public INDEXLIB_TESTBASE
{
    DECLARE_CLASS_NAME(PartitionInfoTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetGlobalId();
    void TestGetDocId();
    void TestGetSegmentId();
    void TestGetLocalDocId();
    void TestGetPartitionInfoHint();
    void TestGetDiffDocIdRangesForIncChanged();
    void TestGetDiffDocIdRangesForRtBuiltChanged();
    void TestGetDiffDocIdRangesForRtBuildingChanged();
    void TestGetDiffDocIdRangesForAllChanged();
    void TestGetOrderedAndUnorderDocIdRanges();
    void TestInitWithDumpingSegments();
    void TestPartitionInfoWithTemperature();

    void TestKVKKVTable();

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PartitionInfoTest);

INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetGlobalId);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetDocId);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetSegmentId);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetLocalDocId);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetPartitionInfoHint);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetDiffDocIdRangesForIncChanged);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetDiffDocIdRangesForRtBuiltChanged);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetDiffDocIdRangesForRtBuildingChanged);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetDiffDocIdRangesForAllChanged);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestGetOrderedAndUnorderDocIdRanges);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestInitWithDumpingSegments);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestKVKKVTable);
INDEXLIB_UNIT_TEST_CASE(PartitionInfoTest, TestPartitionInfoWithTemperature);

}} // namespace indexlib::index

#endif //__INDEXLIB_PARTITIONINFOTEST_H
