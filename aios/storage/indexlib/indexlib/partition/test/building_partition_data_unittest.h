#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class BuildingPartitionDataTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    BuildingPartitionDataTest();
    ~BuildingPartitionDataTest();

    DECLARE_CLASS_NAME(BuildingPartitionDataTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestNormalOpen();
    void TestInvalidOpen();
    void TestDumpAndCommitVersionSuccess();
    void TestRemoveSegmentAndCommitVersionSuccess();
    void TestNoCommitVersion();
    void TestCreateJoinSegment();
    void TestCommitJoinVersion();
    void TestCreateNewSegment();
    void TestUpdatePartitionInfoWithSub();
    void TestGetDeletionMapReaderWithSub();
    void TestInitInMemorySegmentCreator();
    void TestClone();
    void TestSnapshot();
    void TestCreateSegmentIterator();

private:
    void InnerTestNormalOpen(bool hasSub, bool isOnline);

    void InnerTestInitInMemorySegmentCreator(bool hasSub, bool isOnline, bool expectUseRtOption);

    config::IndexPartitionSchemaPtr CreateSchema(bool hasSub);
    index_base::SegmentDirectoryPtr CreateSegmentDirectory(bool hasSub, bool isOnline);

    BuildingPartitionDataPtr CreatePartitionData();
    void Dump(BuildingPartitionDataPtr partitionData, timestamp_t ts = INVALID_TIMESTAMP);
    index_base::Version GetNewVersion(std::string dirName = RT_INDEX_PARTITION_DIR_NAME);
    index_base::Version MakeVersion(versionid_t vid, std::string segmentIds, timestamp_t ts);

    void CheckPartitionInfo(const index_base::PartitionDataPtr& partData, docid_t docCount, docid_t subDocCount);

    void CheckDeletionMapReader(const index::DeletionMapReaderPtr& deletionMapReader, const std::string& docIds,
                                bool isDeleted);

    void OpenWriter(OnlinePartitionWriter& writer, const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options, const index_base::PartitionDataPtr& partitionData);

private:
    config::IndexPartitionSchemaPtr mSchema;
    util::PartitionMemoryQuotaControllerPtr mMemController;
    bool mIsOnline;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(BuildingPartitionDataTestMode, BuildingPartitionDataTest,
                                     testing::Values(false, true));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingPartitionDataTestMode, TestDumpAndCommitVersionSuccess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingPartitionDataTestMode, TestRemoveSegmentAndCommitVersionSuccess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingPartitionDataTestMode, TestNoCommitVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingPartitionDataTestMode, TestCreateNewSegment);

INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestCreateJoinSegment);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestCommitJoinVersion);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestInvalidOpen);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestNormalOpen);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestUpdatePartitionInfoWithSub);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestGetDeletionMapReaderWithSub);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestInitInMemorySegmentCreator);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestSnapshot);
INDEXLIB_UNIT_TEST_CASE(BuildingPartitionDataTest, TestCreateSegmentIterator);
}} // namespace indexlib::partition
