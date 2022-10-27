#ifndef __INDEXLIB_PARTITIONINFOTEST_H
#define __INDEXLIB_PARTITIONINFOTEST_H
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/test/single_field_partition_data_provider.h"

IE_NAMESPACE_BEGIN(index);

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

    void TestKVKKVTable();

private:
    // (versionId, "incSegId[:docCount][,incSegId[:docCount]]*",
    // "rtSegId[:docCount][,rtSegId[:docCount]*")
    // if not specify "docCount", use default value 1
    PartitionInfo CreatePartitionInfo(versionid_t versionId, const std::string& incSegmentIds,
        const std::string& rtSegmentIds = "",
        index_base::PartitionMetaPtr partMeta = index_base::PartitionMetaPtr(),
        std::vector<index_base::InMemorySegmentPtr> inMemSegs
        = std::vector<index_base::InMemorySegmentPtr>(),
        bool needDelMapReader = true);

    PartitionInfoPtr CreatePartitionInfo(const std::string& incDocCounts,
            const std::string& rtDocCounts = "");

    std::string ExtractDocString(const std::string& docCounts);

    void ExtractDocCount(const std::string& segmentInfos, 
                         std::vector<docid_t> &docCount);

private:
    std::string mRootDir;
    PartitionInfoCreatorPtr mCreator;
    test::SingleFieldPartitionDataProviderPtr mProvider;

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
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PARTITIONINFOTEST_H
