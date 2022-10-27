#ifndef __INDEXLIB_INDEXBUILDERINTETEST_H
#define __INDEXLIB_INDEXBUILDERINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class IndexBuilderInteTest : public INDEXLIB_TESTBASE
{
public:
    IndexBuilderInteTest();
    ~IndexBuilderInteTest();

    DECLARE_CLASS_NAME(IndexBuilderInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMerge();
    void TestOptimizeMergeWithNoSegmentsToMerge();
    void TestAddToUpdate();
    void TestGetLastLocatorForOffline();
    void TestGetIncVersionTimestamp();
    void TestMergeCleanVersion();
    void TestCleanVersionsWithReservedVersionList();
    void TestBuildAllSummaryInAttribute();
    void TestUpdateDeletedDoc();
    void TestInit();
    void TestLoadIndexFormatVersion();
    void TestGetLastLocator();
    void TestEndVersionWithEmptyData();

    void TestMergeWithAdaptiveBitmap();
    void TestBuildAndMergeEnablePackageFile();
    void TestUniqSectionBuildAndMerge();
    void TestOnlineDumpOperation();
    void TestBuildPackAttribute();
    void TestMergeAddDocWithPackAttribute();
    void TestMergeUpdateDocWithPackAttribute();
    void TestOfflineBuildWithUpdatePackAttribute();
    void TestOnlineBuildWithUpdatePackAttribute();
    void TestAddOverLengthPackAttribute();
    void TestUpdateOver64KPackAttribute();
    void TestAutoAdd2Update();
    void TestCheckSortBuild();
    void TestUnifiedVarNumAttribute();
    void TestUnifiedMultiString();
    void TestBuildMultiShardingIndex();
    void TestReadMultiShardingIndex();
    void TestMergeMultiShardingIndex();
    void TestMaxTTLWithDelayDedup();
    void TestShardingIndexBuildTruncateIndex();
    void TestShardingIndexBuildAdaptiveBitmapIndex();
    void TestReadDictInlineIndex();
    void TestAdaptiveBitampFailWithMergeDictInlineTerm();
    void TestBuildAndMergeWithCompressPatch();
    void TestEnablePatchCompress();
    void TestIncTruncateByMetaWithDictInlineTerm();
    void TestTruncateIndex();
    void TestOfflineRecover();
    void TestDictInlineIndexWithTermPayload();
    void TestUpdateDocFailWhenFlushRtIndex();
    void TestAutoAdd2UpdateWhenFlushRtIndex();
    void TestOnlineIntervalDump();
    void TestBuildRecoverWithNonDefaultSchemaVersion();
    void TestMergeMultiSegment();
    void TestMergeMutiSegmentTypeString();
    void TestMergeMutiSegmentTypeRange();
    void TestMergeMutiSegmentTypePack();
    void TestFixLengthMultiValueAttribute();
    void TestFixLengthEncodeFloatAttribute();
    void TestFixLengthStringAttribute();
    void TestSolidPathGetDirectory();
    void TestMergeMutiSegmentTypeRangeWithEmptyOutput();
    void TestMergeMutiSegmentTypeRangeWithEmptyOutputWithSubDoc();
    void TestForTimeSeriesStrategy();

private:
    void CheckFileContent(const std::string& filePath,
                          const std::string& fileContent);

    void CheckFileStat(file_system::IndexlibFileSystemPtr fileSystem, std::string filePath,
        file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType);

    void InnerTestMergeWithAdaptiveBitmap(bool optimize, bool enableArchiveFile = false);

    void SetAdaptiveHighFrequenceDictionary(const std::string& indexName,
            const std::string& adaptiveRuleStr,
            HighFrequencyTermPostingType type,
            const config::IndexPartitionSchemaPtr& schema);

    void CheckPackgeFile(const std::string& segmentPath);
    void CheckUpdateInfo(const file_system::DirectoryPtr& partDir,
                         const std::string& updateInfoRelPath,
                         const std::string& updateInfoStr);

    void InnerTestBuildAndMergeEnablePackageFile(bool optimizeMerge);
    void InnerTestOfflineBuildWithUpdatePackAttribute(
        bool uniqEncode, bool equalCompress);
    void InnerTestOnlineBuildWithUpdatePackAttribute(
        bool uniqEncode, bool equalCompress);
    void InnerTestMergeUpdateDocWithPackAttribute(bool sortBuild, bool uniq);
    void InnerTestUpdateOver64KPackAttribute(bool uniqEncode);

    void CheckIndexDir(const file_system::DirectoryPtr& segDirectory,
                       const std::string& indexName,
                       size_t shardingCount, bool hasSection);

    void CheckBitmapOnlyIndexData(
            const file_system::DirectoryPtr& indexDirectory,
            const config::IndexConfigPtr& indexConfig);

    void CheckTermPosting(const std::string& word, const std::string& indexName,
                          const std::string& truncateName,
                          const index::IndexReaderPtr& indexReader,
                          const std::vector<docid_t>& expectDocIds,
                          const std::vector<int32_t>& expectFieldMaps = std::vector<int32_t>(),
                          int32_t expectTermpayload = -1);
    void MakeConfig(int outputSegmentCount, config::IndexPartitionOptions& options,
        int buildMaxDocCount, bool sort = false, std::string sortField = "",
        SortPattern sortPattern = sp_asc);

    void InnerTestFixLengthMultiValueAttribute(
            bool isPackAttr, const std::string& fieldType, const std::string& encodeStr);

    template <typename T>
    void assertAttribute(const index_base::SegmentMetrics& metrics, const std::string& attrName,
        T attrMin, T attrMax, size_t checkedDocCount);
    void assertLifecycle(const index_base::SegmentMetrics& metrics, const std::string& expected);

    index_base::SegmentMetrics LoadSegmentMetrics(
            test::PartitionStateMachine& psm, segmentid_t segId);
private:
    config::IndexPartitionSchemaPtr mSchema;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestAddToUpdate);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestGetLastLocatorForOffline);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestGetIncVersionTimestamp);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeCleanVersion);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestCleanVersionsWithReservedVersionList);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildAllSummaryInAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUpdateDeletedDoc);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOptimizeMergeWithNoSegmentsToMerge);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestLoadIndexFormatVersion);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestGetLastLocator);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestEndVersionWithEmptyData);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeWithAdaptiveBitmap);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildAndMergeEnablePackageFile);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUniqSectionBuildAndMerge);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOnlineDumpOperation);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeAddDocWithPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeUpdateDocWithPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOfflineBuildWithUpdatePackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOnlineBuildWithUpdatePackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestAddOverLengthPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUpdateOver64KPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestAutoAdd2Update);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestCheckSortBuild);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUnifiedVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUnifiedMultiString);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestReadMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMaxTTLWithDelayDedup);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestShardingIndexBuildTruncateIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestShardingIndexBuildAdaptiveBitmapIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestReadDictInlineIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestAdaptiveBitampFailWithMergeDictInlineTerm);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildAndMergeWithCompressPatch);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestEnablePatchCompress);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestIncTruncateByMetaWithDictInlineTerm);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestTruncateIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOfflineRecover);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestDictInlineIndexWithTermPayload);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestUpdateDocFailWhenFlushRtIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestAutoAdd2UpdateWhenFlushRtIndex);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestOnlineIntervalDump);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestBuildRecoverWithNonDefaultSchemaVersion);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMultiSegment);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMutiSegmentTypeString);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMutiSegmentTypeRange);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMutiSegmentTypePack);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestFixLengthMultiValueAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestFixLengthEncodeFloatAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestFixLengthStringAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestSolidPathGetDirectory);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMutiSegmentTypeRangeWithEmptyOutput);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestMergeMutiSegmentTypeRangeWithEmptyOutputWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderInteTest, TestForTimeSeriesStrategy);



IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXBUILDERINTETEST_H
