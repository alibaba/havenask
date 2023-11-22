#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class IndexBuilderInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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
    void TestGetLastLocatorForRealtime();
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
    void TestTruncateIndexWithTimeAttribute();
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
    void TestFixLengthEncodeFloatAttribute_1();
    void TestFixLengthEncodeFloatAttribute_2();
    void TestFixLengthStringAttribute();
    void TestSolidPathGetDirectory();
    void TestMergeMutiSegmentTypeRangeWithEmptyOutput();
    void TestMergeMutiSegmentTypeRangeWithEmptyOutputWithSubDoc();
    void TestForTimeSeriesStrategy();
    void TestBuildAndLoadPK();
    void TestBuildAndRedoPkParallelPK();
    void TestBatchBuildMetrics();

    void TestSimpleProcessWithNullValue();
    void TestNullValueWithBitmap();
    void TestNullValueWithTruncateIndex();
    void TestEnablePatchCompressWhitNull();

    void TestSortMergeWithTimeAttribute();
    void TestSortMergeWithHashId();
    void TestPKExceedMemLimit();

    void TestFileCompressForTruncateIndex();
    void TestFileCompressForShardingIndex();

    void TestBuilderBranchHinter();
    void TestFullBranchBuilder();
    void TestBranchPathMultiRound();
    void TestBranchPathRetrogressToRoot();
    void TestIncNoData();
    void TestBranchWithSwitchFence();

private:
    void CheckFileCompressInfo(const file_system::DirectoryPtr& segDir, const std::string& filePath,
                               const std::string& expectCompressType);

    void InnerTestSortMergeWithTimeAttribute(const std::string& sortField);
    void InnerTestSortMergeWithHashId(bool userDefined);
    void InnerTestNullValueWithTruncateIndex(const std::string& schemaFile);

    void InnerTestSimpleProcessWithNullValue(const std::string& fieldType, const std::string& indexType, bool sharding);

    void InnerTestNullValueForBitmapIndex(bool adaptive = false);

    void CheckFileContent(const std::string& filePath, const std::string& fileContent);

    void CheckFileStat(file_system::IFileSystemPtr fileSystem, std::string filePath,
                       file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType);

    void InnerTestMergeWithAdaptiveBitmap(bool optimize, bool enableArchiveFile = false);

    void SetAdaptiveHighFrequenceDictionary(const std::string& indexName, const std::string& adaptiveRuleStr,
                                            indexlib::index::HighFrequencyTermPostingType type,
                                            const config::IndexPartitionSchemaPtr& schema);

    void CheckPackgeFile(const std::string& segmentPath);
    void CheckUpdateInfo(const file_system::DirectoryPtr& partDir, const std::string& updateInfoRelPath,
                         const std::string& updateInfoStr);

    void InnerTestBuildAndMergeEnablePackageFile(bool optimizeMerge);
    void InnerTestOfflineBuildWithUpdatePackAttribute(bool uniqEncode, bool equalCompress);
    void InnerTestOnlineBuildWithUpdatePackAttribute(bool uniqEncode, bool equalCompress);
    void InnerTestMergeUpdateDocWithPackAttribute(bool sortBuild, bool uniq);
    void InnerTestUpdateOver64KPackAttribute(bool uniqEncode);
    void InnerTestBuildAndLoadPK(std::string loadConfigListStr, std::string schemaFileName);
    void CheckIndexDir(const file_system::DirectoryPtr& segDirectory, const std::string& indexName,
                       size_t shardingCount, bool hasSection);

    void CheckBitmapOnlyIndexData(const file_system::DirectoryPtr& indexDirectory,
                                  const config::IndexConfigPtr& indexConfig);

    void CheckTermPosting(const index::Term& term, const index::InvertedIndexReaderPtr& indexReader,
                          const std::vector<docid_t>& expectDocIds,
                          const std::vector<int32_t>& expectFieldMaps = std::vector<int32_t>(),
                          int32_t expectTermpayload = -1);

    void CheckTermPosting(const std::string& word, const std::string& indexName, const std::string& truncateName,
                          const index::InvertedIndexReaderPtr& indexReader, const std::vector<docid_t>& expectDocIds,
                          const std::vector<int32_t>& expectFieldMaps = std::vector<int32_t>(),
                          int32_t expectTermpayload = -1);
    void MakeConfig(int outputSegmentCount, config::IndexPartitionOptions& options, int buildMaxDocCount,
                    bool sort = false, std::string sortField = "",
                    indexlibv2::config::SortPattern sortPattern = indexlibv2::config::sp_asc);

    void InnerTestFixLengthMultiValueAttribute(bool isPackAttr, const std::string& fieldType,
                                               const std::string& encodeStr);

    template <typename T>
    void assertAttribute(const framework::SegmentMetrics& metrics, const std::string& attrName, T attrMin, T attrMax,
                         size_t checkedDocCount);
    void assertLifecycle(const framework::SegmentMetrics& metrics, const std::string& expected);

    framework::SegmentMetrics LoadSegmentMetrics(test::PartitionStateMachine& psm, segmentid_t segId);

private:
    config::IndexPartitionSchemaPtr mSchema;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestAddToUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestGetLastLocatorForOffline);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestGetLastLocatorForRealtime);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestGetIncVersionTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeCleanVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestCleanVersionsWithReservedVersionList);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildAllSummaryInAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUpdateDeletedDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOptimizeMergeWithNoSegmentsToMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestInit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestLoadIndexFormatVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestGetLastLocator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestEndVersionWithEmptyData);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeWithAdaptiveBitmap);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildAndMergeEnablePackageFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUniqSectionBuildAndMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOnlineDumpOperation);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeAddDocWithPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeUpdateDocWithPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOfflineBuildWithUpdatePackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOnlineBuildWithUpdatePackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestAddOverLengthPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUpdateOver64KPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestAutoAdd2Update);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestCheckSortBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUnifiedVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUnifiedMultiString);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestReadMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMaxTTLWithDelayDedup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestShardingIndexBuildTruncateIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestShardingIndexBuildAdaptiveBitmapIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestReadDictInlineIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestAdaptiveBitampFailWithMergeDictInlineTerm);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildAndMergeWithCompressPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestEnablePatchCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestIncTruncateByMetaWithDictInlineTerm);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestTruncateIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestTruncateIndexWithTimeAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOfflineRecover);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestDictInlineIndexWithTermPayload);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestUpdateDocFailWhenFlushRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestAutoAdd2UpdateWhenFlushRtIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestOnlineIntervalDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildRecoverWithNonDefaultSchemaVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMultiSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMutiSegmentTypeString);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMutiSegmentTypeRange);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMutiSegmentTypePack);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFixLengthMultiValueAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFixLengthEncodeFloatAttribute_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFixLengthEncodeFloatAttribute_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFixLengthStringAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestSolidPathGetDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMutiSegmentTypeRangeWithEmptyOutput);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestMergeMutiSegmentTypeRangeWithEmptyOutputWithSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestForTimeSeriesStrategy);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildAndLoadPK);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuildAndRedoPkParallelPK);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBatchBuildMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestSimpleProcessWithNullValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestNullValueWithBitmap);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestNullValueWithTruncateIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestEnablePatchCompressWhitNull);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestSortMergeWithTimeAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestSortMergeWithHashId);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestPKExceedMemLimit);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFileCompressForTruncateIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFileCompressForShardingIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBuilderBranchHinter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestFullBranchBuilder);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBranchPathMultiRound);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBranchPathRetrogressToRoot);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestIncNoData);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(IndexBuilderInteTest, TestBranchWithSwitchFence);
INSTANTIATE_TEST_CASE_P(BuildMode, IndexBuilderInteTest, testing::Values(0, 1, 2));

}} // namespace indexlib::partition
