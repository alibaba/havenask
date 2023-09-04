#ifndef __INDEXLIB_TIMERANGEINDEXINTETEST_H
#define __INDEXLIB_TIMERANGEINDEXINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class TimeRangeIndexInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, int>>
{
public:
    TimeRangeIndexInteTest();
    ~TimeRangeIndexInteTest();

    DECLARE_CLASS_NAME(TimeRangeIndexInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAddEmptyFieldDoc();
    void TestRtDocCoverFullDoc();
    void TestAbnormalInput();
    void TestOpenRangeQuery();
    void TestDecoderDoc_1();
    void TestDecoderDoc_2();
    void TestDecoderDoc_3();
    void TestIterator();
    void TestLookupBuildingSegmentWithAsyncDump();
    void TestAttributeValueIterator();
    void TestMergeToMulitSegment();
    void TestBug23953311();
    void TestSearchTime();
    void TestSearchDate();
    void TestSearchTimestamp();
    void TestSearchNullTime();
    void TestSearchNullDate();
    void TestSearchNullTimestamp();
    void TestSearchTimeByBlockCache();
    void TestSearchTimeByBlockCacheConcurrency();

private:
    void InnerTestDecoderDoc(const std::string& fullDoc, const std::string& fullQuery, const std::string& fullResult,
                             const std::string& incDoc, const std::string& incQuery, const std::string& incResult,
                             const std::string& rtDoc, const std::string& rtQuery, const std::string& rtResult);
    void InnerTestIterator(index::PostingIterator* iter, IndexPartitionReaderPtr reader, const std::string& result);
    std::string GetRangeDocString(uint64_t startTime, uint64_t endTime, uint64_t unit, uint64_t startPk = 1);
    uint64_t GetGlobalTime();

    void CheckTimeRangeIndexCompressInfo(const file_system::IFileSystemPtr& fileSystem);
    void CheckFileCompressInfo(const index_base::PartitionDataPtr& partitionData, const std::string& filePath,
                               const std::string& compressType);

private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(TimeRangeIndexInteTestModeAndBuildMode, TimeRangeIndexInteTest,
                        testing::Combine(testing::Bool(), testing::Values(0, 1, 2)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestAddEmptyFieldDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestRtDocCoverFullDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestAbnormalInput);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestOpenRangeQuery);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestDecoderDoc_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestDecoderDoc_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestDecoderDoc_3);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestIterator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestLookupBuildingSegmentWithAsyncDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestAttributeValueIterator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestMergeToMulitSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestBug23953311);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchTime);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchDate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchNullTime);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchNullDate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchNullTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchTimeByBlockCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(TimeRangeIndexInteTest, TestSearchTimeByBlockCacheConcurrency);
}} // namespace indexlib::partition

#endif //__INDEXLIB_TIMERANGEINDEXINTETEST_H
