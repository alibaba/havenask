#ifndef __INDEXLIB_TIMERANGEINDEXINTETEST_H
#define __INDEXLIB_TIMERANGEINDEXINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

IE_NAMESPACE_BEGIN(partition);

class TimeRangeIndexInteTest : public INDEXLIB_TESTBASE
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
    void TestDecoderDoc();
    void TestIterator();
    void TestLookupBuildingSegmentWithAsyncDump();
    void TestAttributeValueIterator();
    void TestMergeToMulitSegment();
    void TestBug23953311();
private:
    void InnerTestDecoderDoc(
            const std::string& fullDoc, const std::string& fullQuery,
            const std::string& fullResult, const std::string& incDoc,
            const std::string& incQuery, const std::string& incResult,
            const std::string& rtDoc, const std::string& rtQuery,
            const std::string& rtResult);
    void InnerTestIterator(index::PostingIterator* iter,
                           IndexPartitionReaderPtr reader,
                           const std::string& result);
    std::string GetRangeDocString(uint64_t startTime, uint64_t endTime,
                                  uint64_t unit, uint64_t startPk = 1);
    std::string GetQueryResultString(uint64_t startPk, uint64_t endPk);
    uint64_t GetGlobalTime();

private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestAddEmptyFieldDoc);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestRtDocCoverFullDoc);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestAbnormalInput);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestOpenRangeQuery);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestDecoderDoc);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestIterator);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestLookupBuildingSegmentWithAsyncDump);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestAttributeValueIterator);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestMergeToMulitSegment);
INDEXLIB_UNIT_TEST_CASE(TimeRangeIndexInteTest, TestBug23953311);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_TIMERANGEINDEXINTETEST_H
