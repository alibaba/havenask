#pragma once

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SegmentLockSizeCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    SegmentLockSizeCalculatorTest();
    ~SegmentLockSizeCalculatorTest();

    DECLARE_CLASS_NAME(SegmentLockSizeCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSimpleProcessWithSubSegment();
    void TestCalculateSize();
    void TestCalculateRangeIndexSizeBlockCache();
    void TestCalculateRangeIndexSizeBlockCache_2();
    void TestCalculateRangeIndexSizeMMapNoLock();
    void TestCalculateRangeIndexSizeMMapNoLock_2();
    void TestCalculateRangeIndexSizeMMapLock();
    void TestCalculateRangeIndexSizeMMapLock_2();
    void TestCalculateSummarySize();
    void TestCalculateSourceSize();
    void TestCalculateSizeWithDiffType();
    void TestCalculateSizeWithMultiShardingIndex();
    void TestCalculateSizeForKVTable();
    void TestCalculateSizeForKKVTable();
    void TestDisable();
    void TestCalculateChangeSize();

private:
    void MakeOneByteFile(const file_system::DirectoryPtr& dir, const std::string& fileName);
    void MakeMultiByteFile(const file_system::DirectoryPtr& dir, const std::string& fileName, size_t count);
    void InnerTestCalculateSizeWithLoadConfig(const std::string& loadConfigStr, size_t expectSize);
    void PrepareData(const file_system::DirectoryPtr& dir);
    void InnerTestCalculateSizeForKVTable(const std::string& loadConfigStr, size_t expectSize);
    void InnerTestCalculateSizeForKKVTable(const std::string& loadConfigStr, size_t expectSize);
    void InnerTestCalculateRangeIndexSize(const std::string& loadConfigStr, size_t expectSize);

private:
    file_system::DirectoryPtr mRootDir;
    file_system::DirectoryPtr mLocalDirectory;
    config::IndexPartitionSchemaPtr mSchema;

    std::string mCacheLoadConfigForRange;
    std::string mMMapLockLoadConfigForRange;
    std::string mMMapNoLockLoadConfigForRange;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestSimpleProcessWithSubSegment);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSummarySize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSourceSize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeBlockCache);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeBlockCache_2);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeMMapNoLock);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeMMapNoLock_2);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeMMapLock);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSizeMMapLock_2);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeWithDiffType);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeWithMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeForKVTable);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeForKKVTable);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestDisable);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateChangeSize);
}} // namespace indexlib::index
