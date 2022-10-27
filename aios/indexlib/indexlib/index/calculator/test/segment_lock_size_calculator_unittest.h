#ifndef __INDEXLIB_SEGMENTLOCKSIZECALCULATORTEST_H
#define __INDEXLIB_SEGMENTLOCKSIZECALCULATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

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
    void TestCalculateSize();
    void TestCalculateRangeIndexSize();
    void TestCalculateSummarySize();
    void TestCalculateSizeWithDiffType();
    void TestCalculateSizeWithMultiShardingIndex();
    void TestDisable();

private:
    void MakeOneByteFile(const file_system::DirectoryPtr& dir,
                         const std::string& fileName);
    void MakeMultiByteFile(const file_system::DirectoryPtr& dir,
                           const std::string& fileName, size_t count);
    void InnerTestCalculateSizeWithLoadConfig(
            const std::string& loadConfigStr, size_t expectSize);
    void PrepareData(const file_system::DirectoryPtr& dir);

private:
    file_system::DirectoryPtr mRootDir;
    file_system::DirectoryPtr mLocalDirectory;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSummarySize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateRangeIndexSize);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeWithDiffType);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestCalculateSizeWithMultiShardingIndex);
INDEXLIB_UNIT_TEST_CASE(SegmentLockSizeCalculatorTest, TestDisable);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENTLOCKSIZECALCULATORTEST_H
