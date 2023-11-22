#pragma once

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class OnDiskSegmentSizeCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskSegmentSizeCalculatorTest();
    ~OnDiskSegmentSizeCalculatorTest();

    DECLARE_CLASS_NAME(OnDiskSegmentSizeCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCalculateWithSubSchema();
    void TestCalculateWithPackAttributes();
    void TestCalculateWithPackageFile();
    void TestGetSegmentSize();
    void TestGetSegmentSourceSize();
    void TestGetKvSegmentSize();
    void TestGetKkvSegmentSize();
    void TestCustomizedTable();

private:
    void MakeOneByteFile(const file_system::DirectoryPtr& dir, const std::string& fileName);
    void PrepareSegmentData(const file_system::DirectoryPtr& segmentDir, bool mergedSegment = false);
    void PrepareKVSegmentData(const file_system::DirectoryPtr& segmentDir, size_t columnCount, bool mergedSegment);
    void PrepareKKVSegmentData(const file_system::DirectoryPtr& segmentDir, size_t columnCount, bool mergedSegment);
    index_base::SegmentData CreateSegmentData(segmentid_t segmentId);

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
    merger::SegmentDirectoryPtr mSegDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestCalculateWithSubSchema);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestCalculateWithPackAttributes);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestCalculateWithPackageFile);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestGetSegmentSize);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestGetSegmentSourceSize);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestGetKvSegmentSize);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestGetKkvSegmentSize);
INDEXLIB_UNIT_TEST_CASE(OnDiskSegmentSizeCalculatorTest, TestCustomizedTable);
}} // namespace indexlib::index
