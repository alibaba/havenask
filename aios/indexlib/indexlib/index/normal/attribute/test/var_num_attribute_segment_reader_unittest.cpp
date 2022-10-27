#include "indexlib/index/normal/attribute/test/var_num_attribute_segment_reader_unittest.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteRead);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithSmallOffsetThreshold);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithUniqEncode);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithUniqEncodeAndSmallThreshold);

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithCompress);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithSmallOffsetThresholdWithCompress);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithUniqEncodeWithCompress);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeSegmentReaderTest, TestCaseForWriteReadWithUniqEncodeAndSmallThresholdWithCompress);

void VarNumAttributeSegmentReaderTest::CaseSetUp()
{
}

void VarNumAttributeSegmentReaderTest::CaseTearDown()
{
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteRead()
{
    TestWriteAndRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestWriteAndRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestWriteAndRead<float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestWriteAndRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestWriteAndRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithSmallOffsetThreshold()
{
    uint64_t smallOffsetThreshold = 16;
    TestWriteAndRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t));
    TestWriteAndRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t));
    TestWriteAndRead<float>(smallOffsetThreshold, sizeof(uint64_t));
    TestWriteAndRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t));
    TestWriteAndRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t));
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithUniqEncode()
{
    TestWriteAndRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);
    TestWriteAndRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);
    TestWriteAndRead<float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);
    TestWriteAndRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);
    TestWriteAndRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);
}
    
void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithUniqEncodeAndSmallThreshold()
{
    uint64_t smallOffsetThreshold = 16;
    TestWriteAndRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), true);
    TestWriteAndRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), true);
    TestWriteAndRead<float>(smallOffsetThreshold, sizeof(uint64_t), true);
    TestWriteAndRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), true);
    TestWriteAndRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), true);
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithCompress()
{
    TestWriteAndRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    TestWriteAndRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    TestWriteAndRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    TestWriteAndRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithSmallOffsetThresholdWithCompress()
{
    uint64_t smallOffsetThreshold = 16;
    TestWriteAndRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    TestWriteAndRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    TestWriteAndRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    TestWriteAndRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
}

void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithUniqEncodeWithCompress()
{
    TestWriteAndRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    TestWriteAndRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    TestWriteAndRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    TestWriteAndRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
}
    
void VarNumAttributeSegmentReaderTest::TestCaseForWriteReadWithUniqEncodeAndSmallThresholdWithCompress()
{
    uint64_t smallOffsetThreshold = 16;
    TestWriteAndRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    TestWriteAndRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    TestWriteAndRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    TestWriteAndRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
}

void VarNumAttributeSegmentReaderTest::CheckOffsetFileLength(string &offsetFilePath, int64_t targetLength)
{
    FileMeta fileMeta;
    FileSystemWrapper::GetFileMeta(offsetFilePath, fileMeta);
    INDEXLIB_TEST_EQUAL(targetLength, fileMeta.fileLength);
}

IE_NAMESPACE_END(index);
