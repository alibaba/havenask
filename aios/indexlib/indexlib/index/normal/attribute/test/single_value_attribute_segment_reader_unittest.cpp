#include "indexlib/index/normal/attribute/test/single_value_attribute_segment_reader_unittest.h"
#include <time.h>

using namespace std::tr1;
using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

void SingleValueAttributeSegmentReaderTest::CaseSetUp()
{
    mRoot = GET_TEST_DATA_PATH();
    mSeg0Directory = GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0");
    srand(time(NULL));
}

void SingleValueAttributeSegmentReaderTest::CaseTearDown()
{
}

void SingleValueAttributeSegmentReaderTest::TestIsInMemory()
{
    AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<int32_t>();
    SingleValueAttributeSegmentReader<int32_t> int32Reader(attrConfig);
    INDEXLIB_TEST_EQUAL(false, int32Reader.IsInMemory());
}

void SingleValueAttributeSegmentReaderTest::TestGetDataLength()
{
    AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<int32_t>();
    SingleValueAttributeSegmentReader<int32_t> int32Reader(attrConfig);
    INDEXLIB_TEST_EQUAL(sizeof(int32_t), int32Reader.GetDataLength(0));
    INDEXLIB_TEST_EQUAL(sizeof(int32_t), int32Reader.GetDataLength(100));

    attrConfig = AttributeTestUtil::CreateAttrConfig<int8_t>();
    SingleValueAttributeSegmentReader<int8_t> int8Reader(attrConfig);
    INDEXLIB_TEST_EQUAL(sizeof(int8_t), int8Reader.GetDataLength(0));
    INDEXLIB_TEST_EQUAL(sizeof(int8_t), int8Reader.GetDataLength(100));
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt32NoPatch()
{
    TestOpenForNoPatch<uint32_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt32NoPatch()
{
    TestOpenForNoPatch<int32_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt32Patch()
{
    TestOpenForPatch<uint32_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt32Patch()
{
    TestOpenForPatch<int32_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt64NoPatch()
{
    TestOpenForNoPatch<uint64_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt64NoPatch()
{
    TestOpenForNoPatch<int64_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt64Patch()
{
    TestOpenForPatch<uint64_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt64Patch()
{
    TestOpenForPatch<int64_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt8NoPatch()
{
    TestOpenForNoPatch<uint8_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt8NoPatch()
{
    TestOpenForNoPatch<int8_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt8Patch()
{
    TestOpenForPatch<uint8_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt8Patch()
{
    TestOpenForPatch<int8_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt16NoPatch()
{
    TestOpenForNoPatch<uint16_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt16NoPatch()
{
    TestOpenForNoPatch<int16_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUInt16Patch()
{
    TestOpenForPatch<uint16_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForInt16Patch()
{
    TestOpenForPatch<int16_t>();
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUpdateFieldWithoutPatch()
{
    TestUpdateFieldWithoutPatch<int8_t>(true);
    TestUpdateFieldWithoutPatch<uint8_t>(true);
    TestUpdateFieldWithoutPatch<int16_t>(true);
    TestUpdateFieldWithoutPatch<uint16_t>(true);
    TestUpdateFieldWithoutPatch<int32_t>(true);
    TestUpdateFieldWithoutPatch<int64_t>(true);
    TestUpdateFieldWithoutPatch<uint64_t>(true);

    TestUpdateFieldWithoutPatch<int8_t>(false);
    TestUpdateFieldWithoutPatch<uint8_t>(false);
    TestUpdateFieldWithoutPatch<int16_t>(false);
    TestUpdateFieldWithoutPatch<uint16_t>(false);
    TestUpdateFieldWithoutPatch<int32_t>(false);
    TestUpdateFieldWithoutPatch<int64_t>(false);
    TestUpdateFieldWithoutPatch<uint64_t>(false);
}

void SingleValueAttributeSegmentReaderTest::TestCaseForUpdateFieldWithPatch()
{
    TestUpdateFieldWithPatch<int8_t>(true);
    TestUpdateFieldWithPatch<uint8_t>(true);
    TestUpdateFieldWithPatch<int16_t>(true);
    TestUpdateFieldWithPatch<uint16_t>(true);
    TestUpdateFieldWithPatch<int32_t>(true);
    TestUpdateFieldWithPatch<int64_t>(true);
    TestUpdateFieldWithPatch<uint64_t>(true);

    TestUpdateFieldWithPatch<int8_t>(false);
    TestUpdateFieldWithPatch<uint8_t>(false);
    TestUpdateFieldWithPatch<int16_t>(false);
    TestUpdateFieldWithPatch<uint16_t>(false);
    TestUpdateFieldWithPatch<int32_t>(false);
    TestUpdateFieldWithPatch<int64_t>(false);
    TestUpdateFieldWithPatch<uint64_t>(false);
}

void SingleValueAttributeSegmentReaderTest::TestCaseForSearch()
{
    TestSearch<int8_t>();
    TestSearch<uint8_t>();
    TestSearch<int16_t>();
    TestSearch<uint16_t>();
    TestSearch<int32_t>();
    TestSearch<int64_t>();
    TestSearch<uint64_t>();
    TestSearch<float>();
    TestSearch<double>();

    //test for compress
    TestSearch<int8_t>(true);
    TestSearch<uint8_t>(true);
    TestSearch<int16_t>(true);
    TestSearch<uint16_t>(true);
    TestSearch<int32_t>(true);
    TestSearch<int64_t>(true);
    TestSearch<uint64_t>(true);
    TestSearch<float>(true);
    TestSearch<double>(true);
}

void SingleValueAttributeSegmentReaderTest::TestCaseDoOpenWithoutPatch()
{
    uint32_t data[2] = {1, 2};
    string attrName = "field";
    string segmentPath = FileSystemWrapper::JoinPath(mRoot, attrName);
    string filePath = FileSystemWrapper::JoinPath(segmentPath, "data");
    string content((char*)data, sizeof(data));
    FileSystemWrapper::AtomicStore(filePath, content);

    {
        //TestNeedCompressData
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig(attrName, ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        SingleValueAttributeSegmentReader<uint32_t> reader(attrConfig);
        INDEXLIB_TEST_TRUE(reader.mCompressReader != NULL);
        SegmentInfo segInfo;
        segInfo.docCount = 1;
        SegmentData segData = IndexTestUtil::CreateSegmentData(
                GET_PARTITION_DIRECTORY(), segInfo, 0, 0);
        reader.Open(segData);
        INDEXLIB_TEST_EQUAL((uint32_t)1, reader.mDocCount);
    }

    {
        //Test not CompressData
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig(attrName, ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        SingleValueAttributeSegmentReader<uint32_t> reader(attrConfig);
        INDEXLIB_TEST_TRUE(reader.mCompressReader == NULL);
        SegmentInfo segInfo;
        segInfo.docCount = 2;
        SegmentData segData = IndexTestUtil::CreateSegmentData(
                GET_PARTITION_DIRECTORY(), segInfo, 0, 0);
        reader.Open(segData);
        INDEXLIB_TEST_EQUAL((uint32_t)2, reader.mDocCount);
    }
}

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestIsInMemory);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestGetDataLength);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt32NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt32NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt32Patch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt32Patch);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt64NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt64NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt64Patch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt64Patch);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt16NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt16NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt16Patch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt16Patch);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt8NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt8NoPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUInt8Patch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForInt8Patch);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUpdateFieldWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForUpdateFieldWithPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeSegmentReaderTest, TestCaseForSearch);

IE_NAMESPACE_END(index);
