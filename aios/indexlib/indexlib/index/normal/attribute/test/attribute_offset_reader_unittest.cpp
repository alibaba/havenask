#include "indexlib/index/normal/attribute/test/attribute_offset_reader_unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/simple_pool.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeOffsetReaderTest);

AttributeOffsetReaderTest::AttributeOffsetReaderTest()
{
}

AttributeOffsetReaderTest::~AttributeOffsetReaderTest()
{
}

void AttributeOffsetReaderTest::CaseSetUp()
{
}

void AttributeOffsetReaderTest::CaseTearDown()
{
}

AttributeConfigPtr AttributeOffsetReaderTest::CreateAttrConfig(
        const string& compressTypeStr)
{
    AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>();
    FieldConfigPtr fieldConfig = attrConfig->GetFieldConfig();
    fieldConfig->SetMultiValue(true);
    fieldConfig->SetCompressType(compressTypeStr);
    return attrConfig;
}

void AttributeOffsetReaderTest::TestInit()
{
    {
        // test init uncompress attribute
        AttributeConfigPtr attrConfig = CreateAttrConfig("");
        AttributeOffsetReader reader(attrConfig);

        uint8_t buffer[16];
        reader.Init(buffer, 16, 3);
        ASSERT_FALSE(reader.mIsCompressOffset);
        ASSERT_TRUE(reader.IsU32Offset());
    }

    {
        // test init compress attribute
        AttributeConfigPtr attrConfig = CreateAttrConfig("uniq|equal");
        AttributeOffsetReader reader(attrConfig);
        uint8_t buffer[16];
        uint32_t* head = (uint32_t*) buffer;
        *head = 8;
        uint32_t* tail = (uint32_t*)(buffer + 12);
        *tail = UINT32_OFFSET_TAIL_MAGIC;

        reader.Init(buffer, 16, 7);
        ASSERT_TRUE(reader.mIsCompressOffset);
        ASSERT_TRUE(reader.IsU32Offset());
    }
}

void AttributeOffsetReaderTest::TestInitUncompressOffsetData()
{
    //check unupdatable offset reader not generate slice file
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "int32:true", SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", SFP_OFFLINE);

    DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(
            "segment_0_level_0", true);
    segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);

    DirectoryPtr attrDirectory = segDirectory->GetDirectory("attribute/field", true);
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeOffsetReader reader(attrConfig);
    uint8_t* baseAddress;
    uint64_t offsetFileLen;
    SliceFileReaderPtr sliceFileReader;
    reader.InitCompressOffsetData(attrDirectory, baseAddress, offsetFileLen, sliceFileReader);
    ASSERT_TRUE(!sliceFileReader);
    ASSERT_EQ((uint64_t)20, offsetFileLen);
}

void AttributeOffsetReaderTest::TestGetOffset()
{
    // uncompress
    InnerTestOffsetReader(false, "", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
                          "12,125,234,1234,643,12345,123431", 0);

    // uncompress, updatable
    InnerTestOffsetReader(true, "", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
                          "12,125,234,1234,643,12345,123431", 0);

    // compress
    InnerTestOffsetReader(false, "equal", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
                          "12,125,234,1234,643,12345,123431", 0);

    // compress, updatable
    InnerTestOffsetReader(true, "equal", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 
                          "12,125,234,1234,643,12345,123431", 0);
}

void AttributeOffsetReaderTest::TestSetOffset()
{
    // uncompress, u32, no expand
    InnerTestOffsetReader(true, "", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                          "12,125,234,1234,643,12345,123431", 100);

    // uncompress, u64, no expand
    InnerTestOffsetReader(true, "", 1234,
                          "12,125,234,1234,643,12345,123431", 100);

    // uncompress, u32, expand
    InnerTestOffsetReader(true, "", 123500,
                          "12,125,234,1234,643,12345,123431", 100);

    // compress, inplace update
    InnerTestOffsetReader(true, "", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                          "12,13,14,15,16,17,18,19,20", 1);

    // compress, expand update
    InnerTestOffsetReader(true, "", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                          "12,13,14,15,16,17,18,19,20", 1000);
}

void AttributeOffsetReaderTest::PrepareUnCompressedOffsetBuffer(
        bool isU32Offset, uint32_t value[], uint32_t valueLen,
        uint8_t*& buffer, uint64_t& bufferLength)
{
    if (isU32Offset)
    {
        buffer = (uint8_t*)new uint32_t[valueLen];
        for (size_t i = 0; i < valueLen; i++)
        {
            ((uint32_t*)buffer)[i] = value[i];
        }
        bufferLength = valueLen * sizeof(uint32_t);
    }
    else
    {
        buffer = (uint8_t*)new uint64_t[valueLen];
        for (size_t i = 0; i < valueLen; i++)
        {
            ((uint64_t*)buffer)[i] = value[i];
        }
        bufferLength = valueLen * sizeof(uint64_t);
    }
 
}

void AttributeOffsetReaderTest::InnerTestOffsetReader(
        bool isUpdatable, const string& compressTypeStr,
        uint32_t offsetThreshold, const string& offsetStr, bool updateStep)
{
    TearDown();
    SetUp();

    AttributeConfigPtr attrConfig = CreateAttrConfig(compressTypeStr);
    attrConfig->SetUpdatableMultiValue(isUpdatable);
    attrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThreshold);

    vector<uint64_t> offsetVec;
    StringUtil::fromString(offsetStr, offsetVec, ",");
    uint32_t docCount = offsetVec.size() - 1;

    util::SimplePool pool;
    // prepare offset file
    AdaptiveAttributeOffsetDumper offsetDumper(&pool);
    offsetDumper.Init(attrConfig);
    for (size_t i = 0; i < offsetVec.size(); i++)
    {
        offsetDumper.PushBack(offsetVec[i]);
    }
    DirectoryPtr attrDirectory = GET_SEGMENT_DIRECTORY();
    FileWriterPtr offsetFileWriter = attrDirectory->CreateFileWriter(
            ATTRIBUTE_OFFSET_FILE_NAME);
    offsetDumper.Dump(offsetFileWriter);
    offsetFileWriter->Close();

    // init offset reader
    AttributeOffsetReader offsetReader(attrConfig);
    offsetReader.Init(attrDirectory, docCount);

    // check offset
    for (uint32_t i = 0; i < docCount; i++)
    {
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));
    }
    
    if (!isUpdatable || updateStep == 0)
    {
        return;
    }

    bool isU32Offset = offsetReader.IsU32Offset();
    for (uint32_t i = 0; i < docCount; i++)
    {
        offsetVec[i] += updateStep;
        ASSERT_TRUE(offsetReader.SetOffset((docid_t)i, offsetVec[i]));
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));

        if (offsetVec[i] > offsetThreshold)
        {
            isU32Offset = false;
        }
        ASSERT_EQ(isU32Offset, offsetReader.IsU32Offset());
    }
    for (uint32_t i = 0; i < docCount; i++)
    {
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));
    }
}

void AttributeOffsetReaderTest::TestGetOffsetPerfLongCaseTest()
{
    InnerTestGetOffsetPerf(1);
    InnerTestGetOffsetPerf(8);
    InnerTestGetOffsetPerf(16);
    InnerTestGetOffsetPerf(64);
    InnerTestGetOffsetPerf(256);
    InnerTestGetOffsetPerf(1024);
}

void AttributeOffsetReaderTest::InnerTestGetOffsetPerf(uint32_t step)
{
    const size_t READ_COUNT = 100000000;
    const uint32_t DOC_COUNT = 30000000;
    const uint32_t TIMES = 10;

    AttributeConfigPtr attrConfig = CreateAttrConfig("");

    vector<uint32_t> offsetVec;
    offsetVec.resize(DOC_COUNT + 1, 10101);

    AttributeOffsetReader offsetReader(attrConfig);
    offsetReader.Init((uint8_t*)offsetVec.data(), 
                      offsetVec.size() * sizeof(uint32_t),
                      DOC_COUNT);

    uint32_t value = 0;
    docid_t docId = 0;
    int64_t beginTime = TimeUtility::currentTime();

    for (size_t i = 0; i < TIMES; ++i)
    {
        for (size_t j = 0; j < READ_COUNT; ++j)
        {
            value = offsetReader.GetOffset(docId);
            docId += step;
            if (docId >= (docid_t)DOC_COUNT)
            {
                docId = docId % DOC_COUNT;
            }
        }
    }

    int64_t endTime = TimeUtility::currentTime();
    cout << "##########" << value <<"##########" << endl;
    cout << "*** read times:" << READ_COUNT << " inc step:" << step
         << " avg use time: " << (endTime - beginTime)/1000 / TIMES << "ms." << endl;
}

IE_NAMESPACE_END(index);

