#include "indexlib/index/normal/attribute/test/var_num_attribute_writer_unittest.h"
#include "indexlib/index/normal/attribute/attribute_value_type_traits.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/config/attribute_config.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

void VarNumAttributeWriterTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void VarNumAttributeWriterTest::CaseTearDown()
{
}
    
void VarNumAttributeWriterTest::TestCaseForUpdateField()
{
    {
        SCOPED_TRACE("TEST for uniq update");
        BuildResourceMetrics buildMetrics;
        buildMetrics.Init();
        int64_t dataLen = 0;
        VarNumAttributeWriter<uint8_t>* writer =
            PrepareWriter<uint8_t>(true, &buildMetrics);
        ConstString encodeStr = writer->mAttrConvertor->Encode(ConstString("1"), &mPool);
        writer->AddField((docid_t)0, encodeStr);
        dataLen += writer->mAttrConvertor->Decode(encodeStr).data.size();
        EXPECT_EQ(1*sizeof(uint64_t) + dataLen,
                  buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));
        
        encodeStr = writer->mAttrConvertor->Encode(ConstString("2"), &mPool);
        writer->AddField((docid_t)1, encodeStr);
        dataLen += writer->mAttrConvertor->Decode(encodeStr).data.size();
        EXPECT_EQ(2*sizeof(uint64_t) + dataLen,
                  buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));
        
        writer->UpdateField((docid_t)0, encodeStr);
        EXPECT_EQ(2*sizeof(uint64_t) + dataLen,
                  buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));        
        
        //data format: 1bytes = count, the left is data
        uint8_t* data = NULL;
        uint32_t dataLength = 0;
        writer->mAccessor->ReadData(0, data, dataLength);
        ASSERT_EQ((uint64_t)2, dataLength);
        ASSERT_EQ(2, (char)data[1]);
        ASSERT_EQ((uint64_t)6, writer->mAccessor->GetOffset(0));

        encodeStr = writer->mAttrConvertor->Encode(ConstString("3"), &mPool);
        writer->UpdateField((docid_t)1, encodeStr);
        writer->mAccessor->ReadData(1, data, dataLength);
        ASSERT_EQ((uint64_t)2, dataLength);
        ASSERT_EQ(3, (char)data[1]);
        ASSERT_EQ((uint64_t)12, writer->mAccessor->GetOffset(1));
        delete writer;
    }
    {
        SCOPED_TRACE("TEST for normal update");
        BuildResourceMetrics buildMetrics;
        buildMetrics.Init();
        int64_t dataLen = 0;
        VarNumAttributeWriter<uint8_t>* writer = 
            PrepareWriter<uint8_t>(false, &buildMetrics);
        ConstString encodeStr = writer->mAttrConvertor->Encode(
                ConstString("1"), &mPool);
        writer->AddField((docid_t)0, encodeStr);
        dataLen += writer->mAttrConvertor->Decode(encodeStr).data.size();
        
        EXPECT_EQ(1*sizeof(uint64_t)+ dataLen,
                  buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));

        int64_t dumpTempBufferSizeBefore =
            buildMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE);
        
        encodeStr = writer->mAttrConvertor->Encode(ConstString("2"), &mPool);
        writer->UpdateField((docid_t)0, encodeStr);
        dataLen += writer->mAttrConvertor->Decode(encodeStr).data.size();
        EXPECT_EQ(dumpTempBufferSizeBefore,
                  buildMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
        EXPECT_EQ(1*sizeof(uint64_t)+ dataLen,
                  buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));

        uint8_t* data = NULL;
        uint32_t dataLength = 0;
        writer->mAccessor->ReadData(0, data, dataLength);
        ASSERT_EQ((uint64_t)2, dataLength);
        ASSERT_EQ(2, (char)data[1]);
        ASSERT_EQ((uint64_t)6, writer->mAccessor->GetOffset(0));
        delete writer;
    }
}

void VarNumAttributeWriterTest::TestCaseForWrite()
{
    uint32_t docCount = 100;
    uint32_t valueCountPerField = 3;
    // default attribute data file threshold
    TestAddField<uint8_t>(docCount, valueCountPerField, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));

    // small attribute data file threshold: easy to go beyond the threshold
    uint64_t smallOffsetThreshold = 16;
    TestAddField<uint8_t>(docCount, valueCountPerField, smallOffsetThreshold, sizeof(uint64_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, smallOffsetThreshold, sizeof(uint64_t));

    // Boundary value for uint8 
    uint64_t offsetThresholdForUint8 = docCount * (valueCountPerField * sizeof(uint8_t) + 
            VarNumAttributeFormatter::GetEncodedCountLength(valueCountPerField));
    TestAddField<uint8_t>(docCount, valueCountPerField, offsetThresholdForUint8, sizeof(uint32_t));
    TestAddField<uint8_t>(docCount, valueCountPerField, offsetThresholdForUint8 - 1, sizeof(uint64_t));

    // Boundary value for uint32
    uint64_t offsetThresholdForUint32 = docCount * (valueCountPerField * sizeof(uint32_t) +
            VarNumAttributeFormatter::GetEncodedCountLength(valueCountPerField));
    TestAddField<uint32_t>(docCount, valueCountPerField, offsetThresholdForUint32, sizeof(uint32_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, offsetThresholdForUint32 - 1, sizeof(uint64_t));
}

void VarNumAttributeWriterTest::TestCaseForAddEncodeField()
{
    uint32_t docCount = 3;
    uint32_t valueCountPerField = 3;
    {
        SCOPED_TRACE("encode uint32_t");
        InnerTestAddEncodeField<uint32_t>(docCount, valueCountPerField, 2);
    }
    InnerTestAddEncodeField<int32_t>(docCount, valueCountPerField, 2);

    InnerTestAddEncodeField<uint8_t>(docCount, valueCountPerField, 4);
    InnerTestAddEncodeField<int8_t>(docCount, valueCountPerField, 3);
        
    uint64_t smallOffsetThreshold = 10;
    InnerTestAddEncodeField<uint32_t>(docCount, valueCountPerField, 3, smallOffsetThreshold, sizeof(uint64_t));
}

void VarNumAttributeWriterTest::PrepareData(
        vector<string> &fields, uint32_t uniqDocCount, 
        uint32_t valueCountPerField, uint32_t loop)
{
    vector<string> uniqFields;
    for (uint32_t i = 0; i < uniqDocCount; i++)
    {
        stringstream ss;
        for (uint32_t j = 0; j < valueCountPerField; j++)
        {
            if (j != 0)
            {
                ss << MULTI_VALUE_SEPARATOR;
            }
            ss << i + j;
        }
        uniqFields.push_back(ss.str());
    }
    fields.insert(fields.end(), uniqFields.begin(), uniqFields.end());
    for (uint32_t i = 0; i < loop; i++)
    {
        fields.insert(fields.end(), uniqFields.begin(), uniqFields.end());
    }
}

void VarNumAttributeWriterTest::TestCaseForDumpPerf()
{
    config::AttributeConfigPtr attrConfig = 
        AttributeTestUtil::CreateAttrConfig<uint32_t>(true);

    AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    VarNumAttributeWriter<uint32_t> writer(attrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    writer.SetAttrConvertor(convertor);
            
    uint32_t valueCountPerField = 10;

    std::vector<std::string> fields;
    PrepareData(fields, 50, valueCountPerField, 0);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++)
    {
        autil::ConstString encodeStr = convertor->Encode(autil::ConstString(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    string dirPath = mRootDir + "dump_path";
    fileSystem->MountInMemStorage(dirPath);
    file_system::DirectoryPtr dir(
            new file_system::InMemDirectory(dirPath, fileSystem));
    
    int64_t beginTime = TimeUtility::currentTime();
    util::SimplePool dumpPool;
    writer.Dump(dir, &dumpPool);
    int64_t interval = TimeUtility::currentTime() - beginTime;

    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}


void VarNumAttributeWriterTest::TestCaseForDumpPerfForUniqField()
{
    config::AttributeConfigPtr attrConfig = 
        AttributeTestUtil::CreateAttrConfig<uint32_t>(true);
    attrConfig->SetUniqEncode(true);

    AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    VarNumAttributeWriter<uint32_t> writer(attrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    writer.SetAttrConvertor(convertor);
            
    uint32_t valueCountPerField = 10;

    std::vector<std::string> fields;
    PrepareData(fields, 50000, valueCountPerField, 10);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++)
    {
        autil::ConstString encodeStr = convertor->Encode(autil::ConstString(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    string dirPath = mRootDir + "dump_path";
    fileSystem->MountInMemStorage(dirPath);
    file_system::DirectoryPtr dir(
            new file_system::InMemDirectory(dirPath, fileSystem));
    
    int64_t beginTime = TimeUtility::currentTime();
    util::SimplePool dumpPool;
    writer.Dump(dir, &dumpPool);
    int64_t interval = TimeUtility::currentTime() - beginTime;

    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}

void VarNumAttributeWriterTest::TestCaseForDumpCompressedOffset()
{
    // expectLen : header(4+4) + slotItemMeta (8) + slotItemData(4+3*1) + tail(4) = 27
    InnerTestDumpCompressedOffset(false, 27);

    // expectLen : header(4+4) + slotItemMeta (8) + slotItemData(8+8+3*1) + tail(4) = 39
    InnerTestDumpCompressedOffset(true, 39);
}

void VarNumAttributeWriterTest::TestCaseForDumpDataInfo()
{
    {
        SCOPED_TRACE("TEST for uniq update");
        VarNumAttributeWriter<uint8_t>* writer = PrepareWriter<uint8_t>(true);
        ConstString encodeStr = writer->mAttrConvertor->Encode(ConstString("12"), &mPool);
        writer->AddField((docid_t)0, encodeStr);
            
        encodeStr = writer->mAttrConvertor->Encode(
                ConstString("234"), &mPool);
        writer->AddField((docid_t)1, encodeStr);
        writer->AddField((docid_t)2, encodeStr);
        writer->AddField((docid_t)3, encodeStr);

        encodeStr = writer->mAttrConvertor->Encode(
                ConstString("23456"), &mPool);
        writer->UpdateField((docid_t)0, encodeStr);
        //data format: 1bytes = count, the left is data
        file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
        string dirPath = mRootDir + "uniq_dump_path";
        fileSystem->MountInMemStorage(dirPath);
        file_system::DirectoryPtr dir(
                new file_system::InMemDirectory(dirPath, fileSystem));
        util::SimplePool dumpPool;
        writer->Dump(dir, &dumpPool);
        delete writer;
        CheckDataInfo(dir, "attr_name", 2, 6);
    }

    {
        SCOPED_TRACE("TEST for normal update");
        VarNumAttributeWriter<uint8_t>* writer = PrepareWriter<uint8_t>(false);
        ConstString encodeStr = writer->mAttrConvertor->Encode(ConstString("12"), &mPool);
        writer->AddField((docid_t)0, encodeStr);
            
        encodeStr = writer->mAttrConvertor->Encode(
                ConstString("234"), &mPool);
        writer->AddField((docid_t)1, encodeStr);
        writer->AddField((docid_t)2, encodeStr);
        writer->AddField((docid_t)3, encodeStr);

        encodeStr = writer->mAttrConvertor->Encode(
                ConstString("23456"), &mPool);
        writer->UpdateField((docid_t)0, encodeStr);
        //data format: 1bytes = count, the left is data
        file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
        string dirPath = mRootDir + "normal_dump_path";
        fileSystem->MountInMemStorage(dirPath);
        file_system::DirectoryPtr dir(
                new file_system::InMemDirectory(dirPath, fileSystem));
        util::SimplePool dumpPool;
        writer->Dump(dir, &dumpPool);
        delete writer;
        CheckDataInfo(dir, "attr_name", 4, 6);
    }
}

void VarNumAttributeWriterTest::InnerTestDumpCompressedOffset(bool isUpdatable, size_t expectLen)
{
    TearDown();
    SetUp();

    config::AttributeConfigPtr attrConfig = 
        AttributeTestUtil::CreateAttrConfig<uint32_t>(true, true);
    attrConfig->SetUpdatableMultiValue(isUpdatable);
 
    std::vector<std::string> fields;
    PrepareData(fields, 2, 10, 0);  // create 2 field(each 10 value)

    AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    VarNumAttributeWriter<uint32_t> writer(attrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    writer.SetAttrConvertor(convertor);

    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++)
    {
        autil::ConstString encodeStr = convertor->Encode(autil::ConstString(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    string dirPath = mRootDir + "dump_path";
    fileSystem->MountInMemStorage(dirPath);
    file_system::DirectoryPtr dir(
            new file_system::InMemDirectory(dirPath, fileSystem));
    util::SimplePool dumpPool;
    writer.Dump(dir, &dumpPool);

    std::string offsetFilePath = attrConfig->GetAttrName() + "/offset";
    size_t offsetLen = dir->GetFileLength(offsetFilePath);
    EXPECT_EQ(expectLen, offsetLen);
}

void VarNumAttributeWriterTest::TestCaseForFloatEncode()
{
    InnerTestAddEncodeFloatField(
            4, "fp16", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2);
    InnerTestAddEncodeFloatField(
            4, "fp16|uniq", 16, sizeof(uint64_t), 100, 2);

    InnerTestAddEncodeFloatField(
            4, "block_fp", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2);
    InnerTestAddEncodeFloatField(
            4, "block_fp|uniq", 16, sizeof(uint64_t), 100, 2);

    InnerTestAddEncodeFloatField(
            4, "int8#127|uniq", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2);
    InnerTestAddEncodeFloatField(
            4, "int8#127", 16, sizeof(uint64_t), 100, 2);

    InnerTestAddEncodeFloatField(4, "", 16, sizeof(uint64_t), 100, 2);
}

void VarNumAttributeWriterTest::CheckDataInfo(
        file_system::DirectoryPtr dir,
        const string& attrName,
        uint32_t uniqItemCount, uint32_t maxItemLen)
{
    file_system::DirectoryPtr attrDir = dir->GetDirectory(attrName, false);
    ASSERT_TRUE(attrDir);

    ASSERT_TRUE(attrDir->IsExist(ATTRIBUTE_DATA_INFO_FILE_NAME));
    string fileContent;
    attrDir->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, fileContent);

    AttributeDataInfo dataInfo;
    dataInfo.FromString(fileContent);
    ASSERT_EQ(uniqItemCount, dataInfo.uniqItemCount);
    ASSERT_EQ(maxItemLen, dataInfo.maxItemLen);
}

void VarNumAttributeWriterTest::InnerTestAddEncodeFloatField(
        uint32_t fixedValueCount, const string& compressTypeStr,
        uint64_t offsetThreshold, int64_t offsetUnitSize,
        uint32_t docCount, uint32_t loop)
{
    TearDown();
    SetUp();

    AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig(
            ft_float, true, "attr_name", 0, compressTypeStr, fixedValueCount);
    attrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThreshold);

    common::AttributeConvertorPtr convertor(
            common::AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    VarNumAttributeWriter<float> writer(attrConfig);
    util::BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    writer.Init(FSWriterParamDeciderPtr(), &buildMetrics);
    writer.SetAttrConvertor(convertor);

    std::vector<std::string> fields;
    PrepareData(fields, docCount, fixedValueCount, loop);

    size_t fieldSize = fixedValueCount * sizeof(float);
    if (attrConfig->GetCompressType().HasFp16EncodeCompress())
    {
        fieldSize = Fp16Encoder::GetEncodeBytesLen(fixedValueCount);
    }
    else if (attrConfig->GetCompressType().HasBlockFpEncodeCompress())
    {
        fieldSize = BlockFpEncoder::GetEncodeBytesLen(fixedValueCount);
    }
    else if (attrConfig->GetCompressType().HasInt8EncodeCompress())
    {
        fieldSize = FloatInt8Encoder::GetEncodeBytesLen(fixedValueCount);        
    }
    
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++)
    {
        autil::ConstString encodeStr =
            convertor->Encode(autil::ConstString(fields[i]), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    AttributeSegmentReaderPtr inMemReader = writer.CreateInMemReader();
    InMemVarNumAttributeReader<float>* floatInMemReader =
        dynamic_cast<InMemVarNumAttributeReader<float>*>(inMemReader.get());
    ASSERT_TRUE(inMemReader);

    for (size_t i = 0; i < fields.size(); i++)
    {
        MultiValueType<float> value;
        ASSERT_TRUE(floatInMemReader->Read(i, value, &pool));
        ASSERT_EQ(fields[i], AttributeValueTypeToString<MultiValueType<float>>::ToString(value));
    }

    AttributeWriterHelper::DumpWriter(writer, mRootDir);

    int64_t dataLen = attrConfig->IsUniqEncode() ?
                      (int64_t)fieldSize * docCount :
                      (int64_t)fieldSize * fields.size();
    
    // check data file
    std::string dataFilePath = 
        mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = 
        mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    storage::FileSystemWrapper::GetFileMeta(dataFilePath, fileMeta);
    EXPECT_EQ(dataLen, fileMeta.fileLength);
    storage::FileSystemWrapper::GetFileMeta(offsetFilePath, fileMeta);
    EXPECT_EQ(offsetUnitSize * (fields.size() + 1), fileMeta.fileLength);
}

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForAddEncodeField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForUpdateField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForDumpPerf);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForDumpPerfForUniqField);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForDumpCompressedOffset);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForDumpDataInfo);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeWriterTest, TestCaseForFloatEncode);

IE_NAMESPACE_END(index);
