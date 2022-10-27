#include <fslib/fs/FileSystem.h>
#include <autil/StringUtil.h>
#include "indexlib/util/simple_pool.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/index/normal/attribute/test/single_value_attribute_writer_unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeWriterTest, TestCaseForUInt32AddField);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeWriterTest, TestCaseForUInt32UpdateField);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeWriterTest, TestCaseForDumpPerf);

void SingleValueAttributeWriterTest::CaseSetUp()
{
    mDir = GET_TEST_DATA_PATH();
}

void SingleValueAttributeWriterTest::CaseTearDown()
{
}

void SingleValueAttributeWriterTest::TestCaseForUInt32AddField()
{
    TestAddField<uint32_t>();
}

void SingleValueAttributeWriterTest::TestCaseForUInt32UpdateField()
{
    TestUpdateField<uint32_t>();
}

template<typename T>
void SingleValueAttributeWriterTest::AddData(SingleValueAttributeWriter<T>& writer,
        T expectedData[], uint32_t docNum,
        const AttributeConvertorPtr& convertor,
        BuildResourceMetrics& buildMetrics)
{
    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < (docNum - 1); ++i)
    {
        T value = i * 3 % 10;
        expectedData[i] = value;

        string fieldValue = StringUtil::toString(value);
        ConstString encodeStr = convertor->Encode(ConstString(fieldValue), &pool);
        writer.AddField((docid_t)i, encodeStr);
        EXPECT_EQ((i+1)*sizeof(T), buildMetrics.GetValue(BMT_DUMP_FILE_SIZE));
        EXPECT_EQ(0, buildMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
        EXPECT_EQ(0, buildMetrics.GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE)); 
    }
    // last doc for empty value
    expectedData[docNum - 1] = 0;
    writer.AddField((docid_t)(docNum - 1), ConstString());
}

template<typename T>
void SingleValueAttributeWriterTest::UpdateData(SingleValueAttributeWriter<T>& writer, 
        T expectedData[], uint32_t docNum,
        const AttributeConvertorPtr& convertor,
        BuildResourceMetrics& buildMetrics)
{
    int64_t totalMemUseBefore = buildMetrics.GetValue(BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = buildMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t dumpTempExpandMemUseBefore = buildMetrics.GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
    int64_t dumpFileSizeBefore = buildMetrics.GetValue(BMT_DUMP_FILE_SIZE); 
    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < docNum; ++i)
    {
        if (i % 5 == 0) 
        {
            T value = i * 3 % 12;
            expectedData[i] = value;
                                
            string fieldValue = StringUtil::toString(value);
            ConstString encodeStr = convertor->Encode(ConstString(fieldValue), &pool);
            writer.UpdateField((docid_t)i, encodeStr);
            EXPECT_EQ(totalMemUseBefore, buildMetrics.GetValue(BMT_CURRENT_MEMORY_USE));
            EXPECT_EQ(dumpTempMemUseBefore, buildMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
            EXPECT_EQ(dumpTempExpandMemUseBefore, buildMetrics.GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE));
            EXPECT_EQ(dumpFileSizeBefore, buildMetrics.GetValue(BMT_DUMP_FILE_SIZE)); 
        }
    }
}

void SingleValueAttributeWriterTest::TestCaseForDumpPerf()
{
    config::AttributeConfigPtr attrConfig = 
        AttributeTestUtil::CreateAttrConfig<uint32_t>(false);

    AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()
            ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
    autil::mem_pool::Pool pool;
    SingleValueAttributeWriter<uint32_t> writer(attrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    writer.SetAttrConvertor(convertor);
            
    for (uint32_t i = 0; i < 500000; ++i)
    {
        uint32_t value = i * 3 % 10;
        string fieldValue = StringUtil::toString(value);
        ConstString encodeStr = convertor->Encode(ConstString(fieldValue), &pool);
        writer.AddField((docid_t)i, encodeStr);
    }

    file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    string dirPath = GET_TEST_DATA_PATH() + "dump_path";
    fileSystem->MountInMemStorage(dirPath);
    file_system::DirectoryPtr dir(
            new file_system::InMemDirectory(dirPath, fileSystem));
    
    util::SimplePool dumpPool;
    int64_t beginTime = TimeUtility::currentTime();
    writer.Dump(dir, &dumpPool);
    int64_t interval = TimeUtility::currentTime() - beginTime;

    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}

void SingleValueAttributeWriterTest::CheckDataFile(uint32_t docNum, 
        const string& dataFilePath, size_t typeSize)
{
    FileMeta fileMeta;
    FileSystemWrapper::GetFileMeta(mDir + dataFilePath, fileMeta);
    INDEXLIB_TEST_EQUAL((int64_t)typeSize * docNum, fileMeta.fileLength);
}

IE_NAMESPACE_END(index);
