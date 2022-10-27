#include "indexlib/index/normal/attribute/test/virtual_attribute_data_appender_unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_system_define.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VirtualAttributeDataAppenderTest);

VirtualAttributeDataAppenderTest::VirtualAttributeDataAppenderTest()
{
}

VirtualAttributeDataAppenderTest::~VirtualAttributeDataAppenderTest()
{
}

void VirtualAttributeDataAppenderTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    
    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void VirtualAttributeDataAppenderTest::CaseTearDown()
{
}

void VirtualAttributeDataAppenderTest::TestSimpleProcess()
{
    string docString = "cmd=add,string1=hello,price=4;"
                       "cmd=add,string1=hello1,price=5;"
                       "cmd=add,string1=hello2;";
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "pk:hello", 
                                    "docid=0,price=4"));

    AttributeConfigPtr attrConfig1 = CreateAttributeConfig("join1", ft_int32, "1");
    AttributeConfigPtr attrConfig2 = CreateAttributeConfig("join2", ft_string, "hello");
    
    mSchema->AddVirtualAttributeConfig(attrConfig1);
    mSchema->AddVirtualAttributeConfig(attrConfig2);

    VirtualAttributeDataAppender appender(mSchema->GetVirtualAttributeSchema());
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    appender.AppendData(partitionData);

    MultiFieldAttributeReader reader(mSchema->GetVirtualAttributeSchema());
    reader.Open(partitionData);

    CheckAttribute(reader, "join1", 3, "1");
    CheckAttribute(reader, "join2", 3, "hello");
}

void VirtualAttributeDataAppenderTest::TestAppendVirutalAttributeWithoutAttribute()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    string docString = "cmd=add,string1=hello,price=4";
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, 
                                    "pk:hello", "docid=0"));

    AttributeConfigPtr attrConfig = CreateAttributeConfig("join1", ft_int32, "1");
    mSchema->AddVirtualAttributeConfig(attrConfig);

    VirtualAttributeDataAppender appender(mSchema->GetVirtualAttributeSchema());
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    appender.AppendData(partitionData);

    MultiFieldAttributeReader reader(mSchema->GetVirtualAttributeSchema());
    reader.Open(partitionData);

    CheckAttribute(reader, "join1", 1, "1");
    string attrDir = GET_TEST_DATA_PATH() + "/segment_0_level_0/attribute";
    FSStorageType type = GET_FILE_SYSTEM()->GetStorageType(attrDir, true);
    ASSERT_EQ(FSST_IN_MEM, type);
}

void VirtualAttributeDataAppenderTest::TestCheckDataExist()
{
    AttributeConfigPtr attrConfig = CreateAttributeConfig("int32", ft_int32, "1");
    AttributeSchemaPtr attrSchema;
    VirtualAttributeDataAppender appender(attrSchema);

    //check no virtual attribute dir
    DirectoryPtr emptyDirectory;
    ASSERT_FALSE(appender.CheckDataExist(attrConfig, emptyDirectory));

    //check dir exist no data
    ASSERT_THROW(appender.CheckDataExist(attrConfig,
                    GET_PARTITION_DIRECTORY()), misc::IndexCollapsedException);

    //check single attribute exist
    string dataFile = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), ATTRIBUTE_DATA_FILE_NAME);
    FileSystemWrapper::AtomicStore(dataFile, "");
    ASSERT_TRUE(appender.CheckDataExist(attrConfig, GET_PARTITION_DIRECTORY()));

    //check string attribute offset not exist
    AttributeConfigPtr strAttrConfig = CreateAttributeConfig("string", ft_string, "1");
    ASSERT_THROW(appender.CheckDataExist(strAttrConfig,
                    GET_PARTITION_DIRECTORY()), misc::IndexCollapsedException);

    //check string attribute exist
    string offsetFile = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), ATTRIBUTE_OFFSET_FILE_NAME);
    FileSystemWrapper::AtomicStore(offsetFile, "");
    ASSERT_TRUE(appender.CheckDataExist(strAttrConfig, GET_PARTITION_DIRECTORY()));
}

AttributeConfigPtr VirtualAttributeDataAppenderTest::CreateAttributeConfig(
        const string& name, FieldType type, const string& defaultValue)
{
    FieldConfigPtr fieldConfig(new FieldConfig(name, type, false, true));
    fieldConfig->SetDefaultValue(defaultValue);
    AttributeConfigPtr virtualAttrConfig(
            new AttributeConfig(AttributeConfig::ct_virtual));
    virtualAttrConfig->Init(fieldConfig);
    return virtualAttrConfig;
}


void VirtualAttributeDataAppenderTest::CheckAttribute(
        const MultiFieldAttributeReader& attrReaders,
        const string& attrName, uint32_t docCount,
        const string& expectValue)
{
    AttributeReaderPtr attrReader = attrReaders.GetAttributeReader(attrName);
    ASSERT_TRUE(attrReader);
    for (docid_t i = 0; i < (docid_t)docCount; i++)
    {
        string value;
        attrReader->Read(i, value);
        ASSERT_EQ(expectValue, value);
    }
}

IE_NAMESPACE_END(index);
