#include "indexlib/index/normal/attribute/test/multi_field_attribute_reader_unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldAttributeReaderTest);

MultiFieldAttributeReaderTest::MultiFieldAttributeReaderTest()
{
}

MultiFieldAttributeReaderTest::~MultiFieldAttributeReaderTest()
{
}

void MultiFieldAttributeReaderTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void MultiFieldAttributeReaderTest::CaseTearDown()
{
}

void MultiFieldAttributeReaderTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    psm.Init(mSchema, options, GET_TEST_DATA_PATH());
    
    string docString = "cmd=add,string1=hello,price=4;";
    psm.Transfer(BUILD_FULL, docString, 
                 "pk:hello", "docid=0,price=6");
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    MultiFieldAttributeReader reader(mSchema->GetAttributeSchema());
    reader.Open(partitionData);
    AttributeReaderPtr attrReader = reader.GetAttributeReader("string1");
    ASSERT_TRUE(attrReader);
    ASSERT_EQ(AT_STRING, attrReader->GetType());

    attrReader = reader.GetAttributeReader("price");
    ASSERT_TRUE(attrReader);
    ASSERT_EQ(AT_UINT32, attrReader->GetType());

    attrReader = reader.GetAttributeReader("string2");
    ASSERT_FALSE(attrReader);
}

void MultiFieldAttributeReaderTest::TestLazyLoad()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    psm.Init(mSchema, options, GET_TEST_DATA_PATH());
    
    string docString = "cmd=add,string1=hello,price=4;";
    psm.Transfer(BUILD_FULL, docString, 
                 "pk:hello", "docid=0,price=6");
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    MultiFieldAttributeReader reader(mSchema->GetAttributeSchema(), NULL, true);
    reader.Open(partitionData);
    AttributeReaderPtr attrReader = reader.GetAttributeReader("string1");
    ASSERT_TRUE(attrReader);
    ASSERT_TRUE(attrReader->IsLazyLoad());        
    ASSERT_EQ(AT_STRING, attrReader->GetType());

    attrReader = reader.GetAttributeReader("price");
    ASSERT_TRUE(attrReader);
    ASSERT_TRUE(attrReader->IsLazyLoad());    
    ASSERT_EQ(AT_UINT32, attrReader->GetType());

    attrReader = reader.GetAttributeReader("string2");
    ASSERT_FALSE(attrReader);
}

IE_NAMESPACE_END(index);

