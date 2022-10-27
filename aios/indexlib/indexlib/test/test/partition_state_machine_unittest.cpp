#include "indexlib/test/test/partition_state_machine_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/document/locator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, PartitionStateMachineTest);

PartitionStateMachineTest::PartitionStateMachineTest()
{
}

PartitionStateMachineTest::~PartitionStateMachineTest()
{
}

void PartitionStateMachineTest::CaseSetUp()
{
}

void PartitionStateMachineTest::CaseTearDown()
{
}

void PartitionStateMachineTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;long1:uint32;";
    string index = "pk:primarykey64:string1";
    string attr = "long1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, "");
    INDEXLIB_TEST_TRUE(schema);
    schema->SetAutoUpdatePreference(false);

    string docString = "cmd=add,string1=pk1,long1=2,ts=100,vs=1;"       \
                       "cmd=add,string1=pk2,long1=4,ts=100,vs=2";

    IndexPartitionOptions options;
    options.GetBuildConfig(true).enablePackageFile = false;
    options.GetBuildConfig(false).enablePackageFile = false;    
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:pk1", 
                                    "long1=2,docid=0"));

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(directory->IsExist("segment_1_level_0"));
    {
        index_base::Version version;
        ASSERT_TRUE(version.Load(directory, "version.1"));
        Locator locator = version.GetLocator();
        ASSERT_TRUE(locator.IsValid());
        common::IndexLocator indexLocator;
        indexLocator.fromString(locator.GetLocator());
        EXPECT_EQ(101, indexLocator.getOffset());        
    }
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "cmd=add,string1=pk3,long1=3,ts=103,locator=0:105", 
                                    "pk:pk2", 
                                    "long1=4,docid=1"));

    ASSERT_TRUE(directory->IsExist("segment_3_level_0"));    
    {
        index_base::Version version;
        ASSERT_TRUE(version.Load(directory, "version.3")); 
        Locator locator = version.GetLocator();
        ASSERT_TRUE(locator.IsValid());
        common::IndexLocator indexLocator;
        indexLocator.fromString(locator.GetLocator());
        EXPECT_EQ(105, indexLocator.getOffset()); 
    }    

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "cmd=add,string1=pk4,long1=3,ts=106,locator=0:107", 
                                    "", 
                                    ""));
    
    ASSERT_TRUE(directory->IsExist("segment_4_level_0"));
    {
        index_base::Version version;
        ASSERT_TRUE(version.Load(directory, "version.4")); 
        Locator locator = version.GetLocator();
        ASSERT_TRUE(locator.IsValid());
        common::IndexLocator indexLocator;
        indexLocator.fromString(locator.GetLocator());
        EXPECT_EQ(107, indexLocator.getOffset()); 
    } 
    
    //not support inc reopen
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "cmd=add,string1=pk3,long1=30,ts=200", 
                                    "pk:pk3", "long1=30,docid=4"));
}

void PartitionStateMachineTest::TestInvertedIndex()
{
    SCOPED_TRACE("Failed");
    string field = "text1:text;";
    string index = "pack1:pack:text1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    INDEXLIB_TEST_TRUE(schema);

    string docString = "cmd=add,text1=t1 t2 t1;";

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "pack1:t1", 
                                    "docid=0,pos=0#2;"));
}

void PartitionStateMachineTest::TestDocIdQuery()
{
    string field = "text1:text;long1:uint32";
    string index = "pack1:pack:text1;";
    string attr = "long1;";
    string summary = "text1;long1";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attr, summary);
    INDEXLIB_TEST_TRUE(schema);
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    string docString = "cmd=add,text1=t1,long1=1;"
                       "cmd=add,text1=t2,long1=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "__docid:0", 
                                    "docid=0,long1=1,text1=t1;"));
}

void PartitionStateMachineTest::TestBuildWithBinaryString()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;long1:uint32;";
    string index = "pk:primarykey64:string1";
    string attr = "long1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, "");
    INDEXLIB_TEST_TRUE(schema);
    schema->SetAutoUpdatePreference(false);

    string fullDocStr = "cmd=add,string1=pk1,long1=2,ts=100,vs=1;"
                       "cmd=add,string1=pk2,long1=4,ts=100,vs=2";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(schema, fullDocStr, true);
    string docBinaryStr = PartitionStateMachine::SerializeDocuments(docs);

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    psm.SetPsmOption("documentFormat", "binary");
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docBinaryStr, "pk:pk1", 
                                    "long1=2,docid=0"));

    string incDocStr = "cmd=add,string1=pk3,long1=3";
    vector<NormalDocumentPtr> incDocs = DocumentCreator::CreateDocuments(schema, incDocStr, true);
    string incDocBinaryStr = PartitionStateMachine::SerializeDocuments(incDocs);    
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, incDocBinaryStr, "pk:pk2", 
                                    "long1=4,docid=1"));

    string rtDocStr = "cmd=add,string1=pk3,long1=30,ts=200";
    vector<NormalDocumentPtr> rtDocs = DocumentCreator::CreateDocuments(schema, rtDocStr, true);
    string rtDocBinStr = PartitionStateMachine::SerializeDocuments(rtDocs);
            
    //not support inc reopen
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocBinStr, 
                                    "pk:pk3", "long1=30,docid=3"));
}

void PartitionStateMachineTest::TestCustomizedDocumentFactory()
{
    string field = "string1:string;long1:uint32;";
    string index = "pk:primarykey64:string1";
    string attr = "long1;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            field, index, attr, "");
    CustomizedConfigVector docFactoryConfig;
    CustomizedConfigPtr cfg1(new CustomizedConfig);

    cfg1->SetPluginName("customized_doc_for_psm_test");
    cfg1->SetId(config::CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
    docFactoryConfig.push_back(cfg1);
    schema->SetCustomizedDocumentConfig(docFactoryConfig);
    IndexPartitionOptions options;
    partition::IndexPartitionResource resource;
    resource.indexPluginPath = BUILD_LD_PATH; 
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false, resource);
    ASSERT_TRUE(psm.Init(schema, options,
                         GET_TEST_DATA_PATH(), "psm", ""));

    string fullDocStr = "cmd=add,string1=pk1,long1=2,ts=100,myId=11;"
        "cmd=add,string1=pk2,long1=4,ts=100,myId=12";

    vector<DocumentPtr> docs = psm.CreateDocuments(fullDocStr);
    string docBinaryStr = PartitionStateMachine::SerializeDocuments(docs);
    vector<DocumentPtr> resultDocs =
        psm.DeserializeDocuments(docBinaryStr);
        
    ASSERT_EQ(2, resultDocs.size());
    ASSERT_EQ(11, resultDocs[0]->GetDocId());
    ASSERT_EQ(12, resultDocs[1]->GetDocId());
}

IE_NAMESPACE_END(test);

