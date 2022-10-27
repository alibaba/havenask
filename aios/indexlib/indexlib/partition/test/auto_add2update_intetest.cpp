#include "indexlib/partition/test/auto_add2update_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, AutoAdd2updateTest);

AutoAdd2updateTest::AutoAdd2updateTest()
{
}

AutoAdd2updateTest::~AutoAdd2updateTest()
{
}

void AutoAdd2updateTest::CaseSetUp()
{
}

void AutoAdd2updateTest::CaseTearDown()
{
}

void AutoAdd2updateTest::TestSimpleProcess()
{
    string field = "string1:string;attr1:int32;attr2:int32;attr3:int32;"
                   "attr4:string:true:true;attr5:int32:true:true;attr6:string:false:true";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5,attr6";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    // set support auto add2update
    schema->SetAutoUpdatePreference(true);
    IndexPartitionOptions options;
   
    string fullDocs = "cmd=add,string1=pk0,attr1=1,attr2=2,attr3=3,attr4=hello world,attr5=5 6,attr6=hello;"
                      "cmd=add,string1=pk0,attr1=11,attr2=12,attr3=13,attr4=hello kitty,attr5=50 60,attr6=hi;";

    PartitionStateMachine psm;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk0",
                             "attr1=11,attr2=12,attr3=13,attr4=hello kitty,attr5=50 60,attr6=hi"));
}

void AutoAdd2updateTest::TestIncCoverRt()
{
    string field = "string1:string;attr1:int32;attr2:int32;attr3:int32;"
                   "attr4:string:true:true;attr5:int32:true:true;attr6:string:false:true";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5,attr6";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    // set support auto add2update
    schema->SetAutoUpdatePreference(true);
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    options.GetOfflineConfig().buildConfig.keepVersionCount = 5;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1);
    
    string fullDocs = "cmd=add,string1=pk0,attr1=1,attr2=2,attr3=3,attr4=hello world,attr5=5 6,attr6=hello,ts=1;"
                      "cmd=add,string1=pk1,attr1=11,attr2=12,attr3=13,attr4=hello kitty,attr5=50 60,attr6=hi,ts=1;";

    string rtDocs = "cmd=add,string1=pk0,attr1=2,attr2=3,attr3=4,attr4=hello,attr5=7 8,attr6=hi,ts=2;"
        "cmd=add,string1=pk1,attr1=22,attr2=22,attr3=23,attr4=kitty hi,attr5=50 60,attr6=he,ts=3;";

    PartitionStateMachine psm;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs, "pk:pk0",
                             "attr1=2,attr2=3,attr3=4,attr4=hello,attr5=7 8,attr6=hi"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1",
                             "attr1=22,attr2=22,attr3=23,attr4=kitty hi,attr5=50 60,attr6=he"));
    string incDocs = "cmd=add,string1=pk0,attr1=21,attr2=31,attr3=41,attr4=hello 1,attr5=71 81,attr6=he,ts=2;"
        "cmd=add,string1=pk1,attr1=11,attr2=12,attr3=13,attr4=hello kitty,attr5=50 60,attr6=hi,ts=2;##stopTs=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "pk:pk0",
                             "attr1=21,attr2=31,attr3=41,attr4=hello 1,attr5=71 81,attr6=he"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1",
                             "attr1=22,attr2=22,attr3=23,attr4=kitty hi,attr5=50 60,attr6=he")); 
}

void AutoAdd2updateTest::TestBug_22229643()
{
    string field = "string1:string;attr1:int32;attr2:int32;attr3:int32;"
                   "attr4:string:true:true:uniq;attr5:int32:true:true;attr6:string:false:true";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5,attr6";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    // set support auto add2update
    schema->SetAutoUpdatePreference(true);
    IndexPartitionOptions options;

    string fullDocs
        = "cmd=add,string1=pk0,attr1=1,attr2=2,attr3=3,attr4=hello world,attr5=5 6,attr6=hello;";


    PartitionStateMachine psm;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk0",
                             "attr1=1,attr2=2,attr3=3,attr4=hello world,attr5=5 6,attr6=hello"));

    string incDocs = "cmd=add,string1=pk0,attr1=11,attr2=12,attr3=13,attr4=,attr5=50 60,attr6=hi;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk0",
                             "attr1=11,attr2=12,attr3=13,attr4=,attr5=50 60,attr6=hi"));
}

IE_NAMESPACE_END(partition);

