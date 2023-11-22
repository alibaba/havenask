#include "indexlib/common_define.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace indexlib::test;
using namespace indexlib::util;

namespace indexlib { namespace index {

class DynamicIndexTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    void CaseSetUp() override
    {
        util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &_options);
    }
    void CaseTearDown() override {}

    void TestRealtime();
    void TestUpdateAsyncDumpingSegment();
    void TestOffline();
    void TestOfflineSortBiuld();
    // for debug
    // void TestOffline2();
    void TestBitmap();
    void TestBitmapSortBuild();
    void TestNullTerm();
    void TestMultiSharding();
    void TestSubDocs();
    void TestSubDocsMultiSharding();
    void TestMergePatches();
    void TestMergePatchesMultiSharding();
    void TestUpdateWithDisabledField();

    void TestAdaptiveBitmap();

private:
    void InnerTestOffline(bool sortBuild, bool multiSharding);
    void InnerTestMergePatches(bool multiSharding);
    void InnerTestBitmap(bool sortBuild);

private:
    void DoTestSubDocs(bool multiSharding);

private:
    config::IndexPartitionOptions _options;
};

void DynamicIndexTest::TestRealtime()
{
    std::string field = "price:int64;title:string;pk:string";
    std::string index = "price:number:price;title:string:title;pk:primarykey64:pk";
    std::string attribute = "pk;price;title";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    schema->GetIndexSchema()->GetIndexConfig("title")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("title")->SetOptionFlag(0);
    schema->GetAttributeSchema()->GetAttributeConfig("title")->SetUpdatableMultiValue(true);
    std::string rootDir = GET_TEMP_DATA_PATH();

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,price=100,title=hello,pk=a,ts=1;"
                         "cmd=add,price=100,title=world,pk=b,ts=1;"
                         "cmd=add,price=200,title=hello,pk=c,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a;docid=2,pk=c"));
    docStr = "cmd=add,price=200,title=hello,pk=d,ts=2;"
             "cmd=add,price=200,title=world,pk=e,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=2,pk=c;docid=3,pk=d;docid=4,pk=e"));
    // update built
    docStr = "cmd=add,modify_fields=price#title,modify_values=200#hello,price=100,title=new,pk=c,ts=2;";
    // cmd=add,__last_value__price=200,price=100,pk=c,ts=2;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=3,pk=d;docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a,title=hello;docid=3,pk=d,title=hello"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:new", "docid=2,pk=c,title=new"));

    // update building
    docStr = "cmd=add,modify_fields=price#title,modify_values=200#hello,price=100,title=new,pk=d,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c;docid=3,pk=d"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a,title=hello;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:new", "docid=2,pk=c,title=new;docid=3,pk=d,title=new"));
}

void DynamicIndexTest::TestUpdateAsyncDumpingSegment()
{
    std::string field = "price:int64;title:string;pk:string";
    std::string index = "price:number:price;title:string:title;pk:primarykey64:pk";
    std::string attribute = "pk;price;title";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    schema->GetIndexSchema()->GetIndexConfig("title")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("title")->SetOptionFlag(0);
    schema->GetAttributeSchema()->GetAttributeConfig("title")->SetUpdatableMultiValue(true);
    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.GetOnlineConfig().buildConfig.maxDocCount = 2;
    _options.GetOnlineConfig().enableAsyncDumpSegment = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));

    std::string addDoc = "cmd=add,price=1,title=t1,pk=a;";
    std::string updateDoc1 = "cmd=add,modify_fields=price#title,modify_values=1#t1,price=2,title=t2,pk=a;";
    std::string updateDoc2 = "cmd=add,modify_fields=price#title,modify_values=2#t2,price=3,title=t3,pk=a;";
    std::string updateDoc3 = "cmd=add,modify_fields=price#title,modify_values=3#t3,price=4,title=t2,pk=a;";

    // update building
    ASSERT_TRUE(psm.Transfer(BUILD_RT, addDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, updateDoc1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, updateDoc2, "", ""));

    // trigger dump
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,price=999,title=xxx,pk=y;", "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, "cmd=add,price=999,title=xxx,pk=z;", "", ""));
    auto onlinePartition = std::dynamic_pointer_cast<partition::OnlinePartition>(psm.GetIndexPartition());
    onlinePartition->ExecuteTask(partition::OnlinePartitionTaskItem::TT_TRIGGER_ASYNC_DUMP);

    // update dumping
    ASSERT_TRUE(psm.Transfer(BUILD_RT, updateDoc3, "", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "price:999", "pk=y;pk=z"));

    EXPECT_TRUE(psm.Transfer(QUERY, "", "price:1", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "price:2", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "price:3", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "price:4", "docid=0,pk=a"));

    EXPECT_TRUE(psm.Transfer(QUERY, "", "title:t1", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "title:t2", "docid=0,pk=a"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "title:t3", ""));
}

void DynamicIndexTest::TestOffline() { InnerTestOffline(false, false); }
void DynamicIndexTest::TestOfflineSortBiuld() { InnerTestOffline(true, false); }
void DynamicIndexTest::InnerTestOffline(bool sortBuild, bool multiSharding)
{
    if (sortBuild) {
        index_base::PartitionMeta partitionMeta;
        partitionMeta.AddSortDescription("sort", indexlibv2::config::sp_asc);
        partitionMeta.Store(GET_PARTITION_DIRECTORY());
    }
    std::string field = "price:int64;title:string;pk:string;sort:int64";
    std::string index = "price:number:price;title:string:title;pk:primarykey64:pk";
    if (multiSharding) {
        index = "price:number:price:false:2;title:string:title;pk:primarykey64:pk";
    }
    std::string attribute = "pk;price;title;sort";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    schema->GetIndexSchema()->GetIndexConfig("title")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("title")->SetOptionFlag(0);
    schema->GetAttributeSchema()->GetAttributeConfig("title")->SetUpdatableMultiValue(true);

    field = "subprice:int64;subpk:string";
    index = "subprice:number:subprice;subpk:primarykey64:subpk";
    attribute = "subpk;subprice";

    auto subSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->SetSubIndexPartitionSchema(subSchema);

    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,price=100,title=hello,sort=0,pk=a,subpk=a,subprice=1,ts=1;"
                         "cmd=add,price=100,title=world,sort=1,pk=b,subpk=b,subprice=1,ts=1;"
                         "cmd=add,price=201,title=hello,sort=2,pk=c,subpk=c,subprice=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a;docid=2,pk=c"));
    docStr = "cmd=add,price=201,title=hello,sort=3,pk=d,subpk=d,subprice=1,ts=2;"
             "cmd=add,price=201,title=world,sort=4,pk=e,subpk=e,subprice=1,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", "docid=2,pk=c;docid=3,pk=d;docid=4,pk=e"));

    // rt update inc
    docStr = "cmd=add,modify_fields=price#title,modify_values=201#hello,price=100,title=new,sort=2,pk=c,subpk=c,"
             "subprice=1,ts=2;";
    // cmd=add,__last_value__price=201,price=100,pk=c,ts=2;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", "docid=3,pk=d;docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a,title=hello;docid=3,pk=d,title=hello"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:new", "docid=2,pk=c,title=new"));

    // offline update inc
    docStr = "cmd=add,modify_fields=price#title,modify_values=201#hello,price=100,title=new,sort=3,pk=d,subpk=d,"
             "subprice=1,ts=2;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c;docid=3,pk=d"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", "docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:hello", "docid=0,pk=a,title=hello;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:new", "docid=2,pk=c,title=new;docid=3,pk=d,title=new"));

    // update new term
    docStr =
        "cmd=add,price=300,title=toyota,sort=5,pk=f,subpk=f,subprice=1,ts=2;"
        "cmd=add,modify_fields=title,modify_values=toyota,price=300,title=honda,sort=5,pk=f,subpk=f,subprice=1,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:toyota", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "title:honda", "docid=5,pk=f;"));
}

void DynamicIndexTest::TestMultiSharding() { InnerTestOffline(false, true); }

// void DynamicIndexTest::TestOffline2()
// {
//     std::string field = "price:int64;pk:string";
//     std::string index = "price:number:price;pk:primarykey64:pk";
//     std::string attribute = "pk;price";
//     auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
//     schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
//     schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
//     std::string rootDir = GET_TEMP_DATA_PATH();
//     _options.SetEnablePackageFile(false);

//     PartitionStateMachine psm;
//     ASSERT_TRUE(psm.Init(schema, _options, rootDir));
//     std::string docStr = "cmd=add,price=100,pk=a,ts=1;"
//                          "cmd=add,price=100,pk=b,ts=1;"
//                          "cmd=add,price=200,pk=c,ts=1;"
//                          "cmd=add,price=200,pk=d,ts=1;"
//                          "cmd=add,price=300,pk=e,ts=1;";
//     ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
//     ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));

//     // offline update inc
//     docStr = "cmd=add,modify_fields=price,modify_values=200,price=100,pk=c,ts=2;";
//     ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
//     EXPECT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c"));
//     EXPECT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=3,pk=d"));
//     EXPECT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=4,pk=e"));
// }

void DynamicIndexTest::TestBitmap() { InnerTestBitmap(false); }
void DynamicIndexTest::TestBitmapSortBuild() { InnerTestBitmap(true); }

void DynamicIndexTest::InnerTestBitmap(bool sortBuild)
{
    if (sortBuild) {
        index_base::PartitionMeta partitionMeta;
        partitionMeta.AddSortDescription("sort", indexlibv2::config::sp_asc);
        partitionMeta.Store(GET_PARTITION_DIRECTORY());
    }
    std::string field = "price:int64;pk:string;sort:int64";
    std::string index = "price:number:price;pk:primarykey64:pk";
    std::string attribute = "pk;price;sort";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    // Enable bitmap
    std::shared_ptr<config::DictionaryConfig> dictConfig = schema->AddDictionaryConfig("dict", "200;300");
    schema->GetIndexSchema()->GetIndexConfig("price")->SetDictConfig(dictConfig);

    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,sort=1,price=100,pk=a,ts=1;"
                         "cmd=add,sort=1,price=100,pk=b,ts=1;"
                         "cmd=add,sort=1,price=200,pk=c,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));
    docStr = "cmd=add,sort=1,price=200,pk=d,ts=2;"
             "cmd=add,sort=1,price=200,pk=e,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=2,pk=c;docid=3,pk=d;docid=4,pk=e"));

    // rt
    docStr = "cmd=add,sort=1,price=100,pk=f,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=5,pk=f"));

    // rt update rt
    docStr = "cmd=add,sort=1,modify_fields=price,modify_values=100,price=300,pk=f,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=5,pk=f;"));

    // rt update inc
    docStr = "cmd=add,sort=1,modify_fields=price,modify_values=200,price=100,pk=c,ts=2;";
    // cmd=add,sort=1,__last_value__price=200,price=100,pk=c,ts=2;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=3,pk=d;docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c"));

    // offline update inc
    docStr = "cmd=add,sort=1,modify_fields=price,modify_values=200,price=100,pk=d,ts=2;"
             "cmd=add,sort=1,price=300,pk=g,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=5,pk=g;docid=6,pk=f"));
    docStr = "cmd=add,sort=1,modify_fields=price,modify_values=300,price=400,pk=g,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=2,pk=c;docid=3,pk=d"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:200", "docid=4,pk=e"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=6,pk=f"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:400", "docid=5,pk=g"));

    // update new term
    docStr = "cmd=add,sort=1,modify_fields=price,modify_values=400,price=300,title=nissan,pk=g,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=5,pk=g;docid=6,pk=f"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:400", ""));
}

void DynamicIndexTest::TestNullTerm()
{
    std::string field = "price:int64;title:string;pk:string";
    std::string index = "price:number:price;title:string:title;pk:primarykey64:pk";
    std::string attribute = "pk;price;title";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    schema->GetFieldConfig("price")->SetEnableNullField(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);

    std::shared_ptr<config::DictionaryConfig> dictConfig = schema->AddDictionaryConfig("dict", "__NULL__");
    schema->GetIndexSchema()->GetIndexConfig("price")->SetDictConfig(dictConfig);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetHighFreqencyTermPostingType(hp_both);

    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,price=100,pk=a,ts=1;"       // 0
                         "cmd=add,price=100,pk=b,ts=1;"       // 1
                         "cmd=add,price=__NULL__,pk=c,ts=1;"; // 2
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:__NULL__", "docid=2,pk=c;"));

    docStr = "cmd=add,price=100,pk=d,ts=2;"; // 3

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b;docid=3,pk=d;"));

    // rt update inc
    docStr = "cmd=add,modify_fields=price,modify_values=100,price=__NULL__,pk=a,ts=2;"; // update 0 -> null
    // cmd=add,__last_value__price=200,price=100,pk=c,ts=2;
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=1,pk=b;docid=3,pk=d"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:__NULL__", "docid=0,pk=a;docid=2,pk=c;"));

    // offline update inc
    docStr = "cmd=add,modify_fields=price,modify_values=__NULL__,price=100,pk=c,ts=2;"; // update 2 -> 100
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=1,pk=b;docid=2,pk=c;docid=3,pk=d"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:__NULL__", "docid=0,pk=a"));

    // update new term
    docStr = "cmd=add,modify_fields=price,modify_values=100,price=__NULL__,pk=c,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:__NULL__", "docid=0,pk=a;docid=2,pk=c"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=1,pk=b;docid=3,pk=d"));
}

void DynamicIndexTest::TestSubDocs() { DoTestSubDocs(false); }

void DynamicIndexTest::TestSubDocsMultiSharding() { DoTestSubDocs(true); }

void DynamicIndexTest::DoTestSubDocs(bool multiSharding)
{
    std::string field = "price:int64;pk:int64";
    std::string index;
    if (multiSharding) {
        index = "price:number:price:false:2;pk:primarykey64:pk";
    } else {
        index = "price:number:price;pk:primarykey64:pk";
    }

    std::string attribute = "pk;price";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);

    field = "subprice:int64;subpk:int64";
    index = "subprice:number:subprice;subpk:primarykey64:subpk";

    attribute = "subpk;subprice";
    auto subSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    subSchema->GetIndexSchema()->GetIndexConfig("subprice")->TEST_SetIndexUpdatable(true);
    subSchema->GetIndexSchema()->GetIndexConfig("subprice")->SetOptionFlag(0);
    schema->SetSubIndexPartitionSchema(subSchema);

    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, rootDir));
        std::string docStr = "cmd=add,price=10,pk=1000,subprice=100^101,subpk=2000^2001,ts=1;"
                             "cmd=add,price=20,pk=1001,subprice=200^201,subpk=2010^2011,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "price:10", "docid=0,pk=1000;"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:100", "docid=0,subpk=2000;"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:101", "docid=1,subpk=2001;"));

        // rt build
        docStr = "cmd=add,price=30,pk=1002,subprice=200^301,subpk=2020^2021,ts=1;"
                 "cmd=add,price=40,pk=1003,subprice=400^401,subpk=2030^2031,ts=1;";

        ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:200", "docid=2,subpk=2010;docid=4,subpk=2020"));

        // rt update rt
        docStr =
            "cmd=add,modify_fields=subprice,sub_modify_fields=subprice^,modify_values=200^301,sub_modify_values=200^,"
            "price=30,pk=1002,subprice=201^301,subpk=2020^2021,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:200", "docid=2,subpk=2010"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:201", "docid=3,subpk=2011;docid=4,subpk=2020"));

        // rt update inc
        docStr =
            "cmd=add,modify_fields=subprice,modify_values=100^101,sub_modify_fields=^subprice,sub_modify_values=^101,"
            "price=10,pk=1000,subprice=100^109,subpk=2000^2001,ts=1;"
            "cmd=add,modify_fields=subprice,modify_values=200^201,sub_modify_fields=^subprice,sub_modify_values=^201,"
            "price=20,pk=1001,subprice=200^209,subpk=2010^2011,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:101", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:109", "docid=1,subpk=2001"));

        // inc update inc, load patch
        docStr =
            "cmd=add,modify_fields=subprice,modify_values=100^101,sub_modify_fields=subprice^,sub_modify_values=100^,"
            "price=10,pk=1000,subprice=102^101,subpk=2000^2001,ts=1;";
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:100", "docid=0,subpk=2000;"));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:100", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:102", "docid=0,subpk=2000"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:209", "docid=3,subpk=2011"));

        // merge
        docStr =
            "cmd=add,modify_fields=subprice,modify_values=102^101,sub_modify_fields=subprice^,sub_modify_values=102^,"
            "price=10,pk=1000,subprice=104^101,subpk=2000^2001,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:104", "docid=0,subpk=2000"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:102", ""));
    }
    {
        // multi thread patch
        _options.GetOnlineConfig().loadPatchThreadNum = 2;
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, rootDir));
        std::string docStr =
            "cmd=add,modify_fields=subprice,modify_values=104^101,sub_modify_fields=subprice^,sub_modify_values=104^,"
            "price=10,pk=1000,subprice=105^101,subpk=2000^2001,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:104", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "subprice:105", "docid=0,subpk=2000"));
    }
}
void DynamicIndexTest::TestMergePatches() { InnerTestMergePatches(false); }
void DynamicIndexTest::TestMergePatchesMultiSharding() { InnerTestMergePatches(true); }

void DynamicIndexTest::InnerTestMergePatches(bool multiSharding)
{
    std::string field = "price:int64;pk:string";
    std::string index = "price:number:price;pk:primarykey64:pk";
    if (multiSharding) {
        index = "price:number:price:false:2;pk:primarykey64:pk";
    }
    std::string attribute = "pk;price";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,price=100,pk=a,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;"));

    // 100:-0,200:+0
    docStr = "cmd=add,modify_fields=price,modify_values=100,price=201,pk=a,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", "docid=0,pk=a;"));

    // 200:-0,300:+0
    docStr = "cmd=add,modify_fields=price,modify_values=201,price=300,pk=a,ts=3;"
             "cmd=add,price=400,pk=b,ts=3;"
             "cmd=add,modify_fields=price,modify_values=400,price=300,pk=b,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=0,pk=a;docid=1,pk=b"));

    config::MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=2,3;");
    psm.SetMergeConfig(mergeConfig);

    // merged: 100:-0,200:-0,300:+0
    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=0,pk=a;docid=1,pk=b"));

    mergeConfig = config::MergeConfig();
    mergeConfig.mergeStrategyStr = "optimize";
    psm.SetMergeConfig(mergeConfig);

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:201", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "price:300", "docid=0,pk=a;docid=1,pk=b"));
}

void DynamicIndexTest::TestUpdateWithDisabledField()
{
    std::string field = "pk:string;price:int64;disabled:int64;deleted:int64";
    std::string index = "price:number:price;pk:primarykey64:pk;disabled:number:disabled;deleted:number:deleted";
    std::string attribute = "pk;price";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("price")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("price")->SetOptionFlag(0);
    schema->GetIndexSchema()->GetIndexConfig("disabled")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("disabled")->SetOptionFlag(0);
    schema->GetIndexSchema()->GetIndexConfig("deleted")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("deleted")->SetOptionFlag(0);
    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, rootDir));
        std::string docStr = "cmd=add,price=100,pk=a,disabled=0,deleted=0,ts=1;"
                             "cmd=add,price=100,pk=b,disabled=0,deleted=0,ts=1;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=0,pk=a;docid=1,pk=b"));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "disabled:0", "docid=0,pk=a;docid=1,pk=b"));
        docStr = "cmd=add,pk=a,modify_fields=price#disabled#deleted,modify_values=100#0#0,price=200,disabled=1,deleted="
                 "1,ts=2;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "disabled:1", "docid=0,pk=a;"));
    }

    _options.GetOnlineConfig().GetDisableFieldsConfig().indexes = {"disabled"};

    {
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(schema, _options, rootDir));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "price:100", "docid=1,pk=b;"));
        std::string docStr = "cmd=add,price=300,pk=c,disabled=0,deleted=0,ts=3;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));

        // rt update & build
        docStr =
            "cmd=add,pk=c,modify_fields=price#disabled#deleted,modify_values=300#0#0,price=3000,disabled=1,deleted="
            "1,ts=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docStr, "", ""));

        // rt update inc
        docStr =
            "cmd=add,pk=b,modify_fields=price#disabled#deleted,modify_values=100#0#0,price=10000,disabled=1,deleted="
            "1,ts=4;";
        ASSERT_TRUE(psm.Transfer(BUILD_RT, docStr, "", ""));
    }
}

void DynamicIndexTest::TestAdaptiveBitmap()
{
    std::string field = "is_coin:int32;pk:string;";
    std::string index = "is_coin:number:is_coin;pk:primarykey64:pk";
    std::string attribute = "pk;is_coin";
    auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    auto indexSchema = schema->GetIndexSchema();
    auto indexConfig = indexSchema->GetIndexConfig("is_coin");
    auto dictConfig = std::make_shared<indexlib::config::AdaptiveDictionaryConfig>("adaptive_rule", "DOC_FREQUENCY", 2);
    auto adaptiveDictSchema = std::make_shared<indexlib::config::AdaptiveDictionarySchema>();
    adaptiveDictSchema->AddAdaptiveDictionaryConfig(dictConfig);
    schema->SetAdaptiveDictSchema(adaptiveDictSchema);
    indexConfig->SetAdaptiveDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_bitmap);

    indexConfig->TEST_SetIndexUpdatable(true);
    indexConfig->SetOptionFlag(0);

    std::string rootDir = GET_TEMP_DATA_PATH();
    _options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, _options, rootDir));
    std::string docStr = "cmd=add,is_coin=1,pk=a,ts=1;"  // 0
                         "cmd=add,is_coin=1,pk=b,ts=1;"  // 1
                         "cmd=add,is_coin=1,pk=c,ts=1;"  // 2
                         "cmd=add,is_coin=0,pk=d,ts=1;"; // 3
    // is_coin: 1 --> highFreq
    // is_coin: 0 --> normal
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docStr, "", ""));

    auto checkIsCoin = [&](int64_t term, const std::string& postingType, const std::vector<docid_t>& expectedDocIds) {
        std::string queryString = "is_coin:" + std::to_string(term) + "$" + postingType;
        std::string expectedString;
        for (auto docId : expectedDocIds) {
            expectedString += "docid=" + std::to_string(docId) + ";";
        }
        ASSERT_TRUE(psm.Transfer(QUERY, "", queryString, expectedString)) << queryString << " " << expectedString;
    };

    checkIsCoin(1, "pt_normal", {});
    checkIsCoin(1, "pt_bitmap", {0, 1, 2});
    checkIsCoin(1, "pt_default", {0, 1, 2});
    checkIsCoin(0, "pt_normal", {3});
    checkIsCoin(0, "pt_bitmap", {});
    checkIsCoin(0, "pt_default", {3});

    docStr = "cmd=add,is_coin=1,pk=d,modify_fields=is_coin,modify_values=0,ts=2000000;" // 3
             "cmd=add,is_coin=0,pk=a,modify_fields=is_coin,modify_values=1,ts=2000000;" // 0
             "cmd=add,is_coin=1,pk=e,ts=2000000;"                                       // 4
             "cmd=add,is_coin=0,pk=f,ts=2000000;";                                      // 5

    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docStr, "", ""));
    checkIsCoin(1, "pt_normal", {});
    checkIsCoin(1, "pt_bitmap", {1, 2, 3, 4});
    checkIsCoin(1, "pt_default", {1, 2, 3, 4});
    checkIsCoin(0, "pt_normal", {0, 5});
    checkIsCoin(0, "pt_bitmap", {});
    checkIsCoin(0, "pt_default", {0, 5});

    docStr = "cmd=add,is_coin=1,pk=d,modify_fields=is_coin,modify_values=0,ts=3000000;" // 3
             "cmd=add,is_coin=0,pk=a,modify_fields=is_coin,modify_values=1,ts=3000000;" // 0
             "cmd=add,is_coin=1,pk=e,ts=3000000;"                                       // 4
             "cmd=add,is_coin=0,pk=f,ts=3000000;";                                      // 5
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docStr, "", ""));                               // inc coverts all rt
    checkIsCoin(1, "pt_normal", {});
    checkIsCoin(1, "pt_bitmap", {1, 2, 3, 4});
    checkIsCoin(1, "pt_default", {1, 2, 3, 4});
    checkIsCoin(0, "pt_normal", {0, 5});
    checkIsCoin(0, "pt_bitmap", {});
    checkIsCoin(0, "pt_default", {0, 5});

    docStr = "cmd=add,is_coin=1,pk=f,modify_fields=is_coin,modify_values=0,ts=4000000;"; // 5

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docStr, "", "")); // inc coverts all rt
    checkIsCoin(1, "pt_normal", {});
    checkIsCoin(1, "pt_bitmap", {1, 2, 3, 4, 5});
    checkIsCoin(1, "pt_default", {1, 2, 3, 4, 5});
    checkIsCoin(0, "pt_normal", {0});
    checkIsCoin(0, "pt_bitmap", {});
    checkIsCoin(0, "pt_default", {0});
}

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestRealtime);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestUpdateAsyncDumpingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestOffline);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestOfflineSortBiuld);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestOffline2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestBitmap);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestBitmapSortBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestNullTerm);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestSubDocs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestSubDocsMultiSharding);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestMergePatches);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestMultiSharding);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestMergePatchesMultiSharding);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestUpdateWithDisabledField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(DynamicIndexTest, TestAdaptiveBitmap);

INSTANTIATE_TEST_CASE_P(BuildMode, DynamicIndexTest, testing::Values(0, 1, 2));
}} // namespace indexlib::index
