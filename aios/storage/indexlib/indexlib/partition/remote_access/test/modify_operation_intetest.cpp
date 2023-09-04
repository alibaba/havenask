#include "indexlib/partition/remote_access/test/modify_operation_intetest.h"

#include <cmath>
#include <iostream>
#include <type_traits>

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/module_info.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/index_roll_back_util.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/modify_schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/PathUtil.h"
using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::test;
using namespace indexlib::testlib;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ModifyOperationInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(ModifyOperationInteTestMode, ModifyOperationInteTest,
                                     testing::Values(true, false));

INDEXLIB_UNIT_TEST_CASE(ModifyOperationInteTest, TestBuildWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE(ModifyOperationInteTest, TestIncParallelBuildWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestMultiRoundDeleteAndUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestOngoingOpWithCustomizeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestMultiRoundDeleteAndUpdateForFloatCompress);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestModifyOperationSupportNullField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestModifyOperationForTimeAttribute);

ModifyOperationInteTest::ModifyOperationInteTest() {}

ModifyOperationInteTest::~ModifyOperationInteTest() {}

void ModifyOperationInteTest::CaseSetUp() { mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024)); }

void ModifyOperationInteTest::CaseTearDown() {}

void ModifyOperationInteTest::TestBuildWithModifyOperation()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    IndexPartitionSchemaPtr oldSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=tags;indexs=categoryIndex",
                                           "new_int:uint64;new_string:string", "newIntIndex:number:new_int",
                                           "new_int;new_string");

    IndexPartitionSchemaPtr currentSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=category", "new_multi_int:uint64:true:true",
                                           "newMultiIntIndex:number:new_multi_int", "new_multi_int");
    IndexPartitionSchemaPtr newSchema(schema->Clone());

    string docStr = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;";
    NormalDocumentPtr oldDoc = DocumentCreator::CreateNormalDocument(oldSchema, docStr);
    ASSERT_EQ(0, oldDoc->GetSchemaVersionId());
    docStr = "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,new_int=10,new_string=abc,ts=2;";
    NormalDocumentPtr curDoc = DocumentCreator::CreateNormalDocument(currentSchema, docStr);
    ASSERT_EQ(1, curDoc->GetSchemaVersionId());
    docStr = "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,new_int=20,new_multi_int=20 30,ts=3;";
    NormalDocumentPtr newDoc = DocumentCreator::CreateNormalDocument(newSchema, docStr);
    ASSERT_EQ(2, newDoc->GetSchemaVersionId());

    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetOnlineConfig().onlineKeepVersionCount = 10;
    options.SetOngoingModifyOperationIds({1, 2});
    IndexBuilder indexBuilder(GET_TEMP_DATA_PATH(), options, currentSchema, mQuotaControl,
                              BuilderBranchHinter::Option::Test());
    ASSERT_TRUE(indexBuilder.Init());
    indexBuilder.Build(oldDoc);
    indexBuilder.Build(curDoc);
    indexBuilder.Build(newDoc);
    indexBuilder.EndIndex(10);
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 1, {1});

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(newSchema, options, GET_TEMP_DATA_PATH()));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,category=1 2"));
    // opId 1 is not ready
    EXPECT_FALSE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_string=,new_int=0"));

    psm.SetSchema(newSchema);
    docStr = "cmd=add,name=doc3,price=40,category=3 4,"
             "tags=test3 test,new_int=20,new_multi_int=20 30,ts=13;";
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docStr, "", ""));
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 2, {1, 2});

    // schema id change
    ASSERT_FALSE(psm.Transfer(PE_REOPEN, "", "", ""));

    options.SetOngoingModifyOperationIds({2});
    PartitionStateMachine newSchemaPsm;
    ASSERT_TRUE(newSchemaPsm.Init(newSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));

    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 2, {2});
    EXPECT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_string=,new_int=0"));
    EXPECT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc2", "docid=1,new_string=abc,new_int=10"));
    EXPECT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc3", "docid=3,price=40,new_int=20"));

    newSchemaPsm.SetOngoingModifyOperations({});
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 2, {});
    // ongoing opIds change
    ASSERT_FALSE(psm.Transfer(PE_REOPEN, "", "", ""));
}

void ModifyOperationInteTest::TestIncParallelBuildWithModifyOperation()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    IndexPartitionSchemaPtr oldSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=tags;indexs=categoryIndex",
                                           "new_int:uint64;new_string:string", "newIntIndex:number:new_int",
                                           "new_int;new_string");
    IndexPartitionSchemaPtr currentSchema(schema->Clone());

    string docStr = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;";
    NormalDocumentPtr oldDoc = DocumentCreator::CreateNormalDocument(oldSchema, docStr);
    ASSERT_EQ(0, oldDoc->GetSchemaVersionId());
    docStr = "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,new_int=10,new_string=abc,ts=2;";
    NormalDocumentPtr curDoc = DocumentCreator::CreateNormalDocument(currentSchema, docStr);
    ASSERT_EQ(1, curDoc->GetSchemaVersionId());

    IndexPartitionOptions options;
    options.SetOngoingModifyOperationIds({1});
    IndexBuilder indexBuilder(GET_TEMP_DATA_PATH(), options, currentSchema, mQuotaControl,
                              BuilderBranchHinter::Option::Test());
    ASSERT_TRUE(indexBuilder.Init());
    indexBuilder.Build(oldDoc);
    indexBuilder.Build(curDoc);
    indexBuilder.EndIndex(10);
    indexBuilder.TEST_BranchFSMoveToMainRoad();
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 1, {1});

    options.SetOngoingModifyOperationIds({});

    ParallelBuildInfo info;
    info.parallelNum = 2;
    options.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    for (size_t i = 0; i < info.parallelNum; i++) {
        info.instanceId = i;
        string incDoc =
            "cmd=add,name=doc" + std::to_string(i + 5) + ",price=40,category=3 4,tags=test3 test,new_int=20,ts=13;";
        IndexlibPartitionCreator::BuildParallelIncData(currentSchema, GET_TEMP_DATA_PATH(), info, incDoc, options);
    }
    merger::IndexPartitionMergerPtr merger(
        (merger::IndexPartitionMerger*)merger::PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
            GET_TEMP_DATA_PATH(), info, options, NULL, ""));
    merger->PrepareMerge(0);
    merger::MergeMetaPtr meta = merger->CreateMergeMeta(false, 1, 0);
    merger->DoMerge(false, meta, 0);
    merger->EndMerge(meta, meta->GetTargetVersion().GetVersionId());

    PartitionStateMachine newSchemaPsm;
    ASSERT_TRUE(newSchemaPsm.Init(currentSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_string=,new_int=0"));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc2", "docid=1,new_string=abc,new_int=10"));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc5", "docid=2,price=40,new_int=20"));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc6", "docid=3,price=40,new_int=20"));
}

void ModifyOperationInteTest::TestMultiRoundDeleteAndUpdateForFloatCompress()
{
    string field = "name:string:false:false:uniq;price:float:true:true:block_fp:4;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4;value:int32";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload;value";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10 11 12 13,category=1 2,value=1,payload=1 2 3 "
                  "4,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20 21 22 23,category=2 3,value=2,payload=2 3 4 "
                  "5,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30 31 32 33,category=3 4,value=3,payload=3 4 5 "
                  "6,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40 41 42 43,category=4 5,value=4,payload=4 5 6 "
                  "7,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11 12 13 14,value=5,ts=0;";

    IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH(), docs, options, "", false);
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price,value,tags", "", "", "price;value;tags");
    IndexPartitionSchemaPtr newSchema(schema->Clone());
    uint32_t schemaId = newSchema->GetSchemaVersionId();

    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(GET_TEMP_DATA_PATH(), options, "");
    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    DirectoryPtr patchDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        FillOneAttributeForSegment(patcher, iter, "price", segData.GetSegmentId());
        FillOneAttributeForSegment(patcher, iter, "value", segData.GetSegmentId());
        FillOneAttributeForSegment(patcher, iter, "tags", segData.GetSegmentId());
        segIterator->MoveToNext();
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(newSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "price=11 12 13 14");
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "value=5");
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc2", "tags=test2 test");
}

void ModifyOperationInteTest::TestMultiRoundDeleteAndUpdate()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11,ts=0;"
                  "cmd=update_field,name=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH(), docs, options, "", false);

    // delete price, add price, idx = 1
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", "price");
    IndexPartitionSchemaPtr schema1(schema->Clone());
    PreparePatchIndex(schema1, "price", 1);
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema1, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "price=12");

    string incDoc = "cmd=update_field,name=doc1,price=12,ts=0;"
                    "cmd=update_field,name=doc2,price=21,ts=0;";
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, incDoc, "", ""));
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "price=12");
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc2", "price=21");

    // delete price, add price, idx = 2
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", "price");
    IndexPartitionSchemaPtr schema2(schema->Clone());
    PreparePatchIndex(schema2, "price", 2);

    PartitionStateMachine psm2;
    ASSERT_TRUE(psm2.Init(schema2, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm2.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "price=14");

    incDoc = "cmd=update_field,name=doc1,price=13,ts=1;";
    ASSERT_TRUE(psm2.Transfer(PE_BUILD_INC, incDoc, "", ""));
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "price=13");
}

void ModifyOperationInteTest::TestOngoingOpWithCustomizeIndex()
{
    string pluginPath = GET_PRIVATE_TEST_DATA_PATH() + "/demo_indexer_plugins/";
    IndexPartitionSchemaPtr schema = SchemaAdapter::TEST_LoadSchema(
        PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "modify_customized_schema_base.json"));
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,pk=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                  "cmd=add,pk=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                  "cmd=add,pk=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                  "cmd=add,pk=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                  "cmd=update_field,pk=doc1,price=11,ts=0;"
                  "cmd=update_field,pk=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH(), docs, options, "", false);
    IndexPartitionSchemaPtr newSchema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "modify_customized_index_schema.json");
    uint32_t schemaId = newSchema->GetSchemaVersionId();
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(GET_TEMP_DATA_PATH(), options, pluginPath);
    DirectoryPtr patchDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    fieldid_t fieldId1 = newSchema->GetFieldId("cfield1");
    fieldid_t fieldId2 = newSchema->GetFieldId("cfield2");
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        // attribute
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("test_customize", segData.GetSegmentId());
        if (indexPatcher) {
            for (uint32_t i = 0; i < segData.GetSegmentInfo()->docCount; i++) {
                indexPatcher->AddField("test1", fieldId1);
                indexPatcher->AddField("test2", fieldId2);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }
        segIterator->MoveToNext();
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
    options.SetOngoingModifyOperationIds({1});

    IndexPartitionResource resource;
    resource.indexPluginPath = pluginPath;
    PartitionStateMachine newSchemaPsm(true, resource);
    ASSERT_TRUE(newSchemaPsm.Init(newSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 1, {1});

    newSchemaPsm.SetOngoingModifyOperations({});
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    RESET_FILE_SYSTEM();
    CheckLatestVersion(GET_PARTITION_DIRECTORY(), 1, {});
    CheckQuery(GET_PARTITION_DIRECTORY(), "test_customize:test1", "docid=0;docid=1;docid=2;docid=3", pluginPath);
}

void ModifyOperationInteTest::TestModifyOperationSupportNullField()
{
    InnerTestModifyOperationSupportNullField("int8:false", "number");
    InnerTestModifyOperationSupportNullField("int8:true", "number");

    InnerTestModifyOperationSupportNullField("uint32:false", "number");
    InnerTestModifyOperationSupportNullField("uint32:true", "number");

    InnerTestModifyOperationSupportNullField("float:false", "");
    InnerTestModifyOperationSupportNullField("float:true", "");

    InnerTestModifyOperationSupportNullField("double:false", "");
    InnerTestModifyOperationSupportNullField("double:true", "");

    InnerTestModifyOperationSupportNullField("string:false", "string");
    InnerTestModifyOperationSupportNullField("string:true", "string");
}

void ModifyOperationInteTest::TestModifyOperationForTimeAttribute()
{
    InnerTestModifyOperationForTimeAttribute("date", "date");
    InnerTestModifyOperationForTimeAttribute("timestamp", "date");
    InnerTestModifyOperationForTimeAttribute("time", "date");
}

void ModifyOperationInteTest::InnerTestModifyOperationSupportNullField(const string& fieldInfo, const string& indexType)
{
    tearDown();
    setUp();

    bool needCheckIndex = !indexType.empty();
    string field = "name:string;value:" + fieldInfo + ":true";
    string index = "pk:primarykey64:name";
    string attributes = "value";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attributes, "");
    SchemaMaker::EnableNullFields(schema, "value");

    string fullDocs = "cmd=add,name=doc1,value=1;"
                      "cmd=add,name=doc2;"
                      "cmd=add,name=doc3,value=3;";
    string incDocs = "cmd=update_field,name=doc3,value=__NULL__";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    string modifyFieldInfo = "new_value:" + fieldInfo + ":true";
    string modifyIndexInfo = needCheckIndex ? "new_index:" + indexType + ":new_value" : "";
    // schema add new value
    ModifySchemaMaker::AddModifyOperations(schema, "", modifyFieldInfo, modifyIndexInfo, "new_value");
    IndexPartitionSchemaPtr newSchema(schema->Clone());
    SchemaMaker::EnableNullFields(newSchema, "new_value");

    uint32_t schemaId = newSchema->GetSchemaVersionId();

    // do add new attribute & index
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(GET_TEMP_DATA_PATH(), options, "");
    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    DirectoryPtr patchDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    fieldid_t newFieldId = newSchema->GetFieldConfig("new_value")->GetFieldId();
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        if (segData.GetSegmentInfo()->docCount == 0) {
            segIterator->MoveToNext();
            continue;
        }
        AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator("value", segData.GetSegmentId());
        // attribute
        AttributeDataPatcherPtr attrPatcher =
            patcher->CreateSingleAttributePatcher("new_value", segData.GetSegmentId());
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("new_index", segData.GetSegmentId());
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
            assert(attrIter->IsValid());
            bool valueIsNull = attrIter->IsNullValue();
            bool useRawIndex = GET_CASE_PARAM();
            if (useRawIndex) {
                attrPatcher->AppendFieldValueFromRawIndex(attrIter->GetRawIndexImageValue(), valueIsNull);
            } else {
                attrPatcher->AppendFieldValue(attrIter->GetValueStr());
            }

            if (indexPatcher) {
                vector<string> tokens = StringUtil::split(attrIter->GetValueStr(), "");
                for (auto& t : tokens) {
                    indexPatcher->AddField(t, newFieldId);
                }
                indexPatcher->EndDocument();
            }
            attrIter->MoveToNext();
        }
        attrPatcher->Close();
        if (indexPatcher) {
            indexPatcher->Close();
        }
        segIterator->MoveToNext();
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
    resourceProvider->StoreVersion(newSchema, resourceProvider->GetVersion().GetVersionId() + 1);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();

    // check new attribute & index
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "new_value=1");
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc2", "new_value=__NULL__");
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc3", "new_value=__NULL__");

    if (needCheckIndex) {
        CheckQuery(GET_PARTITION_DIRECTORY(), "new_index:1", "docid=0");
        CheckQuery(GET_PARTITION_DIRECTORY(), "new_index:__NULL__", "docid=1;docid=2");
    }
}

void ModifyOperationInteTest::InnerTestModifyOperationForTimeAttribute(const string& fieldInfo, const string& indexType)
{
    tearDown();
    setUp();

    string valueStr = "2010-01-01 00:00:00.000";
    if (fieldInfo == "time") {
        valueStr = "00:00:00.000";
    }
    if (fieldInfo == "date") {
        valueStr = "2010-01-01";
    }
    bool needCheckIndex = !indexType.empty();
    string field = "name:string;value:" + fieldInfo;
    string index = "pk:primarykey64:name";
    string attributes = "value";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attributes, "");
    SchemaMaker::EnableNullFields(schema, "value");

    string fullDocs = "cmd=add,name=doc1,value=" + valueStr +
                      ";"
                      "cmd=add,name=doc2;"
                      "cmd=add,name=doc3,value=" +
                      valueStr + ";";
    string incDocs = "cmd=update_field,name=doc3,value=__NULL__";

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    string modifyFieldInfo = "new_value:" + fieldInfo;
    string modifyIndexInfo = needCheckIndex ? "new_index:" + indexType + ":new_value" : "";
    // schema add new value
    ModifySchemaMaker::AddModifyOperations(schema, "", modifyFieldInfo, modifyIndexInfo, "new_value");
    IndexPartitionSchemaPtr newSchema(schema->Clone());
    SchemaMaker::EnableNullFields(newSchema, "new_value");

    uint32_t schemaId = newSchema->GetSchemaVersionId();

    // do add new attribute & index
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(GET_TEMP_DATA_PATH(), options, "");
    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    DirectoryPtr patchDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    fieldid_t newFieldId = newSchema->GetFieldConfig("new_value")->GetFieldId();
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        if (segData.GetSegmentInfo()->docCount == 0) {
            segIterator->MoveToNext();
            continue;
        }
        AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator("value", segData.GetSegmentId());
        // attribute
        AttributeDataPatcherPtr attrPatcher =
            patcher->CreateSingleAttributePatcher("new_value", segData.GetSegmentId());
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher("new_index", segData.GetSegmentId());
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
            assert(attrIter->IsValid());
            bool valueIsNull = attrIter->IsNullValue();
            bool useRawIndex = GET_CASE_PARAM();
            if (useRawIndex) {
                attrPatcher->AppendFieldValueFromRawIndex(attrIter->GetRawIndexImageValue(), valueIsNull);
            } else {
                attrPatcher->AppendFieldValue(attrIter->GetValueStr());
            }

            if (indexPatcher) {
                indexPatcher->AddField(attrIter->GetValueStr(), newFieldId);
                indexPatcher->EndDocument();
            }
            attrIter->MoveToNext();
        }
        attrPatcher->Close();
        if (indexPatcher) {
            indexPatcher->Close();
        }
        segIterator->MoveToNext();
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
    resourceProvider->StoreVersion(newSchema, resourceProvider->GetVersion().GetVersionId() + 1);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();

    // check new attribute & index
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc1", "new_value=" + valueStr);
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc2", "new_value=__NULL__");
    RESET_FILE_SYSTEM();
    CheckQuery(GET_PARTITION_DIRECTORY(), "pk:doc3", "new_value=__NULL__");

    if (needCheckIndex) {
        if (fieldInfo == "time") {
            CheckQuery(GET_PARTITION_DIRECTORY(), "new_index:[0, 80000000)", "docid=0");
        } else {
            CheckQuery(GET_PARTITION_DIRECTORY(), "new_index:[1262302000000, 1262314000000)", "docid=0");
        }
        CheckQuery(GET_PARTITION_DIRECTORY(), "new_index:__NULL__", "docid=1;docid=2");
    }
}

void ModifyOperationInteTest::PreparePatchIndex(const IndexPartitionSchemaPtr& newSchema, const string& attribute,
                                                uint32_t delta)
{
    string rootPath = GET_TEMP_DATA_PATH();
    uint32_t schemaId = newSchema->GetSchemaVersionId();
    DirectoryPtr patchDir =
        GET_PARTITION_DIRECTORY()->MakeDirectory(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));

    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(GET_TEMP_DATA_PATH(), options, "");

    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    PartitionPatcherPtr patcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator = resourceProvider->CreatePartitionSegmentIterator();
    while (segIterator->IsValid()) {
        SegmentData segData = segIterator->GetSegmentData();
        AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator(attribute, segData.GetSegmentId());

        // attribute
        AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(attribute, segData.GetSegmentId());
        if (attrPatcher) {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
                assert(attrIter->IsValid());
                uint32_t value = StringUtil::fromString<uint32_t>(attrIter->GetValueStr());
                attrPatcher->AppendFieldValue(StringUtil::toString(value + delta));
                attrIter->MoveToNext();
            }
            attrPatcher->Close();
        }
        segIterator->MoveToNext();
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
}

void ModifyOperationInteTest::CheckLatestVersion(const file_system::DirectoryPtr& rootDir, schemavid_t expectSchemaId,
                                                 const vector<schema_opid_t>& ongoingOpIds)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    ASSERT_EQ(expectSchemaId, version.GetSchemaVersionId());
    ASSERT_EQ(ongoingOpIds, version.GetOngoingModifyOperations());
}

void ModifyOperationInteTest::CheckQuery(const file_system::DirectoryPtr& rootDir, const string& query,
                                         const string& result, const string& pluginPath)
{
    Version onDiskVersion;
    VersionLoader::GetVersion(rootDir, onDiskVersion, INVALID_VERSION);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchemaForVersion(rootDir, onDiskVersion.GetVersionId());
    ASSERT_NE(schema, nullptr);

    IndexPartitionOptions options;
    IndexPartitionResource resource;
    resource.indexPluginPath = pluginPath;
    PartitionStateMachine psm(true, resource);
    auto rootPath = rootDir->GetOutputPath();
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", query, result)) << query << " " << result;
}

void ModifyOperationInteTest::FillOneAttributeForSegment(const PartitionPatcherPtr& patcher,
                                                         const PartitionIteratorPtr& iter, const string& attrName,
                                                         segmentid_t segId)
{
    bool useRawIndex = GET_CASE_PARAM();
    AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator(attrName, segId);
    AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(attrName, segId);
    if (attrPatcher) {
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++) {
            assert(attrIter->IsValid());
            if (useRawIndex) {
                attrPatcher->AppendFieldValueFromRawIndex(attrIter->GetRawIndexImageValue());
            } else {
                attrPatcher->AppendFieldValue(attrIter->GetValueStr());
            }
            attrIter->MoveToNext();
        }
        attrPatcher->Close();
    }
}
}} // namespace indexlib::partition
