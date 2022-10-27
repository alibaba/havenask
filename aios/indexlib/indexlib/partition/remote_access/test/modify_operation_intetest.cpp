#include "indexlib/partition/remote_access/test/modify_operation_intetest.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/partition/index_roll_back_util.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/util/path_util.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/modify_schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/config/module_info.h"
#include <cmath>
#include <type_traits>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ModifyOperationInteTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(ModifyOperationInteTestMode,
                                     ModifyOperationInteTest,
                                     testing::Values(true, false));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestBuildWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestMultiRoundDeleteAndUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestOngoingOpWithCustomizeIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ModifyOperationInteTestMode, TestMultiRoundDeleteAndUpdateForFloatCompress);

ModifyOperationInteTest::ModifyOperationInteTest()
{
}

ModifyOperationInteTest::~ModifyOperationInteTest()
{
}

void ModifyOperationInteTest::CaseSetUp()
{
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
}

void ModifyOperationInteTest::CaseTearDown()
{
}

void ModifyOperationInteTest::TestBuildWithModifyOperation()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
            "index_table", field, index, attributes, "");

    IndexPartitionSchemaPtr oldSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema,
            "attributes=tags;indexs=categoryIndex", "new_int:uint64;new_string:string",
            "newIntIndex:number:new_int", "new_int;new_string");

    IndexPartitionSchemaPtr currentSchema(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(schema,
            "attributes=category", "new_multi_int:uint64:true:true",
            "newMultiIntIndex:number:new_multi_int", "new_multi_int");
    IndexPartitionSchemaPtr newSchema(schema->Clone());

    string docStr = "cmd=add,name=doc1,price=10,category=1 2,tags=test1 test,ts=0;";
    NormalDocumentPtr oldDoc = DocumentCreator::CreateDocument(oldSchema, docStr);
    ASSERT_EQ(0, oldDoc->GetSchemaVersionId());
    docStr = "cmd=add,name=doc2,price=20,category=2 3,tags=test2 test,new_int=10,new_string=abc,ts=2;";
    NormalDocumentPtr curDoc = DocumentCreator::CreateDocument(currentSchema, docStr);
    ASSERT_EQ(1, curDoc->GetSchemaVersionId());
    docStr = "cmd=add,name=doc3,price=30,category=3 4,tags=test3 test,new_int=20,new_multi_int=20 30,ts=3;";
    NormalDocumentPtr newDoc = DocumentCreator::CreateDocument(newSchema, docStr);
    ASSERT_EQ(2, newDoc->GetSchemaVersionId());

    IndexPartitionOptions options;
    options.GetBuildConfig(false).enablePackageFile = false;
    options.GetBuildConfig(false).keepVersionCount = 10;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetOnlineConfig().onlineKeepVersionCount = 10;
    options.SetOngoingModifyOperationIds({1,2});
    IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, currentSchema, mQuotaControl);
    ASSERT_TRUE(indexBuilder.Init());
    indexBuilder.Build(oldDoc);
    indexBuilder.Build(curDoc);
    indexBuilder.Build(newDoc);
    indexBuilder.EndIndex(10);
    CheckLatestVersion(GET_TEST_DATA_PATH(), 1, {1});

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(newSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,category=1 2"));
    // opId 1 is not ready
    ASSERT_FALSE(psm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_string=,new_int=0"));
        
    psm.SetSchema(newSchema);
    docStr = "cmd=add,name=doc3,price=40,category=3 4,"
             "tags=test3 test,new_int=20,new_multi_int=20 30,ts=13;";
    ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, docStr, "", ""));
    CheckLatestVersion(GET_TEST_DATA_PATH(), 2, {1, 2});
    
    // schema id change
    ASSERT_FALSE(psm.Transfer(PE_REOPEN, "", "", ""));

    options.SetOngoingModifyOperationIds({2});
    PartitionStateMachine newSchemaPsm;
    ASSERT_TRUE(newSchemaPsm.Init(newSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));

    CheckLatestVersion(GET_TEST_DATA_PATH(), 2, {2});    
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc1", "docid=0,new_string=,new_int=0"));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc2", "docid=1,new_string=abc,new_int=10"));
    ASSERT_TRUE(newSchemaPsm.Transfer(QUERY, "", "pk:doc3", "docid=3,price=40,new_int=20"));

    newSchemaPsm.SetOngoingModifyOperations({});
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    CheckLatestVersion(GET_TEST_DATA_PATH(), 2, {});
    // ongoing opIds change
    ASSERT_FALSE(psm.Transfer(PE_REOPEN, "", "", ""));
}

void ModifyOperationInteTest::TestMultiRoundDeleteAndUpdateForFloatCompress()
{
    string field = "name:string:false:false:uniq;price:float:true:true:block_fp:4;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4;value:int32";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload;value";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
            "index_table", field, index, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10 11 12 13,category=1 2,value=1,payload=1 2 3 4,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20 21 22 23,category=2 3,value=2,payload=2 3 4 5,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30 31 32 33,category=3 4,value=3,payload=3 4 5 6,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40 41 42 43,category=4 5,value=4,payload=4 5 6 7,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11 12 13 14,value=5,ts=0;";

    IndexlibPartitionCreator::CreateIndexPartition(
            schema, GET_TEST_DATA_PATH(), docs, options, "", false);
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price,value,tags", "", "", "price;value;tags");
    IndexPartitionSchemaPtr newSchema(schema->Clone());
    uint32_t schemaId = newSchema->GetSchemaVersionId();
    string patchDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
            PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FileSystemWrapper::MkDirIfNotExist(patchDir);

    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
                GET_TEST_DATA_PATH(), options, "");
    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    PartitionPatcherPtr patcher =
        resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator =
        resourceProvider->CreatePartitionSegmentIterator();
    while (segIterator->IsValid())
    {
        SegmentData segData = segIterator->GetSegmentData();
        FillOneAttributeForSegment(patcher, iter, "price", segData.GetSegmentId());
        FillOneAttributeForSegment(patcher, iter, "value", segData.GetSegmentId());
        FillOneAttributeForSegment(patcher, iter, "tags", segData.GetSegmentId());                
        segIterator->MoveToNext();        
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(newSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, "", "", ""));
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "price=11 12 13 14");
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "value=5");
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc2", "tags=test2 test");    
}

void ModifyOperationInteTest::TestMultiRoundDeleteAndUpdate()
{
    string field = "name:string:false:false:uniq;price:uint32;"
                   "category:int32:true:true;tags:string:true;"
                   "payload:float:true:true:fp16:4";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category;tags;payload";
    IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateSchema(
            "index_table", field, index, attributes, "");

    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,name=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                  "cmd=add,name=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                  "cmd=add,name=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                  "cmd=add,name=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                  "cmd=update_field,name=doc1,price=11,ts=0;"
                  "cmd=update_field,name=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(
            schema, GET_TEST_DATA_PATH(), docs, options, "", false);
    
    // delete price, add price, idx = 1
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", "price");
    IndexPartitionSchemaPtr schema1(schema->Clone());
    PreparePatchIndex(schema1, "price", 1);
    PartitionStateMachine psm1;
    ASSERT_TRUE(psm1.Init(schema1, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, "", "", ""));
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "price=12");

    string incDoc = "cmd=update_field,name=doc1,price=12,ts=0;"
                    "cmd=update_field,name=doc2,price=21,ts=0;";
    ASSERT_TRUE(psm1.Transfer(PE_BUILD_INC, incDoc, "", ""));
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "price=12");
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc2", "price=21");
    
    // delete price, add price, idx = 2
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", "price");
    IndexPartitionSchemaPtr schema2(schema->Clone());
    PreparePatchIndex(schema2, "price", 2);
    
    PartitionStateMachine psm2;
    ASSERT_TRUE(psm2.Init(schema2, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm2.Transfer(PE_BUILD_INC, "", "", ""));
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "price=14");
    
    incDoc = "cmd=update_field,name=doc1,price=13,ts=1;";
    ASSERT_TRUE(psm2.Transfer(PE_BUILD_INC, incDoc, "", ""));
    CheckQuery(GET_TEST_DATA_PATH(), "pk:doc1", "price=13");
}

void ModifyOperationInteTest::TestOngoingOpWithCustomizeIndex()
{
    string pluginPath = TEST_DATA_PATH"modify_demo_indexer_plugins";
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "modify_customized_schema_base.json");
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;
    string docs = "cmd=add,pk=doc1,price=10,category=1 2,payload=1 2 3 4,tags=test1 test,ts=0;"
                  "cmd=add,pk=doc2,price=20,category=2 3,payload=2 3 4 5,tags=test2 test,ts=0;"
                  "cmd=add,pk=doc3,price=30,category=3 4,payload=3 4 5 6,tags=test3 test,ts=0;"
                  "cmd=add,pk=doc4,price=40,category=4 5,payload=4 5 6 7,tags=test4 test,ts=0;"
                  "cmd=update_field,pk=doc1,price=11,ts=0;"
                  "cmd=update_field,pk=doc2,category=2 2,ts=0;";
    IndexlibPartitionCreator::CreateIndexPartition(
            schema, GET_TEST_DATA_PATH(), docs, options, "", false);
    IndexPartitionSchemaPtr newSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "modify_customized_index_schema.json");
    uint32_t schemaId = newSchema->GetSchemaVersionId();
    string patchDir = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
            PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FileSystemWrapper::MkDirIfNotExist(patchDir);
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
                GET_TEST_DATA_PATH(), options, pluginPath);

    PartitionPatcherPtr patcher =
        resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator =
        resourceProvider->CreatePartitionSegmentIterator();
    fieldid_t fieldId1 = newSchema->GetFieldSchema()->GetFieldId("cfield1");
    fieldid_t fieldId2 = newSchema->GetFieldSchema()->GetFieldId("cfield2");
    while (segIterator->IsValid())
    {
        SegmentData segData = segIterator->GetSegmentData();
        // attribute
        IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher(
                "test_customize", segData.GetSegmentId());
        if (indexPatcher)
        {
            for (uint32_t i = 0; i < segData.GetSegmentInfo().docCount; i++)
            {
                indexPatcher->AddField("test1", fieldId1);
                indexPatcher->AddField("test2", fieldId2);
                indexPatcher->EndDocument();
            }
            indexPatcher->Close();
        }
        segIterator->MoveToNext();        
    }
    resourceProvider->DumpDeployIndexForPatchSegments(newSchema);

    options.SetOngoingModifyOperationIds({1});

    IndexPartitionResource resource;
    resource.indexPluginPath = pluginPath;
    PartitionStateMachine newSchemaPsm(DEFAULT_MEMORY_USE_LIMIT, true, resource);
    ASSERT_TRUE(newSchemaPsm.Init(newSchema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    CheckLatestVersion(GET_TEST_DATA_PATH(), 1, {1});
    
    newSchemaPsm.SetOngoingModifyOperations({});
    ASSERT_TRUE(newSchemaPsm.Transfer(PE_BUILD_INC, "", "", ""));
    CheckLatestVersion(GET_TEST_DATA_PATH(), 1, {});
    CheckQuery(GET_TEST_DATA_PATH(), "test_customize:test1",
               "docid=0;docid=1;docid=2;docid=3", pluginPath);
}

void ModifyOperationInteTest::PreparePatchIndex(
        const IndexPartitionSchemaPtr& newSchema,
        const string& attribute, uint32_t delta)
{
    string rootPath = GET_TEST_DATA_PATH();
    uint32_t schemaId = newSchema->GetSchemaVersionId();
    string patchDir = PathUtil::JoinPath(rootPath,
            PartitionPatchIndexAccessor::GetPatchRootDirName(schemaId));
    FileSystemWrapper::MkDirIfNotExist(patchDir);

    IndexPartitionOptions options;
    PartitionResourceProviderPtr resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(
                GET_TEST_DATA_PATH(), options, "");

    PartitionIteratorPtr iter = resourceProvider->CreatePartitionIterator();
    PartitionPatcherPtr patcher =
        resourceProvider->CreatePartitionPatcher(newSchema, patchDir);
    PartitionSegmentIteratorPtr segIterator =
        resourceProvider->CreatePartitionSegmentIterator();
    while (segIterator->IsValid())
    {
        SegmentData segData = segIterator->GetSegmentData();
        AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator(
                attribute, segData.GetSegmentId());
        
        // attribute
        AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(
                attribute, segData.GetSegmentId());
        if (attrPatcher)
        {
            for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++)
            {
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

void ModifyOperationInteTest::CheckLatestVersion(const string& rootPath,
        schemavid_t expectSchemaId, const vector<schema_opid_t>& ongoingOpIds)
{
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);    
    ASSERT_EQ(expectSchemaId, version.GetSchemaVersionId());
    ASSERT_EQ(ongoingOpIds, version.GetOngoingModifyOperations());
}

void ModifyOperationInteTest::CheckQuery(const string& rootPath,
        const string& query, const string& result, const string& pluginPath)
{
    Version onDiskVersion;
    VersionLoader::GetVersion(rootPath, onDiskVersion, INVALID_VERSION);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchemaForVersion(
            rootPath, onDiskVersion.GetVersionId());

    IndexPartitionOptions options;
    IndexPartitionResource resource;
    resource.indexPluginPath = pluginPath;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, resource);
    ASSERT_TRUE(psm.Init(schema, options, rootPath));
    ASSERT_TRUE(psm.Transfer(QUERY, "", query, result));
}

void ModifyOperationInteTest::FillOneAttributeForSegment(
        const PartitionPatcherPtr& patcher,
        const PartitionIteratorPtr& iter,
        const string& attrName, segmentid_t segId)
{
    bool useRawIndex = GET_CASE_PARAM();
    AttributeDataIteratorPtr attrIter = iter->CreateSingleAttributeIterator(attrName, segId);
    AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(attrName, segId);
    if (attrPatcher)
    {
        for (uint32_t i = 0; i < attrPatcher->GetTotalDocCount(); i++)
        {
            assert(attrIter->IsValid());
            if (useRawIndex)
            {
                attrPatcher->AppendFieldValueFromRawIndex(attrIter->GetRawIndexImageValue());
            }
            else
            {
                attrPatcher->AppendFieldValue(attrIter->GetValueStr());
            }
            attrIter->MoveToNext();
        }
        attrPatcher->Close();
    }
}

IE_NAMESPACE_END(partition);

