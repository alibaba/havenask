#include "indexlib/index/normal/inverted_index/customized_index/test/customized_index_intetest.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_module_factory.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_indexer.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::plugin;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::merger;

namespace indexlib { namespace index {

CustomizedIndexInteTest::CustomizedIndexInteTest() {}

CustomizedIndexInteTest::~CustomizedIndexInteTest() {}

void CustomizedIndexInteTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mOptions = IndexPartitionOptions();
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &mOptions);
    mOptions.SetEnablePackageFile(false);
    mOptions.GetMergeConfig().SetEnablePackageFile(false);
    mSchema = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "customized_index_schema.json");
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    mPluginManager.reset(new PluginManager(pluginRoot));
    mCounterMap.reset(new CounterMap());
    PluginResourcePtr resource(
        new IndexPluginResource(mSchema, mOptions, mCounterMap, PartitionMeta(), pluginRoot, nullptr));
    mPluginManager->SetPluginResource(resource);
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "demo_indexer";
    moduleInfo.modulePath = "libdemo_indexer.so";
    mPluginManager->addModule(moduleInfo);
    mPartitionResource = IndexPartitionResource();
    mPartitionResource.indexPluginPath = pluginRoot;
}

void CustomizedIndexInteTest::CaseTearDown() {}

void CustomizedIndexInteTest::TestSimpleProcess()
{
    Module* module = mPluginManager->getModule("demo_indexer");
    ASSERT_TRUE(module);
    IndexModuleFactory* factory =
        dynamic_cast<IndexModuleFactory*>(module->getModuleFactory(IndexPluginUtil::INDEX_MODULE_FACTORY_SUFFIX));
    ASSERT_TRUE(factory);
    CounterMapPtr counterMap(new CounterMap());
    IndexerResourcePtr resource(new IndexerResource(mSchema, mOptions, counterMap, PartitionMeta(), "customized_index",
                                                    GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins", nullptr));
    KeyValueMap parameters;
    IndexerPtr indexer(factory->createIndexer(parameters));
    indexer->Init(resource);
    IndexReducerPtr reducer(factory->createIndexReducer(parameters));

    MergeItemHint hint(0, 1.0, 1, 2);
    MergeTaskResourceVector taskResources;
    reducer->Init(resource, hint, taskResources);

    ASSERT_TRUE(reducer);
    IndexSegmentRetrieverPtr indexSegRetriever(factory->createIndexSegmentRetriever(parameters));

    const auto& dir = GET_PARTITION_DIRECTORY();
    Pool pool;
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("hello"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 0));
    }
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("world"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 1));
    }
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("hello world"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 2));
    }
    ASSERT_TRUE(indexer->Dump(dir));

    IndexerSegmentData segData;
    segData.indexDir = dir;
    segData.segId = 0;
    segData.baseDocId = 0;
    segData.isBuildingSegment = false;
    ASSERT_TRUE(indexSegRetriever->Open(segData));

    MatchInfo matchInfo = indexSegRetriever->Search("hello", &pool);
    ASSERT_EQ(2u, matchInfo.matchCount);
    docid_t* docIds = matchInfo.docIds;
    matchvalue_t* matchValues = matchInfo.matchValues;

    // in DemoIndexer, matchValue is the length of the matched docStr
    EXPECT_EQ(docid_t(0), docIds[0]);
    EXPECT_EQ(docid_t(2), docIds[1]);
    EXPECT_EQ(5, matchValues[0].GetInt32());
    EXPECT_EQ(11, matchValues[1].GetInt32());
}

void CustomizedIndexInteTest::TestIndexRetrieverAsync()
{
    Module* module = mPluginManager->getModule("demo_indexer");
    ASSERT_TRUE(module);
    IndexModuleFactory* factory =
        dynamic_cast<IndexModuleFactory*>(module->getModuleFactory(IndexPluginUtil::INDEX_MODULE_FACTORY_SUFFIX));
    ASSERT_TRUE(factory);
    CounterMapPtr counterMap(new CounterMap());
    IndexerResourcePtr resource(new IndexerResource(mSchema, mOptions, counterMap, PartitionMeta(), "customized_index",
                                                    GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins", nullptr));
    KeyValueMap parameters;
    IndexerPtr indexer(factory->createIndexer(parameters));
    indexer->Init(resource);
    IndexReducerPtr reducer(factory->createIndexReducer(parameters));

    MergeItemHint hint(0, 1.0, 1, 2);
    MergeTaskResourceVector taskResources;
    reducer->Init(resource, hint, taskResources);
    ASSERT_TRUE(reducer);

    IndexSegmentRetrieverPtr indexSegRetriever(factory->createIndexSegmentRetriever(parameters));

    const auto& dir = GET_PARTITION_DIRECTORY();
    Pool pool;
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("hello"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 0));
    }
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("world"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 1));
    }
    {
        vector<const Field*> fieldVec;
        IndexRawField field(&pool);
        field.SetData(autil::MakeCString(string("hello world"), &pool));
        fieldVec.push_back(&field);
        EXPECT_TRUE(indexer->Build(fieldVec, 2));
    }
    ASSERT_TRUE(indexer->Dump(dir));

    IndexerSegmentData segData;
    segData.indexDir = dir;
    segData.segId = 0;
    segData.baseDocId = 0;
    segData.isBuildingSegment = false;
    ASSERT_TRUE(indexSegRetriever->Open(segData));

    IndexRetriever* indexRetriever = factory->createIndexRetriever(parameters);
    vector<IndexSegmentRetrieverPtr> segmentRetrievers(1, indexSegRetriever);
    ASSERT_TRUE(indexRetriever->Init(DeletionMapReaderPtr(), segmentRetrievers));
    Term term("hello", "demo_index");
    indexRetriever->SearchAsync(term, &pool, [](bool success, vector<SegmentMatchInfo> segMatchInfos) {
        MatchInfoPtr matchInfo = segMatchInfos[0].matchInfo;
        EXPECT_EQ(2u, matchInfo->matchCount);
        docid_t* docIds = matchInfo->docIds;
        matchvalue_t* matchValues = matchInfo->matchValues;

        // in DemoIndexer, matchValue is the length of the matched docStr
        EXPECT_EQ(docid_t(0), docIds[0]);
        EXPECT_EQ(docid_t(2), docIds[1]);
        EXPECT_EQ(5, matchValues[0].GetInt32());
        EXPECT_EQ(11, matchValues[1].GetInt32());
        return;
    });
    delete indexRetriever;
}

void CustomizedIndexInteTest::TestTwoIndexShareOneIndexer()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "customized_index_schema2.json");
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    IndexPartitionOptions options;
    PluginManagerPtr manager = IndexPluginLoader::Load(pluginRoot, schema->GetIndexSchema(), options);
    ASSERT_TRUE(manager);
    IndexConfigPtr indexConfig1 = schema->GetIndexSchema()->GetIndexConfig("cindex1");
    IndexConfigPtr indexConfig2 = schema->GetIndexSchema()->GetIndexConfig("cindex2");
    IndexerPtr indexer1(IndexPluginUtil::CreateIndexer(indexConfig1, manager));
    IndexerPtr indexer2(IndexPluginUtil::CreateIndexer(indexConfig2, manager));
    ASSERT_TRUE(indexer1);
    ASSERT_TRUE(indexer2);
    EXPECT_EQ(string("100"), indexer1->mParameters["k1"]);
    EXPECT_EQ(string("200"), indexer1->mParameters["k2"]);
    EXPECT_EQ(string("300"), indexer2->mParameters["k1"]);
    EXPECT_EQ(string("400"), indexer2->mParameters["k2"]);
}

void CustomizedIndexInteTest::TestMultiFieldBuild()
{
    DirectoryPtr rootDirectory;
    {
        string fullDoc = "cmd=add,pk=pk0,cfield1=hello,cfield2=hello world,ts=1;"
                         "cmd=add,pk=pk1,cfield1=hello kitty, cfield2=kitty,ts=1;"
                         "cmd=add,pk=pk2,cfield1=hello, cfield2=hello test,ts=1;";

        PartitionStateMachine psm(true, mPartitionResource);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello", "docid=0;docid=1;docid=2;"));
        string incDoc = "cmd=add,pk=pk3,cfield1=hello boy,ts=4;"
                        "cmd=add,pk=pk4,cfield1=hello girl,ts=5;"
                        "cmd=delete,pk=pk1,ts=6;"
                        "cmd=delete,pk=pk2,ts=7;"
                        "cmd=delete,pk=pk4,ts=8;";
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "", "docid=0;docid=1;docid=2;docid=3;docid=4;"));

        // check counters
        IndexPartitionPtr indexPartition = psm.GetIndexPartition();
        auto counterMap = indexPartition->GetCounterMap();
        EXPECT_EQ(5, counterMap->GetAccCounter("test_customize.user.DemoIndexer.docCount")->Get());
        rootDirectory = indexPartition->GetRootDirectory();
    }
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, true);
    mOptions.SetIsOnline(false);
    IndexPartitionMerger merger(segDir, mSchema, mOptions, merger::DumpStrategyPtr(), NULL, mPluginManager,
                                CommonBranchHinterOption::Test());
    auto counterMap = merger.GetCounterMap();
    // TODO(qingran & panghai)
    // merger.Merge(true);
    // EXPECT_EQ(2, counterMap->GetStateCounter("test_customize.user.DemoIndexReducer.mergedItemCount")->Get());
}

void CustomizedIndexInteTest::TestSearchBuildingData()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw", "pk:primarykey64:pk;test_customize:customized:demo_field", "", "");

    auto customizedIndexConfig =
        DYNAMIC_POINTER_CAST(CustomizedIndexConfig, schema->GetIndexSchema()->GetIndexConfig("test_customize"));
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;"
                     "cmd=add,pk=pk1,demo_field=hello kitty,ts=2;"
                     "cmd=add,pk=pk2,demo_field=hello baby,ts=3;";

    PartitionStateMachine psm(true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello", "docid=0;docid=1;docid=2"));
    string rtDoc = "cmd=add,pk=pk3,demo_field=hello boy,ts=4;"
                   "cmd=add,pk=pk4,demo_field=hello girl,ts=5;"
                   "cmd=delete,pk=pk1,ts=6;"
                   "cmd=delete,pk=pk2,ts=7;"
                   "cmd=delete,pk=pk4,ts=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "test_customize:hello", "docid=0;docid=3;"));
}

void CustomizedIndexInteTest::TestMergeCustomizedIndex()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw", "pk:primarykey64:pk;test_customize:customized:demo_field", "", "");

    auto customizedIndexConfig =
        DYNAMIC_POINTER_CAST(CustomizedIndexConfig, schema->GetIndexSchema()->GetIndexConfig("test_customize"));
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;"
                     "cmd=add,pk=pk1,demo_field=hello kitty,ts=2;"
                     "cmd=add,pk=pk2,demo_field=hello baby,ts=3;";

    PartitionStateMachine psm(true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello", "docid=0;docid=1;docid=2"));
    string incDoc = "cmd=add,pk=pk3,demo_field=hello boy,ts=4;"
                    "cmd=add,pk=pk4,demo_field=hello girl,ts=5;"
                    "cmd=delete,pk=pk1,ts=6;"
                    "cmd=delete,pk=pk2,ts=7;"
                    "cmd=delete,pk=pk4,ts=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "test_customize:hello", "docid=0;docid=1;"));
}

void CustomizedIndexInteTest::TestIsCompleteMergeFlag()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw", "pk:primarykey64:pk;test_customize:customized:demo_field", "", "");

    auto customizedIndexConfig =
        DYNAMIC_POINTER_CAST(CustomizedIndexConfig, schema->GetIndexSchema()->GetIndexConfig("test_customize"));
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;"
                     "cmd=add,pk=pk1,demo_field=hello kitty,ts=2;"
                     "cmd=add,pk=pk2,demo_field=hello baby,ts=3;";

    PartitionStateMachine psm(true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello", "docid=0;docid=1;docid=2"));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(partDirectory->IsExist("segment_0_level_0"));
    ASSERT_FALSE(partDirectory->IsExist("segment_1_level_0"));
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    CheckDemoIndexerMeta(partDirectory, "segment_0_level_0", "test_customize", {"segment_type", "plugin_path"},
                         {"build_segment", pluginRoot});

    string incDoc = "cmd=add,pk=pk3,demo_field=hello boy,ts=4;"
                    "cmd=add,pk=pk4,demo_field=hello girl,ts=5;"
                    "cmd=delete,pk=pk1,ts=6;"
                    "cmd=delete,pk=pk2,ts=7;"
                    "cmd=delete,pk=pk4,ts=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "test_customize:hello", "docid=0;docid=1;"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckDemoIndexerMeta(GET_CHECK_DIRECTORY(), "segment_1_level_0", "test_customize", {"segment_type"},
                         {"build_segment"});
    CheckDemoIndexerMeta(partDirectory, "segment_2_level_0", "test_customize", {"segment_type", "is_complete_merge"},
                         {"merge_segment", "true"});

    incDoc = "cmd=add,pk=pk5,demo_field=hello boy,ts=4;"
             "cmd=add,pk=pk6,demo_field=hello girl,ts=5;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckDemoIndexerMeta(partDirectory, "segment_3_level_0", "test_customize", {"segment_type"}, {"build_segment"});

    incDoc = "cmd=add,pk=pk7,demo_field=hello boy,ts=4;"
             "cmd=add,pk=pk8,demo_field=hello girl,ts=5;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckDemoIndexerMeta(partDirectory, "segment_4_level_0", "test_customize", {"segment_type"}, {"build_segment"});

    MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=3,4");

    psm.SetMergeConfig(mergeConfig);

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckDemoIndexerMeta(partDirectory, "segment_5_level_0", "test_customize", {"segment_type", "is_complete_merge"},
                         {"merge_segment", "false"});
}

void CustomizedIndexInteTest::TestEstimateBuildMemUse()
{
    string docString = "cmd=add,pk=pk0,cfield1=hello,cfield2=world,ts=1;"
                       "cmd=add,pk=pk1,cfield1=hello,cfield2=kitty,ts=1;"
                       "cmd=add,pk=pk2,cfield1=hello,cfield2=apple,ts=1;";

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(mSchema, docString);

    BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("test_customize");
    assert(indexConfig);

    CustomizedIndexWriter cindexWriter(mPluginManager, 0, /*ignoreBuildError=*/false);
    cindexWriter.CustomizedInit(indexConfig, &buildMetrics, /*PartitionSegmentIteratorPtr=*/nullptr);

    int64_t currentMemUse = buildMetrics.GetValue(util::BMT_CURRENT_MEMORY_USE);

    for (size_t i = 0; i < docs.size(); ++i) {
        NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, docs[i]);
        doc->SetDocId((docid_t)i);
        const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
        assert(indexDoc);
        IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
        IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

        for (; iter != iterEnd; ++iter) {
            const Field* field = *iter;
            if (!field || field->GetFieldTag() != Field::FieldTag::RAW_FIELD) {
                continue;
            }
            cindexWriter.AddField(field);
        }
        cindexWriter.EndDocument(*indexDoc);
        EXPECT_TRUE(buildMetrics.GetValue(util::BMT_CURRENT_MEMORY_USE) >= currentMemUse);
        currentMemUse = buildMetrics.GetValue(util::BMT_CURRENT_MEMORY_USE);
        typedef pair<docid_t, string> DocItem;
        EXPECT_EQ((sizeof(DocItem) + 11) * (i + 1), buildMetrics.GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE))
            << "differ in " << i << " th doc";
        EXPECT_EQ(0, buildMetrics.GetValue(util::BMT_DUMP_EXPAND_MEMORY_SIZE));
        EXPECT_EQ(sizeof(DocItem) * (i + 1) * DemoIndexer::TO_JSON_EXPAND_FACTOR + (i + 1) * 11,
                  buildMetrics.GetValue(util::BMT_DUMP_FILE_SIZE));
    }
}

void CustomizedIndexInteTest::TestEstimateMergeMemUse()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw", "pk:primarykey64:pk;test_customize:customized:demo_field", "", "");

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("test_customize");
    auto customizedIndexConfig = DYNAMIC_POINTER_CAST(CustomizedIndexConfig, indexConfig);
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    {
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetBuildConfig().enablePackageFile = false;
        util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
        PartitionStateMachine psm(true, mPartitionResource);
        INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
        string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;"
                         "cmd=add,pk=pk1,demo_field=hello kitty,ts=2;"
                         "cmd=add,pk=pk2,demo_field=hello baby,ts=3;";

        string incDoc1 = "cmd=add,pk=pk4,demo_field=hello,ts=4;"
                         "cmd=add,pk=pk5,demo_field=hello kitty,ts=5;";
        string incDoc2 = "cmd=add,pk=pk6,demo_field=hello,ts=4;"
                         "cmd=add,pk=pk7,demo_field=hello kitty,ts=5;";
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "", ""));
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "", ""));
    }
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version version;
    VersionLoader::GetVersion(mPartitionDirectory, version, INVALID_VERSIONID);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mPartitionDirectory, version));
    segDir->Init(false, true);

    SegmentMergeInfos mergeInfos; // create 3 empty merge info
    mergeInfos.push_back(SegmentMergeInfo(0, SegmentInfo(), 0, 0));
    mergeInfos.push_back(SegmentMergeInfo(1, SegmentInfo(), 0, 3));
    mergeInfos.push_back(SegmentMergeInfo(2, SegmentInfo(), 0, 5));

    CustomizedIndexMerger merger(mPluginManager);
    merger.Init(indexConfig);
    index_base::OutputSegmentMergeInfos outputSegMergeInfos;
    index::MergerResource resource;

    int64_t mergeMemUse = merger.EstimateMemoryUse(segDir, resource, mergeInfos, outputSegMergeInfos, false);

    vector<string> indexFiles;
    indexFiles.push_back(mRootDir + "segment_0_level_0/index/test_customize/demo_index_file");
    indexFiles.push_back(mRootDir + "segment_1_level_0/index/test_customize/demo_index_file");
    indexFiles.push_back(mRootDir + "segment_2_level_0/index/test_customize/demo_index_file");
    int64_t totalFileLength = 0;
    for (const auto& indexFile : indexFiles) {
        auto ret = file_system::FslibWrapper::GetFileLength(indexFile);
        if (ret.OK()) {
            totalFileLength += ret.GetOrThrow();
        }
    }
    ASSERT_EQ(totalFileLength * 2, mergeMemUse);
}

void CustomizedIndexInteTest::TestEstimateReopenMemUse()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;demo_field:raw", "pk:primarykey64:pk;test_customize:customized:demo_field", "", "");

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("test_customize");
    auto customizedIndexConfig = DYNAMIC_POINTER_CAST(CustomizedIndexConfig, indexConfig);
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().enablePackageFile = false;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();

    PartitionStateMachine psm(true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;"
                     "cmd=add,pk=pk1,demo_field=hello kitty,ts=2;"
                     "cmd=add,pk=pk2,demo_field=hello baby,ts=3;";

    string incDoc1 = "cmd=add,pk=pk4,demo_field=hello,ts=4;"
                     "cmd=add,pk=pk5,demo_field=hello kitty,ts=5;";

    string incDoc2 = "cmd=add,pk=pk6,demo_field=hello,ts=4;"
                     "cmd=add,pk=pk7,demo_field=hello kitty,ts=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "", ""));

    rootDir->Sync(true);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version lastLoadedVersion;
    VersionLoader::GetVersion(mPartitionDirectory, lastLoadedVersion, INVALID_VERSIONID);
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "", ""));
    rootDir->Sync(true);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version newVersion;
    VersionLoader::GetVersion(mPartitionDirectory, newVersion, INVALID_VERSIONID);

    PartitionResourceCalculator calculator(options.GetOnlineConfig());
    calculator.Init(rootDir, PartitionWriterPtr(), InMemorySegmentContainerPtr(), mPluginManager);

    size_t reopenMemUse =
        calculator.EstimateDiffVersionLockSizeWithoutPatch(schema, rootDir, newVersion, lastLoadedVersion);

    ASSERT_TRUE(reopenMemUse > 0) << "reopen memory use: " << reopenMemUse << endl;
}

void CustomizedIndexInteTest::TestEstimateInitMemUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("pk:string;demo_field:raw", "test_customize:customized:demo_field", "", "");

    auto customizedIndexConfig =
        DYNAMIC_POINTER_CAST(CustomizedIndexConfig, schema->GetIndexSchema()->GetIndexConfig("test_customize"));
    ASSERT_TRUE(customizedIndexConfig);
    customizedIndexConfig->SetIndexer("demo_indexer");
    mOptions.GetBuildConfig().dumpThreadCount = 1;

    string fullDoc = "cmd=add,pk=pk0,demo_field=hello,ts=1;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, fullDoc);
    segmentMetrics->SetDistinctTermCount("test_customize", 2);
    SingleSegmentWriter segWriter(schema, mOptions);
    size_t estimateMemUse = SingleSegmentWriter::EstimateInitMemUse(segmentMetrics, schema, mOptions, mPluginManager,
                                                                    PartitionSegmentIteratorPtr());
    ASSERT_EQ(2 * 100u, estimateMemUse);
}

IndexPartitionSchemaPtr CustomizedIndexInteTest::PrepareCustomIndexSchema(const string& fieldStr,
                                                                          const string& indexStr,
                                                                          const vector<string>& customIndexes)
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fieldStr, indexStr, "", "");
    for (size_t i = 0; i < customIndexes.size(); i++) {
        auto customizedIndexConfig =
            DYNAMIC_POINTER_CAST(CustomizedIndexConfig, schema->GetIndexSchema()->GetIndexConfig(customIndexes[i]));
        assert(customizedIndexConfig);
        customizedIndexConfig->SetIndexer("demo_indexer");
    }
    return schema;
}

void CustomizedIndexInteTest::TestAddNewCustomIndexWithCustomIndexExist()
{
    IndexPartitionSchemaPtr schema =
        PrepareCustomIndexSchema("pk:string;cfield1:raw;cfield2:raw",
                                 "pk:primarykey64:pk;test_customize:customized:cfield1", {"test_customize"});

    string rootDir = GET_TEMP_DATA_PATH();
    IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = pluginRoot;

    string fullDoc = "cmd=add,pk=pk0,cfield1=hello,cfield2=hello world,ts=1";

    PartitionStateMachine psm(true, partitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:pk0", "docid=0"));

    PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
        rootDir, options, pluginRoot, INVALID_VERSIONID);
    ASSERT_TRUE(resourceProvider);
    IndexPartitionSchemaPtr newSchema = PrepareCustomIndexSchema("pk:string;cfield1:raw;cfield2:raw",
                                                                 "pk:primarykey64:pk;"
                                                                 "test_customize1:customized:cfield2",
                                                                 {"test_customize1"});
    newSchema->SetSchemaVersionId(1);
    string patchDir = PathUtil::JoinPath(resourceProvider->GetPartition()->GetRootDirectory()->GetOutputPath(),
                                         PartitionPatchIndexAccessor::GetPatchRootDirName(1));
    FslibWrapper::MkDirE(patchDir, true, true);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    string branchName;
    auto patchDirectory = BranchFS::GetDirectoryFromDefaultBranch(patchDir, fsOptions, nullptr, &branchName);
    auto partitionPatcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDirectory);
    ASSERT_TRUE(partitionPatcher);
    IndexDataPatcherPtr indexPatcher = partitionPatcher->CreateSingleIndexPatcher("test_customize1", 0);
    ASSERT_TRUE(indexPatcher);
    indexPatcher->AddField("kitty", 2);
    indexPatcher->EndDocument();
    indexPatcher->Close();
    resourceProvider->StoreVersion(newSchema, 1);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
    PartitionStateMachine psm1(true, partitionResource);
    INDEXLIB_TEST_TRUE(psm1.Init(newSchema, options, rootDir));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "test_customize1:kitty", "docid=0"));
}

void CustomizedIndexInteTest::TestPartitionResourceProviderSupportCustomIndex()
{
    string field = "pk:string;cfield1:raw;cfield2:raw";
    string index = "pk:primarykey64:pk";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");

    string rootDir = GET_TEMP_DATA_PATH();
    IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = pluginRoot;

    string fullDoc = "cmd=add,pk=pk0,cfield1=hello,cfield2=hello world,ts=1";

    PartitionStateMachine psm(true, partitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:pk0", "docid=0"));

    // test add custom index
    PartitionResourceProviderPtr resourceProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(
        rootDir, options, pluginRoot, INVALID_VERSIONID);
    ASSERT_TRUE(resourceProvider);
    IndexPartitionSchemaPtr newSchema = PrepareCustomIndexSchema("pk:string;cfield1:raw;cfield2:raw",
                                                                 "pk:primarykey64:pk;"
                                                                 "test_customize1:customized:cfield2",
                                                                 {"test_customize1"});
    newSchema->SetSchemaVersionId(1);
    auto patchDir = PathUtil::JoinPath(resourceProvider->GetPartition()->GetRootDirectory()->GetOutputPath(),
                                       PartitionPatchIndexAccessor::GetPatchRootDirName(1));
    FslibWrapper::MkDirE(patchDir, true, true);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    string branchName;
    auto patchDirectory = BranchFS::GetDirectoryFromDefaultBranch(patchDir, fsOptions, nullptr, &branchName);
    auto partitionPatcher = resourceProvider->CreatePartitionPatcher(newSchema, patchDirectory);
    ASSERT_TRUE(partitionPatcher);
    IndexDataPatcherPtr indexPatcher = partitionPatcher->CreateSingleIndexPatcher("test_customize1", 0);
    ASSERT_TRUE(indexPatcher);
    indexPatcher->AddField("kitty", 2);
    indexPatcher->EndDocument();
    indexPatcher->Close();
    resourceProvider->StoreVersion(newSchema, 1);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();

    RESET_FILE_SYSTEM();
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    CheckDemoIndexerMeta(partDirectory, "patch_index_1/segment_0_level_0", "test_customize1",
                         {"segment_type", "plugin_path"}, {"build_segment", pluginRoot});

    PartitionStateMachine psm1(true, partitionResource);
    INDEXLIB_TEST_TRUE(psm1.Init(newSchema, options, rootDir));
    ASSERT_TRUE(psm1.Transfer(QUERY, "", "test_customize1:kitty", "docid=0"));

    MergeConfig mergeConfig;
    mergeConfig.mergeStrategyStr = "optimize";
    mergeConfig.mergeStrategyParameter.SetLegacyString("skip-single-merged-segment=false");
    psm.SetMergeConfig(mergeConfig);

    ASSERT_TRUE(psm1.Transfer(BUILD_INC, "", "test_customize1:kitty", "docid=0"));

    RESET_FILE_SYSTEM();
    partDirectory = GET_PARTITION_DIRECTORY();
    CheckDemoIndexerMeta(partDirectory, "segment_1_level_0", "test_customize1",
                         {"segment_type", "is_complete_merge", "plugin_path"}, {"merge_segment", "true", pluginRoot});

    // test drop custom index
    schema->SetSchemaVersionId(2);
    resourceProvider =
        PartitionResourceProviderFactory::GetInstance()->CreateProvider(rootDir, options, "", INVALID_VERSIONID);
    patchDir = PathUtil::JoinPath(resourceProvider->GetPartition()->GetRootDirectory()->GetOutputPath(),
                                  PartitionPatchIndexAccessor::GetPatchRootDirName(2));
    FslibWrapper::MkDirE(patchDir, true, true);
    resourceProvider->StoreVersion(schema, 3);
    PartitionStateMachine psm2(true, partitionResource);
    resourceProvider->GetPartition()->GetBranchFileSystem()->TEST_MoveToMainRoad();
    INDEXLIB_TEST_TRUE(psm2.Init(schema, options, rootDir));
    ASSERT_TRUE(psm2.Transfer(QUERY, "", "test_customize1:kitty", ""));
}

void CustomizedIndexInteTest::CheckDemoIndexerMeta(const DirectoryPtr& rootDir, const string& segDir,
                                                   const string& indexName, const vector<string> keys,
                                                   const vector<string> values)
{
    ASSERT_TRUE(rootDir->IsExist(segDir));
    DirectoryPtr segDirectory = rootDir->GetDirectory(segDir, true);
    string indexDirPath = segDir + "/index/" + indexName + "/";
    DirectoryPtr indexDir = rootDir->GetDirectory(indexDirPath, false);
    ASSERT_TRUE(indexDir);
    string metaFileContent;
    indexDir->Load("demo_meta_file", metaFileContent);
    DemoIndexer::KVMap meta;
    autil::legacy::FromJsonString(meta, metaFileContent);
    int i = 0;
    for (const auto& key : keys) {
        string expectValue = values[i];
        string actualValue = meta[key];
        EXPECT_EQ(expectValue, actualValue) << key;
        i++;
    }
}
}} // namespace indexlib::index
