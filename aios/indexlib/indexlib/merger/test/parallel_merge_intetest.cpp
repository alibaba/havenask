#include "indexlib/merger/test/parallel_merge_intetest.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include <autil/legacy/jsonizable.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace autil::legacy;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ParallelMergeInteTest);

ParallelMergeInteTest::ParallelMergeInteTest()
{
}

ParallelMergeInteTest::~ParallelMergeInteTest()
{
}

void ParallelMergeInteTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mOptions = IndexPartitionOptions();
    mOptions.SetEnablePackageFile(false);
    mSchema = SchemaAdapter::LoadSchema(TEST_DATA_PATH,
                                        "schema/customized_index_parallel_merge_schema.json");
    mPartitionResource.indexPluginPath = TEST_DATA_PATH"demo_indexer_plugins";
}

void ParallelMergeInteTest::CaseTearDown()
{
}

void ParallelMergeInteTest::TestSimpleProcess()
{
    string fullDoc =
        "cmd=add,pk=pk0,cfield1=hello,cfield2=hello world,ts=1,cat_id=1;"
        "cmd=add,pk=pk1,cfield1=hello kitty, cfield2=kitty,ts=1,cat_id=1;"
        "cmd=add,pk=pk2,cfield1=c1, cfield2=hello test,ts=1,cat_id=1;"
        "cmd=add,pk=pk3,cfield1=hello, cfield2=kitty,ts=1,cat_id=2;"
        "cmd=add,pk=pk4,cfield1=c2, cfield2=test,ts=1,cat_id=2;"
        "cmd=add,pk=pk5,cfield1=c3 kitty, cfield2=kitty,ts=1,cat_id=3;"
        "cmd=add,pk=pk6,cfield1=c1, cfield2=test,ts=1,cat_id=4;";
    
    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir)); 
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello",
                             "docid=0;docid=1;docid=2;docid=3;"));

    // merge
    Version version;
    VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(mRootDir, version));
    string mergeMetaDir = mRootDir + "/merge_meta";
    segDir->Init(false, true);
    // prepare plugin manager
    string pluginRoot = TEST_DATA_PATH"demo_indexer_plugins";
    PluginManagerPtr pluginManager(new PluginManager(pluginRoot));
    CounterMapPtr counterMap(new CounterMap());
    PluginResourcePtr resource(new IndexPluginResource(mSchema, mOptions,
                    counterMap, PartitionMeta(), pluginRoot));
    pluginManager->SetPluginResource(resource);
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "indexer_for_intetest";
    moduleInfo.modulePath = "libindexer_for_intetest.so";    
    pluginManager->addModule(moduleInfo);
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = pluginRoot;

    {
        // Begin Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions,
                                    DumpStrategyPtr(), NULL, pluginManager);
        MergeMetaPtr mergeMeta = merger.CreateMergeMeta(true, 2, 0);
        mergeMeta->Store(mergeMetaDir);

        ASSERT_TRUE(FileSystemWrapper::IsDir(mRootDir + "/merge_resource/version.1"));
        DirectoryPtr mergeMetaDir = DirectoryCreator::Create(mRootDir + "/merge_resource/version.1");
        mergeMetaDir->MountPackageFile(PACKAGE_FILE_PREFIX);
        ASSERT_TRUE(mergeMetaDir->IsExist("task_resource.0"));
        string resource0;
        mergeMetaDir->Load("task_resource.0", resource0);
        ASSERT_EQ("1", resource0);
        ASSERT_TRUE(mergeMetaDir->IsExist("task_resource.1"));
        string resource1;
        mergeMetaDir->Load("task_resource.1", resource1);
        ASSERT_EQ("2,3,4", resource1);
    }
    auto CheckInstanceIndex = [] (const string& insDir, const string& indexFile,
                                  set<int32_t>& catIds, size_t expectDocCount)
    {
        string index0Dir = insDir;
        ASSERT_TRUE(FileSystemWrapper::IsDir(index0Dir));
        string index0FileName = index0Dir + "/" + indexFile;
        ASSERT_TRUE(FileSystemWrapper::IsExist(index0FileName));
        string index0;
        FileSystemWrapper::AtomicLoad(index0FileName, index0);
        map<docid_t, string> dataMap;
        int32_t catId;
        autil::legacy::FromJsonString(dataMap, index0);
        ASSERT_EQ(expectDocCount, dataMap.size());
        for (auto it = dataMap.begin(); it!=dataMap.end(); it++)
        {
            vector<string> docData = StringTokenizer::tokenize(
                ConstString(it->second), ",");
            ASSERT_EQ(3, docData.size());
            ASSERT_TRUE(StringUtil::fromString(docData[2], catId));
            ASSERT_TRUE(catIds.find(catId) != catIds.end());
        }                
    };

    {
        // Do Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions,
                                    DumpStrategyPtr(), NULL, pluginManager);
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaDir);

        merger.DoMerge(true, mergeMeta, 0);
        merger.DoMerge(true, mergeMeta, 1);

        set<int32_t> expectedCatIds;
        expectedCatIds.insert(1);
        CheckInstanceIndex(mRootDir + "/instance_0/segment_4_level_0/index",
                           "test_customize/inst_2_0/demo_index_file",
                           expectedCatIds, 3);

        expectedCatIds.clear();
        expectedCatIds.insert(2);
        expectedCatIds.insert(3);
        expectedCatIds.insert(4);
        CheckInstanceIndex(mRootDir + "/instance_1/segment_4_level_0/index",
                           "test_customize/inst_2_1/demo_index_file",
                           expectedCatIds, 4);
    }
    {
        // End Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions,
                                    DumpStrategyPtr(), NULL, pluginManager);
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaDir);
        merger.EndMerge(mergeMeta);

        set<int32_t> expectedCatIds;
        expectedCatIds.insert(1);
        CheckInstanceIndex(mRootDir + "/segment_4_level_0/index",
                           "test_customize/inst_2_0/demo_index_file",
                           expectedCatIds, 3);

        expectedCatIds.clear();
        expectedCatIds.insert(2);
        expectedCatIds.insert(3);
        expectedCatIds.insert(4);
        CheckInstanceIndex(mRootDir + "/segment_4_level_0/index",
                           "test_customize/inst_2_1/demo_index_file",
                           expectedCatIds, 4);
        
        string indexDir = mRootDir + "/segment_4_level_0/index/test_customize";

        ParallelReduceMeta reduceMeta;
        reduceMeta.Load(indexDir);
        ASSERT_EQ(2, reduceMeta.parallelCount);
        ASSERT_EQ("inst_2_0", reduceMeta.GetInsDirName(0));
        ASSERT_EQ("inst_2_1", reduceMeta.GetInsDirName(1));

        ParallelReduceMeta reduceMeta1;
        file_system::DirectoryPtr directory = file_system::DirectoryCreator::Create(indexDir);
        reduceMeta1.Load(directory);
        ASSERT_EQ("inst_2_0", reduceMeta.GetInsDirName(0));
        ASSERT_EQ("inst_2_1", reduceMeta.GetInsDirName(1));
    }
    {
        PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, true, mPartitionResource);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir)); 
        ASSERT_TRUE(psm.Transfer(QUERY, "", "test_customize:c1",
                                 "docid=2;docid=6;"));
    }
}


IE_NAMESPACE_END(merger);

