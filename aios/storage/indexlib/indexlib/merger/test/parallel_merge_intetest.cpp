#include "indexlib/merger/test/parallel_merge_intetest.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/impl/index_partition_options_impl.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace autil::legacy;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, ParallelMergeInteTest);

ParallelMergeInteTest::ParallelMergeInteTest() {}

ParallelMergeInteTest::~ParallelMergeInteTest() {}

void ParallelMergeInteTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mOptions = IndexPartitionOptions();
    mOptions.SetEnablePackageFile(false);
    mSchema = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() +
                                             "schema/customized_index_parallel_merge_schema.json");
    mPartitionResource.indexPluginPath = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
}

void ParallelMergeInteTest::CaseTearDown() {}

void ParallelMergeInteTest::TestSimpleProcess()
{
    RESET_FILE_SYSTEM("ut", false, FileSystemOptions::Offline());
    string fullDoc = "cmd=add,pk=pk0,cfield1=hello,cfield2=hello world,ts=1,cat_id=1;"
                     "cmd=add,pk=pk1,cfield1=hello kitty, cfield2=kitty,ts=1,cat_id=1;"
                     "cmd=add,pk=pk2,cfield1=c1, cfield2=hello test,ts=1,cat_id=1;"
                     "cmd=add,pk=pk3,cfield1=hello, cfield2=kitty,ts=1,cat_id=2;"
                     "cmd=add,pk=pk4,cfield1=c2, cfield2=test,ts=1,cat_id=2;"
                     "cmd=add,pk=pk5,cfield1=c3 kitty, cfield2=kitty,ts=1,cat_id=3;"
                     "cmd=add,pk=pk6,cfield1=c1, cfield2=test,ts=1,cat_id=4;";

    mOptions.GetBuildConfig(false).maxDocCount = 2;
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    mOptions.GetMergeConfig().SetEnablePackageFile(false);
    mOptions.AddVersionDesc("key1", "value1");
    PartitionStateMachine psm(true, mPartitionResource);
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "test_customize:hello", "docid=0;docid=1;docid=2;docid=3;"));
    {
        Version version;
        VersionLoader::GetVersionS(mRootDir, version, 0);
        ASSERT_EQ("value1", version.mDesc["key1"]);
    }
    // merge
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSION);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    string mergeMetaDir = mRootDir + "/merge_meta";
    segDir->Init(false, true);
    // prepare plugin manager
    string pluginRoot = GET_PRIVATE_TEST_DATA_PATH() + "demo_indexer_plugins";
    PluginManagerPtr pluginManager(new PluginManager(pluginRoot));
    CounterMapPtr counterMap(new CounterMap());
    PluginResourcePtr resource(new IndexPluginResource(mSchema, mOptions, counterMap, PartitionMeta(), pluginRoot));
    pluginManager->SetPluginResource(resource);
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "indexer_for_intetest";
    moduleInfo.modulePath = "libindexer_for_intetest.so";
    pluginManager->addModule(moduleInfo);
    IndexPartitionResource partitionResource;
    partitionResource.indexPluginPath = pluginRoot;
    mOptions.AddVersionDesc("batchId", "1234");
    mOptions.DelVersionDesc("key1");
    ASSERT_EQ(1, mOptions.mImpl->mVersionDesc.size());
    {
        // Begin Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, pluginManager,
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta = merger.CreateMergeMeta(true, 2, 0);
        mergeMeta->Store(mergeMetaDir);

        string mergeMetaDirPath = mRootDir + "/merge_resource/version.1/";
        string resource0;
        ASSERT_EQ(FSEC_OK, FslibWrapper::Load(mergeMetaDirPath + "task_resource.0", resource0).Code());
        ASSERT_EQ("1", resource0);
        string resource1;
        ASSERT_EQ(FSEC_OK, FslibWrapper::Load(mergeMetaDirPath + "task_resource.1", resource1).Code());
        ASSERT_EQ("2,3,4", resource1);
    }
    auto CheckInstanceIndex = [](const string& insDir, const string& indexFile, set<int32_t>& catIds,
                                 size_t expectDocCount) {
        string index0Dir = insDir;
        ASSERT_TRUE(FslibWrapper::IsDir(index0Dir).GetOrThrow());
        string index0FileName = index0Dir + "/" + indexFile;
        ASSERT_TRUE(FslibWrapper::IsExist(index0FileName).GetOrThrow());
        string index0;
        FslibWrapper::AtomicLoadE(index0FileName, index0);
        map<docid_t, string> dataMap;
        int32_t catId;
        autil::legacy::FromJsonString(dataMap, index0);
        ASSERT_EQ(expectDocCount, dataMap.size());
        for (auto it = dataMap.begin(); it != dataMap.end(); it++) {
            vector<string> docData = StringTokenizer::tokenize(StringView(it->second), ",");
            ASSERT_EQ(3, docData.size());
            ASSERT_TRUE(StringUtil::fromString(docData[2], catId));
            ASSERT_TRUE(catIds.find(catId) != catIds.end());
        }
    };

    {
        // Do Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, pluginManager,
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaDir);

        merger.DoMerge(true, mergeMeta, 0);
        string branchRootDir0 =
            util::PathUtil::JoinPath(mRootDir, "instance_0", merger.TEST_GetBranchFs()->GetBranchName());
        merger.DoMerge(true, mergeMeta, 1);
        string branchRootDir1 =
            util::PathUtil::JoinPath(mRootDir, "instance_1", merger.TEST_GetBranchFs()->GetBranchName());

        set<int32_t> expectedCatIds;
        expectedCatIds.insert(1);
        CheckInstanceIndex(branchRootDir0 + "/segment_4_level_0/index", "test_customize/inst_2_0/demo_index_file",
                           expectedCatIds, 3);

        expectedCatIds.clear();
        expectedCatIds.insert(2);
        expectedCatIds.insert(3);
        expectedCatIds.insert(4);
        CheckInstanceIndex(branchRootDir1 + "/segment_4_level_0/index", "test_customize/inst_2_1/demo_index_file",
                           expectedCatIds, 4);
    }
    {
        // End Merge
        IndexPartitionMerger merger(segDir, mSchema, mOptions, DumpStrategyPtr(), NULL, pluginManager,
                                    CommonBranchHinterOption::Test());
        MergeMetaPtr mergeMeta(new IndexMergeMeta());
        mergeMeta->Load(mergeMetaDir);
        merger.EndMerge(mergeMeta);
        // check version
        Version version;
        VersionLoader::GetVersionS(mRootDir, version, 1);
        string batchId, value1;
        ASSERT_TRUE(version.GetDescription("batchId", batchId));
        ASSERT_TRUE(version.GetDescription("key1", value1));
        ASSERT_EQ("1234", batchId);
        ASSERT_EQ("value1", value1);

        set<int32_t> expectedCatIds;
        expectedCatIds.insert(1);
        CheckInstanceIndex(mRootDir + "/segment_4_level_0/index", "test_customize/inst_2_0/demo_index_file",
                           expectedCatIds, 3);

        expectedCatIds.clear();
        expectedCatIds.insert(2);
        expectedCatIds.insert(3);
        expectedCatIds.insert(4);
        CheckInstanceIndex(mRootDir + "/segment_4_level_0/index", "test_customize/inst_2_1/demo_index_file",
                           expectedCatIds, 4);

        auto indexDir = GET_PARTITION_DIRECTORY()->GetDirectory("segment_4_level_0/index/test_customize", false);

        ParallelReduceMeta reduceMeta;
        reduceMeta.Load(indexDir);
        ASSERT_EQ(2, reduceMeta.parallelCount);
        ASSERT_EQ("inst_2_0", reduceMeta.GetInsDirName(0));
        ASSERT_EQ("inst_2_1", reduceMeta.GetInsDirName(1));

        ASSERT_EQ("inst_2_0", reduceMeta.GetInsDirName(0));
        ASSERT_EQ("inst_2_1", reduceMeta.GetInsDirName(1));
    }
    {
        PartitionStateMachine psm(true, mPartitionResource);
        INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
        ASSERT_TRUE(psm.Transfer(QUERY, "", "test_customize:c1", "docid=2;docid=6;"));
    }
}
}} // namespace indexlib::merger
