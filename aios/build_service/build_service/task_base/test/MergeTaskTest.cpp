#include "build_service/test/unittest.h"
#include "build_service/task_base/MergeTask.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/common/CheckpointList.h"
#include "build_service/config/ConfigDefine.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/index_merge_meta.h"
#include <autil/StringUtil.h>
#include "build_service/config/CLIOptionNames.h"
#include <indexlib/util/counter/counter_map.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace autil::legacy;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(config);

using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::common;

namespace build_service {
namespace task_base {

class MockIndexPartitionMerger : public IndexPartitionMerger {
public:
    MockIndexPartitionMerger(const IE_NAMESPACE(merger)::SegmentDirectoryPtr& segDir,
                             const IndexPartitionSchemaPtr& schema,
                             const IndexPartitionOptions& options,
                             const DumpStrategyPtr &dumpStrategy)
        : IndexPartitionMerger(segDir, schema, options, dumpStrategy, NULL,
                               IE_NAMESPACE(plugin)::PluginManagerPtr())
    {}
    virtual ~MockIndexPartitionMerger() {}
public:
    void PrepareMerge(int64_t ts) {}
    MOCK_METHOD1(Merge, bool(bool));
    MOCK_METHOD3(CreateMergeMeta, MergeMetaPtr(bool, uint32_t, int64_t));
    MOCK_METHOD3(DoMerge, void(bool optimize, const MergeMetaPtr&, uint32_t));
    MOCK_METHOD2(EndMerge, void(const MergeMetaPtr&, versionid_t));
};


class MergeTaskTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void setConfig(uint32_t buildParallelNum,
                   uint32_t mergeParallelNum,
                   uint32_t partSplitNum = 1)
    {
        _mergeTask._jobConfig._config.buildParallelNum = buildParallelNum;
        _mergeTask._jobConfig._config.mergeParallelNum = mergeParallelNum;
        _mergeTask._jobConfig._config.partitionSplitNum = partSplitNum;
    }
    void testRemoveObsoleteMergeMeta(uint32_t keepCount, uint32_t totalCount);
    void createDir(const string &path, uint32_t count);
    void checkDirCount(const string &path, uint32_t keepCount, uint32_t totalCount);
private:
    MergeTask _mergeTask;
    JobConfig _jobConfig;
    BuildRuleConfig _buildRuleConfig;
};

void MergeTaskTest::setUp() {
}

void MergeTaskTest::tearDown() {
}

ACTION(ThrowException) {
    throw autil::legacy::ExceptionBase("merge exception");
}

//TODO : mock indexlib interface
TEST_F(MergeTaskTest, testDoMerge) {
    IE_NAMESPACE(merger)::SegmentDirectoryPtr segDir(new IE_NAMESPACE(merger)::SegmentDirectory);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    schema->AddFieldConfig("field", ft_string);
    SingleFieldIndexConfigPtr indexConfig = 
        IndexConfigCreator::CreateSingleFieldIndexConfig(
                "index", it_string, "field", "", false, schema->GetFieldSchema());
    schema->AddIndexConfig(indexConfig);
    
    DumpStrategyPtr dumpStrategy;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    MockIndexPartitionMerger *mockMerger = 
        new MockIndexPartitionMerger(segDir, schema, options, dumpStrategy);
    IndexPartitionMergerPtr indexPartitionMerger(mockMerger);
    PartitionId pid;
    pid.set_role(ROLE_MERGER);
    pid.set_step(BUILD_STEP_FULL);


    pid.mutable_range()->set_from(0);
    pid.mutable_range()->set_to(65535);
    pid.mutable_buildid()->set_datatable("simple");
    pid.mutable_buildid()->set_generationid(0);
    *pid.add_clusternames() = "cluster";
    pid.set_mergeconfigname("full");
    _mergeTask._kvMap[MERGE_TIMESTAMP] = "1234567";
    string targetIndexPath = "/fake_target_index_path";
    {
        pair<MergeTask::MergeStep, uint32_t> mergeStep = make_pair(MergeTask::MS_INIT_MERGE, uint32_t(0));
        MergeMetaPtr mergeMeta(new IndexMergeMeta);
        Version fakeVersion(1);
        mergeMeta->SetTargetVersion(fakeVersion);
        EXPECT_CALL(*mockMerger, CreateMergeMeta(true, _, _)).WillOnce(Return(mergeMeta));
        ASSERT_TRUE(_mergeTask.doMerge(indexPartitionMerger, mergeStep, true, pid, targetIndexPath));
    }
    {
        pair<MergeTask::MergeStep, uint32_t> mergeStep = make_pair(MergeTask::MS_DO_MERGE, uint32_t(0));
        EXPECT_CALL(*mockMerger, DoMerge(true, _, _)).Times(1);
        ASSERT_TRUE(_mergeTask.doMerge(indexPartitionMerger, mergeStep, true, pid, targetIndexPath));
    }
    {
        pair<MergeTask::MergeStep, uint32_t> mergeStep = make_pair(MergeTask::MS_END_MERGE, uint32_t(0));
        EXPECT_CALL(*mockMerger, EndMerge(_, _)).Times(1);
        ASSERT_TRUE(_mergeTask.doMerge(indexPartitionMerger, mergeStep, true, pid, targetIndexPath));
    }
}

TEST_F(MergeTaskTest, TestInitOptions) {
    {
        TaskBase taskBase;
        _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_options_config";
        _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
        _mergeTask._kvMap[GENERATION_ID] = "5";
        _mergeTask._kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
        _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_FULL;
        _mergeTask._kvMap[NEED_PARTITION] = "false";
        _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
        _mergeTask._kvMap[OPERATION_IDS] = "1,2,3,4";
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        ASSERT_FALSE(taskBase._mergeIndexPartitionOptions.IsOnline());
        ASSERT_THAT(taskBase._mergeIndexPartitionOptions.GetOngoingModifyOperationIds(),
                    ElementsAre(1,2,3,4));
    }
    {
        string indexRoot = GET_TEST_DATA_PATH();
        TaskBase taskBase;
        _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_options_config";
        _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
        _mergeTask._kvMap[GENERATION_ID] = "5";
        _mergeTask._kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
        _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_FULL;
        _mergeTask._kvMap[NEED_PARTITION] = "false";
        _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
        string docReclaimConfigPath = IndexPathConstructor::constructGenerationPath(
                indexRoot, "cluster1", 5) + "/" + IndexPathConstructor::DOC_RECLAIM_DATA_DIR + 
                "/" + BS_DOC_RECLAIM_CONF_FILE;
        ASSERT_TRUE(FileUtil::writeFile(docReclaimConfigPath, ""));
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        ASSERT_FALSE(taskBase._mergeIndexPartitionOptions.IsOnline());
        ASSERT_EQ(string(""),
                  taskBase._mergeIndexPartitionOptions.GetMergeConfig().docReclaimConfigPath);
    }
}

TEST_F(MergeTaskTest, TestInitOptionsWithDocReclaim) {
    string indexRoot = GET_TEST_DATA_PATH();
    {
        TaskBase taskBase;
        _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_with_doc_reclaim";
        _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
        _mergeTask._kvMap[GENERATION_ID] = "5";
        _mergeTask._kvMap[INDEX_ROOT_PATH] = indexRoot;
        _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
        _mergeTask._kvMap[MERGE_CONFIG_NAME] = "inc";
        _mergeTask._kvMap[NEED_PARTITION] = "false";
        _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
        string docReclaimConfigPath = IndexPathConstructor::constructGenerationPath(
                indexRoot, "cluster1", 5) + IndexPathConstructor::DOC_RECLAIM_DATA_DIR + 
                "/" +BS_DOC_RECLAIM_CONF_FILE;
        ASSERT_TRUE(FileUtil::writeFile(docReclaimConfigPath, ""));
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        ASSERT_FALSE(taskBase._mergeIndexPartitionOptions.IsOnline());
        ASSERT_EQ(docReclaimConfigPath, 
                  taskBase._mergeIndexPartitionOptions.GetMergeConfig().docReclaimConfigPath);
    }
    {
        TaskBase taskBase;
        _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_with_doc_reclaim";
        _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
        _mergeTask._kvMap[GENERATION_ID] = "5";
        _mergeTask._kvMap[INDEX_ROOT_PATH] = indexRoot;
        _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
        _mergeTask._kvMap[MERGE_CONFIG_NAME] = "inc";
        _mergeTask._kvMap[NEED_PARTITION] = "false";
        _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
        string docReclaimConfigPath = IndexPathConstructor::constructGenerationPath(
                indexRoot, "cluster1", 5) + "/" + IndexPathConstructor::DOC_RECLAIM_DATA_DIR 
                + "/"+ BS_DOC_RECLAIM_CONF_FILE;
        ASSERT_TRUE(FileUtil::remove(docReclaimConfigPath));
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        ASSERT_FALSE(taskBase._mergeIndexPartitionOptions.IsOnline());
        ASSERT_EQ("test.json", 
                  taskBase._mergeIndexPartitionOptions.GetMergeConfig().docReclaimConfigPath);
    }

    {
        TaskBase taskBase;
        _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_with_doc_reclaim";
        _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
        _mergeTask._kvMap[GENERATION_ID] = "error_generation";
        _mergeTask._kvMap[INDEX_ROOT_PATH] = indexRoot;
        _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
        _mergeTask._kvMap[MERGE_CONFIG_NAME] = "inc";
        _mergeTask._kvMap[NEED_PARTITION] = "false";
        _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_FALSE(taskBase.init(jobParam));
    }
}

TEST_F(MergeTaskTest, TestInitOptionsWithReservedCheckpoints) {
    _mergeTask._kvMap[CONFIG_PATH] = TEST_DATA_PATH"/build_task_base_test/init_options_config";
    _mergeTask._kvMap[CLUSTER_NAMES] = "cluster1";
    _mergeTask._kvMap[GENERATION_ID] = "5";
    _mergeTask._kvMap[INDEX_ROOT_PATH] = GET_TEST_DATA_PATH();
    _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_FULL;
    _mergeTask._kvMap[NEED_PARTITION] = "false";
    _mergeTask._kvMap[SRC_SIGNATURE] = "1234";
    {
        TaskBase taskBase;
        //prepare checkpoints
        CheckpointList ccpList;
        ccpList.add(1);
        ccpList.add(3);
        ccpList.add(5);
        string ccpJsonStr = ToJsonString(ccpList);
        _mergeTask._kvMap[RESERVED_CLUSTER_CHECKPOINT_LIST] = ccpJsonStr;
    
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        ASSERT_EQ(ccpList.getIdSet(), taskBase._mergeIndexPartitionOptions.GetReservedVersions());
    }
    {
        TaskBase taskBase;
        _mergeTask._kvMap[RESERVED_VERSION_LIST] = "1 2 3";
        string jobParam = ToJsonString(_mergeTask._kvMap);
        ASSERT_TRUE(taskBase.init(jobParam));
        set<versionid_t> reservedVersions = {1,2,3};
        ASSERT_EQ(reservedVersions, taskBase._mergeIndexPartitionOptions.GetReservedVersions());
    }
}


TEST_F(MergeTaskTest, TestCreateMergerWithIndexPlugins) {
    string configPath = TEST_DATA_PATH"/mergeTaskTest/";
    _mergeTask._resourceReader.reset(new ResourceReader(configPath));
    _mergeTask._metricProvider = NULL;
    _mergeTask._kvMap[BUILD_MODE] = BUILD_MODE_INCREMENTAL;
    _mergeTask._jobConfig._clusterName = "simple";
    string targetIndexPath = TEST_DATA_PATH "/mergeTaskTest/schemas/";
    proto::PartitionId pid;
    IndexPartitionMergerPtr indexMerger = _mergeTask.createSinglePartMerger(pid, targetIndexPath, IndexPartitionOptions());
    ASSERT_TRUE(indexMerger);
}

TEST_F(MergeTaskTest, testGetMergeStep) {
    //parallel merge
    {
        setConfig(1, 2);
        uint32_t instanceId = 11;
        ASSERT_EQ(make_pair(MergeTask::MS_INIT_MERGE, uint32_t(0)), 
                  _mergeTask.getMergeStep(TaskBase::MERGE_MAP, instanceId));
        ASSERT_EQ(make_pair(MergeTask::MS_DO_MERGE, uint32_t(1)), 
                  _mergeTask.getMergeStep(TaskBase::MERGE_REDUCE, instanceId));
        ASSERT_EQ(make_pair(MergeTask::MS_END_MERGE, uint32_t(0)), 
                  _mergeTask.getMergeStep(TaskBase::END_MERGE_MAP, instanceId));

        instanceId = 11;
        ASSERT_EQ(make_pair(MergeTask::MS_DO_MERGE, uint32_t(1)), 
                  _mergeTask.getMergeStep(TaskBase::MERGE_REDUCE, instanceId));

        instanceId = 11;
        ASSERT_EQ(make_pair(MergeTask::MS_END_MERGE, uint32_t(0)), 
                  _mergeTask.getMergeStep(TaskBase::END_MERGE_MAP, instanceId));

        instanceId = 11;
        ASSERT_EQ(make_pair(MergeTask::MS_DO_NOTHING, uint32_t(0)), 
                  _mergeTask.getMergeStep(TaskBase::END_MERGE_REDUCE, instanceId));
    }
}

TEST_F(MergeTaskTest, testIsMultiPartMerge) {
    {
        setConfig(1, 1, 1);
        ASSERT_FALSE(_mergeTask.isMultiPartMerge());
    }
    {
        setConfig(4, 1, 2);
        ASSERT_TRUE(_mergeTask.isMultiPartMerge());
    }
    {
        setConfig(2, 1, 2);
        ASSERT_FALSE(_mergeTask.isMultiPartMerge());
    }
    {
        setConfig(2, 1, 1);
        ASSERT_TRUE(_mergeTask.isMultiPartMerge());
    }
    {
        _mergeTask._kvMap[INDEX_ROOT_PATH] = "hdfs://root1/";
        _mergeTask._kvMap[BUILDER_INDEX_ROOT_PATH] = "hdfs://root2/";
        setConfig(1, 1, 1);
        ASSERT_TRUE(_mergeTask.isMultiPartMerge());
    }
}

TEST_F(MergeTaskTest, testRemoveObsoleteMergeMetaNotRemove) {
    testRemoveObsoleteMergeMeta(5, 3);
}

TEST_F(MergeTaskTest, testRemoveObsoleteMergeMetaNormal) {
    testRemoveObsoleteMergeMeta(5, 7);
}

TEST_F(MergeTaskTest, testRemoveObsoleteMergeMetaEqual) {
    testRemoveObsoleteMergeMeta(5, 5);
}

void MergeTaskTest::testRemoveObsoleteMergeMeta(
        uint32_t keepCount, uint32_t totalCount)
{
    string rootPath = GET_TEST_DATA_PATH() + "/merge_meta";
    createDir(rootPath, totalCount);
    MergeTask::removeObsoleteMergeMeta(rootPath, keepCount);
    checkDirCount(rootPath, keepCount, totalCount);

}

void MergeTaskTest::createDir(const string &path, uint32_t count) {
    for (size_t i = 0; i < count; i++) {
        string dirName = "testDir" + StringUtil::toString(i);
        ASSERT_TRUE(FileUtil::mkDir(FileUtil::joinFilePath(path, dirName),true));
    }
}

void MergeTaskTest::checkDirCount(
        const string &path, uint32_t keepCount, uint32_t totalCount)
{
    vector<string> fileList;
    ASSERT_TRUE(FileUtil::listDir(path, fileList));
    uint32_t expectFileCount = min(keepCount, totalCount);
    ASSERT_EQ(expectFileCount, fileList.size());
    sort(fileList.begin(), fileList.end());

    for (uint32_t i = 0; i < expectFileCount; ++i) {
        uint32_t keepFileNum = i + (totalCount > keepCount ? totalCount - keepCount : 0);
        string dirName = "testDir" + StringUtil::toString(keepFileNum);
        EXPECT_EQ(dirName, fileList[i]);
    }
}

}
}
