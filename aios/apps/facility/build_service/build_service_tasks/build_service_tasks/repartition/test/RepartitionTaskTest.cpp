#include "build_service_tasks/repartition/RepartitionTask.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service_tasks/repartition/test/FakeRepartitionTask.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/EpochIdUtil.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;
using namespace build_service::config;
using namespace autil;
using indexlib::file_system::Directory;
using indexlib::file_system::DirectoryPtr;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::testlib;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace build_service { namespace task_base {

class MockRepartitionTask : public RepartitionTask
{
public:
    MockRepartitionTask() {}
    virtual ~MockRepartitionTask() {}

public:
    MOCK_METHOD3(prepareFilteredMultiPartitionMerger,
                 indexlib::merger::FilteredMultiPartitionMergerPtr(versionid_t, const std::string&, uint32_t));
};

class RepartitionTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void checkHighFreqTermCount(const std::string& generationId, const std::string& partitionRange,
                                const std::string& indexName, int32_t expectCount);
    Task::TaskInitParam prepareTaskInitParam(int32_t originalGenerationId, int32_t partitionCount, int32_t partitionId,
                                             int32_t instanceCount, int32_t instanceId);
    void replaceFile(const string& filePath, const string& strToReplace, const string& replaceStr);
    void checkDocs(const string& query, int32_t generationId, const string& partName, vector<int> nids);
    void prepareConfig(const std::string& indexPath, const std::string& originalPartitionCount);
    void prepareTargets(const std::string& version, const std::string& repartitionPath,
                        const std::string& partitionCount, const std::string& parallelNum,
                        const std::string& targetGenerationId);
    void prepareIndex(int32_t originalGenerationId, int32_t originalPartitionCount, int32_t originalVersionId,
                      int32_t targetGenerationId, int32_t targetPartitionCount, int32_t parallelNum);
    void innerTestWithException(int32_t exceptionStep);

private:
    string _configPath;
    string _originalIndexRoot;
    IndexPartitionSchemaPtr _schema;
    vector<config::TaskTarget> _targets;
    config::ResourceReaderPtr _resourceReader;
};

void RepartitionTaskTest::replaceFile(const string& filePath, const string& strToReplace, const string& replaceStr)
{
    string content;
    fslib::util::FileUtil::readFile(filePath, content);
    auto pos = content.find(strToReplace);
    while (pos != string::npos) {
        content = content.replace(pos, strToReplace.size(), replaceStr);
        pos = content.find(strToReplace);
    }
    fslib::util::FileUtil::remove(filePath);
    fslib::util::FileUtil::writeFile(filePath, content);
}

void RepartitionTaskTest::checkDocs(const string& query, int32_t generationId, const string& partName,
                                    vector<int32_t> nids)
{
    string partPath =
        GET_TEMP_DATA_PATH() + "/cluster1/generation_" + StringUtil::toString(generationId) + "/" + partName + "/";
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetBuildConfig(false).keepVersionCount = 10;
    ASSERT_TRUE(psm.Init(_schema, options, partPath));
    stringstream result;
    for (size_t i = 0; i < nids.size(); i++) {
        result << "nid=" << nids[i];
        if (i == nids.size() - 1) {
            break;
        }
        result << ";";
    }
    ASSERT_TRUE(psm.Transfer(QUERY, "", query, result.str())) << result.str();
}

void RepartitionTaskTest::prepareConfig(const string& indexPath, const std::string& originalPartitionCount)
{
    string srcConfigPath = GET_TEST_DATA_PATH() + "/config_repartition";
    _configPath = GET_TEMP_DATA_PATH() + "/config_repartition";
    fslib::util::FileUtil::removeIfExist(_configPath);
    fslib::util::FileUtil::atomicCopy(srcConfigPath, _configPath);
    string buildAppPath = _configPath + "/build_app.json";
    string originalIndexPath = "hdfs://index/root/";
    replaceFile(buildAppPath, originalIndexPath, indexPath);

    string clusterPath = _configPath + "/clusters/cluster1_cluster.json";
    replaceFile(clusterPath, "part_count", originalPartitionCount);
    _resourceReader.reset(new ResourceReader(_configPath));
}

void RepartitionTaskTest::prepareTargets(const std::string& version, const std::string& repartitionPath,
                                         const std::string& partitionCount, const std::string& parallelNum,
                                         const string& targetGenerationId)
{
    _targets.clear();
    string taskJsonPath = _configPath + "/clusters/repartition_task.json";
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    string timestampStr = StringUtil::toString(curTs);
    config::TaskTarget beginTarget("begin_task");
    beginTarget.addTargetDescription(BS_SNAPSHOT_VERSION, version);
    beginTarget.addTargetDescription("indexPath", repartitionPath);
    beginTarget.addTargetDescription("parallelNum", parallelNum);
    beginTarget.addTargetDescription("generationId", targetGenerationId);
    beginTarget.addTargetDescription("timestamp", timestampStr);
    beginTarget.addTaskConfigPath(taskJsonPath);
    _targets.push_back(beginTarget);

    config::TaskTarget doTarget("do_task");
    doTarget.addTargetDescription(BS_SNAPSHOT_VERSION, version);
    doTarget.addTaskConfigPath(taskJsonPath);
    doTarget.addTargetDescription("timestamp", timestampStr);
    doTarget.addTargetDescription("indexPath", repartitionPath);
    doTarget.addTargetDescription("parallelNum", parallelNum);
    doTarget.addTargetDescription("generationId", targetGenerationId);
    _targets.push_back(doTarget);

    config::TaskTarget endTarget("end_task");
    endTarget.addTargetDescription(BS_SNAPSHOT_VERSION, version);
    endTarget.addTaskConfigPath(taskJsonPath);
    endTarget.addTargetDescription("timestamp", timestampStr);
    endTarget.addTargetDescription("indexPath", repartitionPath);
    endTarget.addTargetDescription("parallelNum", parallelNum);
    endTarget.addTargetDescription("generationId", targetGenerationId);
    _targets.push_back(endTarget);
}

void RepartitionTaskTest::setUp()
{
    string configPath = GET_TEST_DATA_PATH() + "/config_repartition/";
    string schemaPath = configPath + "/schemas/";
    _schema = SchemaAdapter::TEST_LoadSchema(PathUtil::JoinPath(schemaPath, "mainse_searcher_schema.json"));
    _originalIndexRoot = GET_TEMP_DATA_PATH() + "/cluster1/generation_1/partition_0_65535";
    string docs = "cmd=add,name=doc,tag=tag1,price=10,nid=1;"
                  "cmd=add,name=doc,tag=tag2,price=12,nid=15000;"
                  "cmd=add,name=doc,tag=tag3,price=13,nid=25000;"
                  "cmd=add,name=doc,tag=tag3,price=14,nid=40000;"
                  "cmd=add,name=doc,tag=tag3,price=15,nid=65535;";

    IndexPartitionOptions options;
    IndexlibPartitionCreator::CreateIndexPartition(_schema, _originalIndexRoot, docs, options, "", false);

    PartitionStateMachine psm;
    options.GetBuildConfig(true).keepVersionCount = 10;
    options.GetBuildConfig(false).keepVersionCount = 10;
    ASSERT_TRUE(psm.Init(_schema, options, _originalIndexRoot));
    string incDocs = "cmd=add,name=doc,tag=tag1,price=9,nid=2;"
                     "cmd=add,name=doc,tag=tag1,price=11,nid=30000;"
                     "cmd=add,name=doc,tag=tag2,price=16,nid=65534;"
                     "cmd=delete,nid=1;";
    psm.Transfer(BUILD_INC, incDocs, "", "");
}

void RepartitionTaskTest::tearDown() {}

void RepartitionTaskTest::prepareIndex(int32_t originalGenerationId, int32_t originalPartitionCount,
                                       int32_t originalVersionId, int32_t targetGenerationId,
                                       int32_t targetPartitionCount, int32_t parallelNum)
{
    std::string originalIndexPath = GET_TEMP_DATA_PATH();
    std::string targetIndexPath = GET_TEMP_DATA_PATH();
    string partitionCountStr = StringUtil::toString(targetPartitionCount);
    string originalPartitionCountStr = StringUtil::toString(originalPartitionCount);
    string parallelNumStr = StringUtil::toString(parallelNum);
    prepareConfig(originalIndexPath, originalPartitionCountStr);
    prepareTargets(std::to_string(originalVersionId), targetIndexPath, partitionCountStr, parallelNumStr,
                   StringUtil::toString(targetGenerationId));

    for (int32_t i = 0; i < targetPartitionCount; i++) {
        // begin merge
        cout << "begin merge" << endl;
        RepartitionTask task;
        Task::TaskInitParam param = prepareTaskInitParam(originalGenerationId, targetPartitionCount, i, 1, 0);
        ASSERT_TRUE(task.init(param));
        ASSERT_TRUE(task.handleTarget(_targets[0]));

        // check checkpoint in use
        MockRepartitionTask mockTask;
        ASSERT_TRUE(mockTask.init(param));
        EXPECT_CALL(mockTask, prepareFilteredMultiPartitionMerger(_, _, _)).Times(0);
        mockTask.handleTarget(_targets[0]);
        // do merge
        cout << "do merge" << endl;
        for (int32_t j = 0; j < parallelNum; j++) {
            RepartitionTask task;
            Task::TaskInitParam param =
                prepareTaskInitParam(originalGenerationId, targetPartitionCount, i, parallelNum, j);
            ASSERT_TRUE(task.init(param));
            task.handleTarget(_targets[1]);
            // check checkpoint in use
            MockRepartitionTask mockTask;
            ASSERT_TRUE(mockTask.init(param));
            EXPECT_CALL(mockTask, prepareFilteredMultiPartitionMerger(_, _, _)).Times(0);
            mockTask.handleTarget(_targets[1]);
        }
        // end merger
        cout << "end merge" << endl;
        RepartitionTask task2;
        param = prepareTaskInitParam(originalGenerationId, targetPartitionCount, i, 1, 0);
        ASSERT_TRUE(task2.init(param));
        task2.handleTarget(_targets[2]);
        // check checkpoint in use

        MockRepartitionTask mockTask1;
        ASSERT_TRUE(mockTask1.init(param));
        EXPECT_CALL(mockTask1, prepareFilteredMultiPartitionMerger(_, _, _)).Times(0);
        mockTask1.handleTarget(_targets[2]);
    }
}

Task::TaskInitParam RepartitionTaskTest::prepareTaskInitParam(int32_t originalGenerationId, int32_t partitionCount,
                                                              int32_t partitionId, int32_t instanceCount,
                                                              int32_t instanceId)
{
    Task::TaskInitParam param;
    param.instanceInfo.partitionCount = partitionCount;
    param.instanceInfo.partitionId = partitionId;
    param.instanceInfo.instanceCount = instanceCount;
    param.instanceInfo.instanceId = instanceId;
    param.buildId.appName = "repartition";
    param.buildId.dataTable = "simple";
    param.buildId.generationId = originalGenerationId;
    param.clusterName = "cluster1";
    param.resourceReader = _resourceReader;
    param.epochId = EpochIdUtil::TEST_GenerateEpochId();
    return param;
}

TEST_F(RepartitionTaskTest, testBaseSegmentIdNot0)
{
    prepareIndex(1, 1, 2, 2, 2, 1);
    checkDocs("name:doc", 2, "partition_0_32767", {15000, 25000, 2, 30000});
    checkDocs("name:doc", 2, "partition_32768_65535", {40000, 65535, 65534});
}

TEST_F(RepartitionTaskTest, testSimple)
{
    // generation_1: 1 part: version 0 => generation_2: 2 part, 1 parallel
    string generationPath = GET_TEMP_DATA_PATH() + "/cluster1/generation_1/";
    PathMeta beforeMeta;
    auto ret = FileSystem::getPathMeta(generationPath, beforeMeta);
    ASSERT_EQ(EC_OK, ret);
    prepareIndex(1, 1, 0, 2, 2, 1);
    PathMeta afterMeta;
    ret = FileSystem::getPathMeta(generationPath, afterMeta);
    ASSERT_EQ(EC_OK, ret);
    ASSERT_EQ(beforeMeta.lastModifyTime, afterMeta.lastModifyTime);
    checkDocs("name:doc", 2, "partition_0_32767", {1, 15000, 25000});
    checkDocs("name:doc", 2, "partition_32768_65535", {40000, 65535});
    // generation_2: 2 part: version 0 => generation_3: 3 part, 2 parallel
    prepareIndex(2, 2, 0, 3, 3, 2);
    checkDocs("name:doc", 3, "partition_0_21845", {1, 15000});
    checkDocs("name:doc", 3, "partition_21846_43690", {25000, 40000});
    checkDocs("name:doc", 3, "partition_43691_65535", {65535});

    generationPath = GET_TEMP_DATA_PATH() + "/cluster1/generation_3/";
    ret = FileSystem::getPathMeta(generationPath, beforeMeta);
    ASSERT_EQ(EC_OK, ret);

    // generation_3: 3 part: version 0 => generation_4: 3 part, 1 parallel
    prepareIndex(3, 3, 0, 4, 3, 1);
    checkDocs("name:doc", 4, "partition_0_21845", {1, 15000});
    checkDocs("name:doc", 4, "partition_21846_43690", {25000, 40000});
    checkDocs("name:doc", 4, "partition_43691_65535", {65535});
    // generation_3: 3 part: version 0 => generation_5: 6 part, 20 parallel
    prepareIndex(3, 3, 0, 5, 6, 20);
    checkDocs("name:doc", 5, "partition_0_10922", {1});
    checkDocs("name:doc", 5, "partition_10923_21845", {15000});
    checkDocs("name:doc", 5, "partition_21846_32768", {25000});
    checkDocs("name:doc", 5, "partition_32769_43691", {40000});
    checkDocs("name:doc", 5, "partition_43692_54613", {});
    checkDocs("name:doc", 5, "partition_54614_65535", {65535});

    ret = FileSystem::getPathMeta(generationPath, afterMeta);
    ASSERT_EQ(EC_OK, ret);
    ASSERT_EQ(beforeMeta.lastModifyTime, afterMeta.lastModifyTime);
}

void RepartitionTaskTest::innerTestWithException(int32_t exceptionStep)
{
    fslib::util::FileUtil::removeIfExist(GET_TEMP_DATA_PATH());
    tearDown();
    setUp();
    prepareIndex(1, 1, 0, 2, 2, 1);
    bool hasException = false;
    std::string originalIndexPath = GET_TEMP_DATA_PATH();
    std::string targetIndexPath = GET_TEMP_DATA_PATH();
    string partitionCountStr = "1";
    string originalPartitionCountStr = "2";
    string parallelNumStr = "1";

    prepareConfig(originalIndexPath, originalPartitionCountStr);

    prepareTargets("0", targetIndexPath, partitionCountStr, parallelNumStr, "3");
    // begin merge
    cout << "begin merge" << endl;
    FakeRepartitionTask task(exceptionStep);
    Task::TaskInitParam param = prepareTaskInitParam(2, 1, 0, 1, 0);
    ASSERT_TRUE(task.init(param));

    while (!task.isDone(_targets[0])) {
        if (!task.handleTarget(_targets[0])) {
            hasException = true;
        }
    }

    // do merge
    cout << "do merge" << endl;
    while (!task.isDone(_targets[1])) {
        if (!task.handleTarget(_targets[1])) {
            hasException = true;
        }
    }

    // end merger
    cout << "end merge" << endl;
    while (!task.isDone(_targets[2])) {
        if (!task.handleTarget(_targets[2])) {
            hasException = true;
        }
    }

    checkDocs("name:doc", 3, "partition_0_65535", {1, 15000, 25000, 40000, 65535});
    ASSERT_TRUE(hasException);
}

TEST_F(RepartitionTaskTest, testWithException)
{
    for (int32_t i = 0; i < 8; i++) {
        cout << "=============" << i << "=============" << endl;
        innerTestWithException(i);
    }
}

void RepartitionTaskTest::checkHighFreqTermCount(const std::string& generationId, const std::string& partitionRange,
                                                 const std::string& indexName, int32_t expectCount)
{
    string indexRoot = GET_TEMP_DATA_PATH() + "/cluster1/generation_" + generationId + "/" + partitionRange;
    DirectoryPtr indexRootPtr = Directory::GetPhysicalDirectory(indexRoot);
    IndexPartitionOptions options;
    auto schema = SchemaAdapter::LoadAndRewritePartitionSchema(indexRootPtr, options);
    auto vol = schema->GetIndexSchema()->GetIndexConfig(indexName)->GetHighFreqVocabulary();
    ASSERT_TRUE(vol);
    ASSERT_EQ(expectCount, vol->GetTermCount());
}

TEST_F(RepartitionTaskTest, testBitmapAndTruncateIndex)
{
    // generation_1: 1 part: version 0 => generation_2: 2 part, 1 parallel
    prepareIndex(1, 1, 0, 2, 2, 1);
    checkDocs("name:doc", 2, "partition_0_32767", {1, 15000, 25000});
    checkDocs("name_asc_price:doc", 2, "partition_0_32767", {1, 15000, 25000});
    checkDocs("name:doc", 2, "partition_32768_65535", {40000, 65535});
    checkDocs("name_asc_price:doc", 2, "partition_32768_65535", {});

    checkHighFreqTermCount("2", "partition_0_32767", "name", 1);
    checkHighFreqTermCount("2", "partition_32768_65535", "name", 0);
    checkHighFreqTermCount("2", "partition_0_32767", "tag", 2);
    checkHighFreqTermCount("2", "partition_32768_65535", "tag", 2);

    // generation_2: 2 part: version 0 => generation_3: 1 part, 1 parallel
    prepareIndex(2, 2, 0, 3, 1, 1);
    checkDocs("name:doc", 3, "partition_0_65535", {1, 15000, 25000, 40000, 65535});
    checkDocs("tag:tag1", 3, "partition_0_65535", {1});
    checkDocs("tag:tag3", 3, "partition_0_65535", {25000, 40000, 65535});
    checkDocs("tag_asc_price:tag3", 3, "partition_0_65535", {25000, 40000, 65535});
    checkDocs("tag_asc_price:tag1", 3, "partition_0_65535", {});
    checkDocs("name_asc_price:doc", 3, "partition_0_65535", {1, 15000, 25000, 40000, 65535});

    checkHighFreqTermCount("3", "partition_0_65535", "name", 1);
    checkHighFreqTermCount("3", "partition_0_65535", "tag", 3);
}

}} // namespace build_service::task_base
