
#include "build_service_tasks/endbuild/EndBuildTask.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/io/Input.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/module_info.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace build_service::util;
using namespace build_service::config;
using namespace autil::legacy;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::merger;
using namespace indexlib::test;
using namespace indexlib::util;
namespace build_service { namespace task_base {

class EndBuildTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    uint32_t workerPathVersion = 11;
    proto::PartitionId _pid;
    string _indexRoot;
    uint32_t _parallelNum = 3;
};

void EndBuildTaskTest::setUp()
{
    _pid.mutable_range()->set_from(0);
    _pid.mutable_range()->set_to(65535);
    _pid.mutable_buildid()->set_generationid(1234);
    _pid.mutable_buildid()->set_appname("simple");
    _pid.mutable_buildid()->set_datatable("simple");
    *_pid.add_clusternames() = "cluster1";

    string jsonStr;
    string srcDataPath = GET_TEST_DATA_PATH() + "endbuild_test/config";
    string tmpDataPath = GET_TEMP_DATA_PATH() + "endbuild_test/config";
    fslib::util::FileUtil::readFile(srcDataPath + "/build_app.json.template", jsonStr);
    BuildServiceConfig BSConfig;
    FromJsonString(BSConfig, jsonStr);
    BSConfig._indexRoot = GET_TEMP_DATA_PATH();
    _indexRoot = IndexPathConstructor::constructIndexPath(BSConfig._indexRoot, _pid);
    fslib::util::FileUtil::mkDir(_indexRoot, true);
    jsonStr = ToJsonString(BSConfig);
    fslib::util::FileUtil::atomicCopy(srcDataPath, tmpDataPath);
    fslib::util::FileUtil::writeFile(tmpDataPath + "/build_app.json", jsonStr);

    // prepare data
    Version baseVersion;
    baseVersion.AddSegment(1);
    baseVersion.AddSegment(2);
    DirectoryPtr indexDir = DirectoryCreator::Create(_indexRoot, true);
    VersionMaker::MakeIncSegment(indexDir, 1, true);
    VersionMaker::MakeIncSegment(indexDir, 2, true);
    baseVersion.SetVersionId(1);
    baseVersion.Store(indexDir, false);
    {
        // ins0
        ParallelBuildInfo info(_parallelNum, workerPathVersion, 0);
        info.SetBaseVersion(baseVersion.GetVersionId());
        string instanceDir = info.GetParallelInstancePath(_indexRoot);
        fslib::util::FileUtil::mkDir(instanceDir, true);
        DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceDir, true);
        info.StoreIfNotExist(instanceRootDir);
        Version version;
        version.AddSegment(4);
        VersionMaker::MakeIncSegment(instanceRootDir, 4, true);
        version.SetVersionId(3);
        version.Store(instanceRootDir, false);
    }
    {
        // ins1
        ParallelBuildInfo info(_parallelNum, workerPathVersion, 1);
        info.SetBaseVersion(baseVersion.GetVersionId());
        string instanceDir = info.GetParallelInstancePath(_indexRoot);
        fslib::util::FileUtil::mkDir(instanceDir, true);
        DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceDir, true);
        info.StoreIfNotExist(instanceRootDir);
        Version version;
        version.AddSegment(3);
        version.AddSegment(5);
        VersionMaker::MakeIncSegment(instanceRootDir, 5, true);
        VersionMaker::MakeIncSegment(instanceRootDir, 3, true);
        version.SetVersionId(4);
        version.Store(instanceRootDir, false);
    }
    {
        // ins2
        ParallelBuildInfo info(_parallelNum, workerPathVersion, 2);
        info.SetBaseVersion(baseVersion.GetVersionId());
        string instanceDir = info.GetParallelInstancePath(_indexRoot);
        fslib::util::FileUtil::mkDir(instanceDir, true);
        DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceDir, true);
        info.StoreIfNotExist(instanceRootDir);
    }
}

void EndBuildTaskTest::tearDown() {}

TEST_F(EndBuildTaskTest, testSimple)
{
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "endbuild_test/config/"));
    Task::TaskInitParam param;
    param.buildId.appName = _pid.buildid().appname();
    param.buildId.generationId = _pid.buildid().generationid();
    param.buildId.dataTable = _pid.buildid().datatable();
    param.clusterName = "cluster1";
    param.resourceReader = resourceReader;
    param.metricProvider = nullptr;
    param.pid = _pid;
    param.epochId = EpochIdUtil::TEST_GenerateEpochId();

    EndBuildTask task;
    config::TaskTarget target;
    target.setTargetTimestamp(123);
    // before init
    ASSERT_FALSE(task.handleTarget(target));
    ASSERT_TRUE(task.init(param));
    // lack work path version
    ASSERT_FALSE(task.handleTarget(target));
    target.addTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, StringUtil::toString(workerPathVersion));
    target.addTargetDescription(OPERATION_IDS, "1,2,3");
    target.addTargetDescription(BATCH_MASK, "5");
    // lack build parallel num
    ASSERT_FALSE(task.handleTarget(target));
    target.addTargetDescription(BUILD_PARALLEL_NUM, StringUtil::toString(_parallelNum));

    ParallelBuildInfo info(_parallelNum, workerPathVersion, 0);
    string parallelBuildDir = info.GetParallelPath(_indexRoot);
    string ckpFile = FslibWrapper::JoinPath(parallelBuildDir, EndBuildTask::CHECKPOINT_NAME) + ".123";
    ASSERT_TRUE(task.handleTarget(target));
    string batchMask;
    task._options.GetVersionDesc(BATCH_MASK, batchMask);
    ASSERT_EQ("5", batchMask);

    ASSERT_EQ("2", task._kvMap[BS_ENDBUILD_VERSION]);
    ASSERT_THAT(task._options.GetOngoingModifyOperationIds(), ElementsAre(1, 2, 3));
    string generationId;
    task._options.GetVersionDesc(GENERATION_ID, generationId);
    ASSERT_EQ("1234", generationId);
    // clear dir when handle done
    ASSERT_FALSE(FslibWrapper::IsExist(parallelBuildDir).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(ckpFile).GetOrThrow());
    // run second time, will not generate a new version
    ASSERT_TRUE(task.handleTarget(target));
    ASSERT_EQ("2", task._kvMap[BS_ENDBUILD_VERSION]);
    ASSERT_EQ(target, task._currentFinishTarget);
    ASSERT_TRUE(task._finished);

    // when receive new target, restart
    TaskTarget target2 = target;
    target2.setTargetTimestamp(1234);
    ASSERT_FALSE(task.handleTarget(target2));

    // mock failure over, and receive new target
    EndBuildTask task3;
    ASSERT_TRUE(task3.init(param));
    ASSERT_TRUE(task3.handleTarget(target2));
    ASSERT_TRUE(task3._finished);
    ASSERT_EQ(1234, task3._currentFinishTarget.getTargetTimestamp());
    ckpFile = FslibWrapper::JoinPath(parallelBuildDir, EndBuildTask::CHECKPOINT_NAME) + ".1234";
    ASSERT_FALSE(FslibWrapper::IsExist(ckpFile).GetOrThrow());
}

TEST_F(EndBuildTaskTest, testHandleTargetTwice)
{
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEMP_DATA_PATH() + "/endbuild_test/config/"));
    Task::TaskInitParam param;
    param.buildId.appName = _pid.buildid().appname();
    param.buildId.generationId = _pid.buildid().generationid();
    param.buildId.dataTable = _pid.buildid().datatable();
    param.clusterName = "cluster1";
    param.resourceReader = resourceReader;
    param.metricProvider = nullptr;
    param.pid = _pid;
    param.epochId = EpochIdUtil::TEST_GenerateEpochId();

    config::TaskTarget target;
    target.setTargetTimestamp(123);
    target.addTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, StringUtil::toString(workerPathVersion));
    target.addTargetDescription(OPERATION_IDS, "1,2,3");
    target.addTargetDescription(BUILD_PARALLEL_NUM, StringUtil::toString(_parallelNum));
    ParallelBuildInfo info(_parallelNum, workerPathVersion, 0);
    string parallelBuildDir = info.GetParallelPath(_indexRoot);
    string ckpFile = FslibWrapper::JoinPath(parallelBuildDir, EndBuildTask::CHECKPOINT_NAME);

    {
        EndBuildTask* task = new EndBuildTask();
        ASSERT_TRUE(task->init(param));
        ASSERT_TRUE(task->handleTarget(target));
        ASSERT_TRUE(task->_finished);
        ASSERT_EQ(target, task->_currentFinishTarget);
        ASSERT_EQ("2", task->_kvMap[BS_ENDBUILD_VERSION]);
        ASSERT_THAT(task->_options.GetOngoingModifyOperationIds(), ElementsAre(1, 2, 3));
        ParallelBuildInfo info(_parallelNum, workerPathVersion, 1);
        string parallelBuildDir = info.GetParallelPath(_indexRoot);
        bool exist = false;
        ASSERT_TRUE(fslib::util::FileUtil::isExist(parallelBuildDir, exist));
        ASSERT_FALSE(exist);
        delete task;
    }
    {
        // handle again
        EndBuildTask* task = new EndBuildTask();
        ASSERT_TRUE(task->init(param));
        ASSERT_TRUE(task->handleTarget(target));
        ASSERT_TRUE(task->_finished);
        ASSERT_EQ(target, task->_currentFinishTarget);
        ASSERT_EQ("2", task->_kvMap[BS_ENDBUILD_VERSION]);
        ASSERT_THAT(task->_options.GetOngoingModifyOperationIds(), ElementsAre(1, 2, 3));
        delete task;
    }
}

}} // namespace build_service::task_base
