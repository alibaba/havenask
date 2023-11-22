
#include "build_service_tasks/script_task/RunScriptTask.h"

#include <assert.h>
#include <cstddef>
#include <deque>
#include <memory>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ScriptTaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/script_task/ScriptExecutor.h"
#include "build_service_tasks/script_task/test/FakeScriptExecutor.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace task_base {

class RunScriptTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    config::TaskTarget prepareTarget();
    Task::TaskInitParam prepareTaskInitParam(int32_t nodeId);
    RunScriptTaskPtr prepareTask(const string& configFileName, int32_t nodeId);
    void innerTest(const string& configFileName, const string& scriptType, int maxRetryTime, int32_t nodeId);
    void innerTestFail(const string& configFileName, int32_t nodeId, int expectedErrorCode);
    void innerTestActionWhenFailed(const string& configFileName, int32_t nodeId, const string& action);

private:
    string _configPath;
    string _binaryPath;
    config::ResourceReaderPtr _resourceReader;
    config::ScriptTaskConfig _scriptConfig;
    string _content;

private:
    BS_LOG_DECLARE();
};
BS_LOG_SETUP(task_base, RunScriptTaskTest);

void RunScriptTaskTest::setUp()
{
    // we will store config in tmp dir (_config) because we need write something to config
    // and why we not use _config to indicate binary path is
    // that _config copied from testdata have no run authority
    _configPath = GET_TEMP_DATA_PATH() + "/config_task_scripts";
    FileSystem::copy(GET_TEST_DATA_PATH() + "/config_task_scripts", _configPath);
    _binaryPath = GET_TEST_DATA_PATH() + "/config_task_scripts/task_scripts/bin/";
    char buffer[1024];
    if (getcwd(buffer, sizeof(buffer)) == NULL) {
        assert(false);
    }
    string s(buffer);
    string logPath = GET_TEMP_DATA_PATH();
    if (logPath[0] != '/') {
        logPath = s + "/" + logPath;
    }
    // logPath += GET_TEMP_DATA_PATH();
    string content = "{\"log_dir\":\"" + logPath + "\"}";
    FileSystem::writeFile(_configPath + "/task_scripts/script_control_args.json", content);
    _resourceReader.reset(new config::ResourceReader(_configPath));
}

void RunScriptTaskTest::tearDown() {}

config::TaskTarget RunScriptTaskTest::prepareTarget()
{
    config::TaskTarget target("abc");
    target.setPartitionCount(_scriptConfig.partitionCount);
    target.setParallelNum(_scriptConfig.parallelNum);
    target.addTargetDescription("runScript", _content);
    return target;
}

Task::TaskInitParam RunScriptTaskTest::prepareTaskInitParam(int32_t nodeId)
{
    Task::TaskInitParam param;
    param.instanceInfo.partitionCount = _scriptConfig.partitionCount;
    param.instanceInfo.partitionId = nodeId / _scriptConfig.parallelNum;
    param.instanceInfo.instanceCount = _scriptConfig.parallelNum;
    param.instanceInfo.instanceId = nodeId % _scriptConfig.parallelNum;
    param.buildId.appName = "test_app";
    param.buildId.dataTable = "simple";
    param.buildId.generationId = 1;
    param.clusterName = "simple";
    param.resourceReader = _resourceReader;
    return param;
}

RunScriptTaskPtr RunScriptTaskTest::prepareTask(const string& configFileName, int32_t nodeId)
{
    string relativeFilePath = config::ResourceReader::getTaskScriptFileRelativePath(configFileName);
    _resourceReader->getConfigContent(relativeFilePath, _content);
    try {
        FromJsonString(_scriptConfig, _content);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "content[%s] fromJson error.", _content.c_str());
        assert(false);
    }

    RunScriptTaskPtr task(new RunScriptTask);
    FakeScriptExecutor* fakeExecutor = new FakeScriptExecutor();
    fakeExecutor->setBinaryPath(_binaryPath);
    task->_executor.reset(fakeExecutor);
    return task;
}

void RunScriptTaskTest::innerTestFail(const string& configFileName, int32_t nodeId, int expectedErrorCode)
{
    RunScriptTaskPtr task = prepareTask(configFileName, nodeId);
    ASSERT_TRUE(task);
    task->TEST_setDebugMode(true);
    Task::TaskInitParam initParam = prepareTaskInitParam(nodeId);
    ASSERT_TRUE(task->init(initParam));
    config::TaskTarget target = prepareTarget();
    task->handleTarget(target);
    task->workThread();
    auto errorInfos = task->_errorInfos;
    ASSERT_EQ(1, errorInfos.size());

    proto::ErrorInfo errorInfo = errorInfos.front();
    ASSERT_EQ(proto::SERVICE_TASK_ERROR, errorInfo.errorcode());
    string errorMsg = errorInfo.errormsg();
    string expected = "user error code [" + to_string(expectedErrorCode) + "]";
    ASSERT_NE(string::npos, errorMsg.find(expected));
}

void RunScriptTaskTest::innerTestActionWhenFailed(const string& configFileName, int32_t nodeId, const string& action)
{
    // given retry times should be 2
    RunScriptTaskPtr task = prepareTask(configFileName, nodeId);
    ASSERT_TRUE(task);
    task->TEST_setDebugMode(true);
    Task::TaskInitParam initParam = prepareTaskInitParam(nodeId);
    ASSERT_TRUE(task->init(initParam));
    config::TaskTarget target = prepareTarget();
    task->handleTarget(target);

    // do and retry 2 times
    task->workThread();
    ASSERT_FALSE(task->isDone(target));
    ASSERT_FALSE(task->TEST_isHanged());

    task->workThread();
    ASSERT_FALSE(task->isDone(target));
    ASSERT_FALSE(task->TEST_isHanged());

    task->workThread();

    auto errorInfos = task->_errorInfos;
    ASSERT_EQ(3, errorInfos.size());
    if (action == "hang") {
        ASSERT_TRUE(task->TEST_isHanged());
        ASSERT_FALSE(task->isDone(target));
        ASSERT_EQ(proto::BS_FATAL_ERROR, errorInfos.front().advice());
    } else if (action == "finish") {
        ASSERT_FALSE(task->TEST_isHanged());
        ASSERT_TRUE(task->isDone(target));
        ASSERT_EQ(proto::BS_RETRY, errorInfos.front().advice());
    } else {
        assert(false);
    }
}

TEST_F(RunScriptTaskTest, testShell) { innerTest("run_shell.json", "shell", 0, 3 /*nodeId*/); }

TEST_F(RunScriptTaskTest, testShellFail) { innerTestFail("run_shell_fail.json", 3, 255); }

TEST_F(RunScriptTaskTest, testShellFail_2) { innerTestFail("run_shell_fail_2.json", 3, 2); }

TEST_F(RunScriptTaskTest, testShellHangWhenFail) { innerTestActionWhenFailed("run_shell_hang.json", 3, "hang"); }

TEST_F(RunScriptTaskTest, testShellFinishWhenFail) { innerTestActionWhenFailed("run_shell_finish.json", 3, "finish"); }

TEST_F(RunScriptTaskTest, testPython) { innerTest("run_python.json", "python", 3, 3); }

TEST_F(RunScriptTaskTest, testPythonFail) { innerTestFail("run_python_fail.json", 3, 12); }

TEST_F(RunScriptTaskTest, testPythonHangWhenFail) { innerTestActionWhenFailed("run_python_hang.json", 3, "hang"); }

TEST_F(RunScriptTaskTest, testPythonFinishWhenFail)
{
    innerTestActionWhenFailed("run_python_finish.json", 3, "finish");
}

void RunScriptTaskTest::innerTest(const string& configFileName, const string& scriptType, int maxRetryTime,
                                  int32_t nodeId)
{
    RunScriptTaskPtr task = prepareTask(configFileName, nodeId);
    ASSERT_TRUE(task);
    Task::TaskInitParam initParam = prepareTaskInitParam(nodeId);
    ASSERT_TRUE(task->init(initParam));

    config::TaskTarget target = prepareTarget();
    task->handleTarget(target);
    while (!task->isDone(target)) {
        sleep(1);
    }

    ASSERT_EQ(maxRetryTime, (int)task->getRetryTimes());

    string logFile = GET_TEMP_DATA_PATH() + "/script_logs/run_script.log.0";
    string content;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(logFile, content));
    BS_LOG(INFO, "ut read logFile content[%s]", content.c_str());

    string arguments = "test arguments: 3 6";
    string partitionId = StringUtil::toString(initParam.instanceInfo.partitionId);
    string instanceId = StringUtil::toString(initParam.instanceInfo.instanceId);
    if (scriptType == "shell") {
        ASSERT_TRUE(content.find(arguments) != string::npos);
        ASSERT_TRUE(content.find("test source file: ok") != string::npos);
        ASSERT_TRUE(content.find(string("BS_TASK_PARTITION_ID = ") + partitionId) != string::npos) << partitionId;
        ASSERT_TRUE(content.find(string("BS_TASK_INSTANCE_ID = ") + instanceId) != string::npos) << instanceId;
        // ASSERT_TRUE(content.find("test binary: fs_util ok") != string::npos);
        // TODO:test use binary in online package
        ASSERT_TRUE(content.find("run link binary: [8]") != string::npos);
        ASSERT_TRUE(content.find("test third-part binary: test_link ok") != string::npos);
    }

    if (scriptType == "python") {
        ASSERT_TRUE(content.find(arguments) != string::npos);
        ASSERT_TRUE(content.find(string("BS_TASK_PARTITION_ID = ") + partitionId) != string::npos) << partitionId;
        ASSERT_TRUE(content.find(string("BS_TASK_INSTANCE_ID = ") + instanceId) != string::npos) << instanceId;
        // ASSERT_TRUE(content.find("test import swift module ok") != string::npos);
        // TODO:test import package in online package
        ASSERT_TRUE(content.find("test import test module ok") != string::npos);
        // ASSERT_TRUE(content.find("test binary: fs_util ok") != string::npos);
        // TODO:test use binary in online package
        ASSERT_TRUE(content.find("test third-part binary: test_link ok") != string::npos);
    }

    sleep(7); // task_script_daemon : max_interval = 5
    string daemonLog = GET_TEMP_DATA_PATH() + "/script_daemon_log";
    vector<string> daemonFiles;
    ASSERT_TRUE(fslib::util::FileUtil::listDir(daemonLog, daemonFiles));

    for (auto& file : daemonFiles) {
        string content;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(fslib::util::FileUtil::joinFilePath(daemonLog, file), content));
        ASSERT_TRUE(content.find("daemon exit now") != string::npos);
    }
}

}} // namespace build_service::task_base
