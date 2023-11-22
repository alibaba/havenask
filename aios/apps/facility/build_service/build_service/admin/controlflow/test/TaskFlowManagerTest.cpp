#include "build_service/admin/controlflow/TaskFlowManager.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <stdlib.h>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/PathDefine.h"
#include "build_service/common_define.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;

namespace build_service { namespace admin {

class TaskFlowManagerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void syncAllTask(TaskFlowPtr& flow);
    void InnerSimpleTest(TaskFlowManagerPtr& mgr);
    TaskResourceManagerPtr createTaskResourceManager(const string& configPath);
};

void TaskFlowManagerTest::setUp() {}

void TaskFlowManagerTest::tearDown() {}

TEST_F(TaskFlowManagerTest, TestSimpleProcess)
{
    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowManagerPtr mgr(new TaskFlowManager(filePath, factory, TaskResourceManagerPtr()));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleFlow)
{
    // set global kv params for taskFlow manager
    KeyValueMap kvMap;
    kvMap["cluster2"] = "config2";
    kvMap["cluster3"] = "config3";
    kvMap["cluster1"] = "config1";

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    ASSERT_TRUE(mgr.init("test_simple_flow.graph", kvMap));

    mgr.stepRun();
    vector<string> flowIds;
    mgr.getFlowIdByTag("cluster1", flowIds);
    TaskFlowPtr flow2 = mgr.getFlow(flowIds[0]);
    flow2->getTask("cluster1")->setTaskStatus(TaskBase::ts_finish);
    mgr.getFlowIdByTag("cluster2", flowIds);
    TaskFlowPtr flow3 = mgr.getFlow(flowIds[0]);
    flow3->getTask("cluster2")->setTaskStatus(TaskBase::ts_finish);
    mgr.getFlowIdByTag("cluster3", flowIds);
    TaskFlowPtr flow4 = mgr.getFlow(flowIds[0]);
    flow4->getTask("cluster3")->setTaskStatus(TaskBase::ts_finish);

    cout << "###########################################" << endl;
    cout << mgr.getDotString() << endl;
    cout << "###########################################" << endl;

    mgr.stepRun();
    ASSERT_TRUE(flow2->isFlowFinish());
    ASSERT_TRUE(flow3->isFlowFinish());
    ASSERT_TRUE(flow4->isFlowFinish());
}

TEST_F(TaskFlowManagerTest, TestUpstream)
{
    // set global kv params for taskFlow manager
    KeyValueMap kvMap;
    kvMap["cluster1"] = "config1";
    kvMap["cluster2"] = "config2";
    kvMap["cluster3"] = "config3";

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowManager mgr(filePath, factory, TaskResourceManagerPtr());
    ASSERT_TRUE(mgr.init("test_set_upstream.graph", kvMap));

    mgr.stepRun();
    vector<string> flowIds;
    mgr.getFlowIdByTag("cluster2", flowIds);
    TaskFlowPtr flow2 = mgr.getFlow(flowIds[0]);
    mgr.getFlowIdByTag("cluster3", flowIds);
    TaskFlowPtr flow3 = mgr.getFlow(flowIds[0]);
    mgr.getFlowIdByTag("cluster1", flowIds);
    TaskFlowPtr flow1 = mgr.getFlow(flowIds[0]);

    ASSERT_TRUE(flow2->getTask("cluster2"));
    ASSERT_FALSE(flow3->getTask("cluster3"));
    ASSERT_FALSE(flow1->getTask("cluster1"));
    flow2->getTask("cluster2")->setTaskStatus(TaskBase::ts_finish);

    mgr.stepRun();
    ASSERT_TRUE(flow2->getTask("cluster2"));
    ASSERT_TRUE(flow3->getTask("cluster3"));
    ASSERT_FALSE(flow1->getTask("cluster1"));
    ASSERT_EQ(TaskFlow::tf_running, flow1->getStatus());

    mgr.removeFlow(flow2->getFlowId(), true);
    mgr.stepRun();
    vector<string> upstreamInfoStr = flow3->getUpstreamItemInfos();
    ASSERT_EQ(string("0:finish:2"), upstreamInfoStr[0]);

    ASSERT_EQ(TaskFlow::tf_eof, flow3->getStatus());
    mgr.stopFlow(flow3->getFlowId());
    flow3->getTask("cluster3")->setTaskStatus(TaskBase::ts_stopped);
    mgr.stepRun();

    mgr.removeFlow(flow3->getFlowId(), true);
    mgr.stepRun();
    ASSERT_EQ(TaskFlow::tf_stopped, flow1->getStatus());
}

TEST_F(TaskFlowManagerTest, TestLocalLuaPath)
{
    unsetenv(BS_ENV_LUA_CONFIG_PATH.c_str());

    string zkRoot = GET_TEMP_DATA_PATH();
    ASSERT_FALSE(LocalLuaScriptReader::initLocalPath(zkRoot));

    string luaConfigPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/lua.conf";
    setenv(BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str(), 1);
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));

    string luaPath;
    LocalLuaScriptReader::ConfigInfo confInfo;
    ASSERT_TRUE(LocalLuaScriptReader::getEnvLuaPathInfo(luaPath, confInfo));

    ASSERT_TRUE(!confInfo.isZip);

    string localLuaPath = LocalLuaScriptReader::getLocalLuaPath(confInfo.pathId);
    std::vector<string> expectFiles;
    fslib::util::FileUtil::listDirRecursive(luaPath, expectFiles, true);
    sort(expectFiles.begin(), expectFiles.end());

    std::vector<string> files;
    fslib::util::FileUtil::listDirRecursive(localLuaPath, files, true);
    sort(files.begin(), files.end());
    ASSERT_EQ(expectFiles, files);

    string luaZkPath =
        fslib::util::FileUtil::joinFilePath(common::PathDefine::getAdminControlFlowRoot(zkRoot), confInfo.pathId);
    std::vector<string> zkFiles;
    fslib::util::FileUtil::listDirRecursive(luaZkPath, zkFiles, true);
    sort(zkFiles.begin(), zkFiles.end());
    ASSERT_EQ(expectFiles, zkFiles);

    // redo
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));
}

TEST_F(TaskFlowManagerTest, TestLocalLuaZipPath)
{
    unsetenv(BS_ENV_LUA_CONFIG_PATH.c_str());

    string zkRoot = GET_TEMP_DATA_PATH();
    ASSERT_FALSE(LocalLuaScriptReader::initLocalPath(zkRoot));

    string luaConfigPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_zip.conf";
    setenv(BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str(), 1);
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));

    string luaPath;
    LocalLuaScriptReader::ConfigInfo confInfo;
    ASSERT_TRUE(LocalLuaScriptReader::getEnvLuaPathInfo(luaPath, confInfo));

    ASSERT_TRUE(confInfo.isZip);

    string localLuaPath = LocalLuaScriptReader::getLocalLuaPath(confInfo.pathId) + ".zip";
    bool exist;
    ASSERT_TRUE(fslib::util::FileUtil::isExist(localLuaPath, exist) && exist);

    string luaZkPath =
        fslib::util::FileUtil::joinFilePath(common::PathDefine::getAdminControlFlowRoot(zkRoot), confInfo.pathId) +
        ".zip";
    ASSERT_TRUE(fslib::util::FileUtil::isExist(luaZkPath, exist) && exist);

    string localLuaDir = LocalLuaScriptReader::getLocalLuaPath(confInfo.pathId);
    fslib::util::FileUtil::mkDir(localLuaDir);
    // redo
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));
    ASSERT_TRUE(fslib::util::FileUtil::isExist(localLuaDir, exist) && !exist);

    fslib::util::FileUtil::mkDir(localLuaDir);

    setenv("reuse_local_lua_dir", "true", 1);
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));
    ASSERT_TRUE(fslib::util::FileUtil::isExist(localLuaDir, exist) && exist);
    unsetenv("reuse_local_lua_dir");
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessWithZipFile)
{
    unsetenv(BS_ENV_LUA_CONFIG_PATH.c_str());
    string luaConfigPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_zip.conf";
    setenv(BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str(), 1);

    string zkRoot = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = LocalLuaScriptReader::getLocalLuaPath("lua");
    TaskFlowManagerPtr mgr(new TaskFlowManager(filePath, factory, TaskResourceManagerPtr()));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessWithZipFileAndNamespace)
{
    unsetenv(BS_ENV_LUA_CONFIG_PATH.c_str());
    string luaConfigPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_ns_zip.conf";
    setenv(BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str(), 1);

    string userConfigPath = GET_TEST_DATA_PATH() + "resource_reader_test/config_with_lua_ns";
    TaskResourceManagerPtr resMgr = createTaskResourceManager(userConfigPath);

    string zkRoot = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = LocalLuaScriptReader::getLocalLuaPath("lua_ns");
    TaskFlowManagerPtr mgr(new TaskFlowManager(filePath, factory, resMgr));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessReadGraphFromNamespaceFirst)
{
    unsetenv(BS_ENV_LUA_CONFIG_PATH.c_str());
    string luaConfigPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_ns_first_zip.conf";
    setenv(BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str(), 1);
    cout << "[chayu_test]" << luaConfigPath << endl;
    // assert(false);
    string userConfigPath = GET_TEST_DATA_PATH() + "resource_reader_test/config_with_lua_ns_first";
    cout << "[chayu_test]" << userConfigPath << endl;
    TaskResourceManagerPtr resMgr = createTaskResourceManager(userConfigPath);

    string zkRoot = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(LocalLuaScriptReader::initLocalPath(zkRoot));

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = LocalLuaScriptReader::getLocalLuaPath("lua_ns_first");
    cout << "[chayu_test]" << filePath << endl;
    TaskFlowManagerPtr mgr(new TaskFlowManager(filePath, factory, resMgr));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessWithUserConfigLuaScript)
{
    string userConfigPath = GET_TEST_DATA_PATH() + "resource_reader_test/config_with_lua";
    TaskResourceManagerPtr resMgr = createTaskResourceManager(userConfigPath);
    TaskFactoryPtr factory(new TaskFactory);
    TaskFlowManagerPtr mgr(new TaskFlowManager("invalid_path", factory, resMgr));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessWithRedirectUserConfigLuaScript)
{
    string jsonStr = R"( {
   "redirect_to" : "$PATH_PREFIX/lua_scripts/ext/new_test_flow.flow",
   "is_absolute_path" : true
} )";

    string userConfigPath = GET_TEST_DATA_PATH() + "resource_reader_test/config_with_redirect_lua";
    StringUtil::replaceAll(jsonStr, "$PATH_PREFIX", userConfigPath);
    string filePath = userConfigPath + "/lua_scripts/test_flow.flow";
    fslib::util::FileUtil::removeIfExist(filePath);
    fslib::util::FileUtil::writeFile(filePath, jsonStr);

    TaskResourceManagerPtr resMgr = createTaskResourceManager(userConfigPath);
    TaskFactoryPtr factory(new TaskFactory);
    TaskFlowManagerPtr mgr(new TaskFlowManager("invalid_path", factory, resMgr));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSimpleProcessWithUserConfigZipLuaScript)
{
    string userConfigPath = GET_TEST_DATA_PATH() + "resource_reader_test/zip_config";
    TaskResourceManagerPtr resMgr = createTaskResourceManager(userConfigPath);
    TaskFactoryPtr factory(new TaskFactory);
    TaskFlowManagerPtr mgr(new TaskFlowManager("invalid_path", factory, resMgr));
    InnerSimpleTest(mgr);
}

TEST_F(TaskFlowManagerTest, TestSuspendFlow)
{
    KeyValueMap kvMap;
    kvMap["cluster1"] = "config1";
    kvMap["cluster2"] = "config2";
    kvMap["cluster3"] = "config3";

    TaskFactoryPtr factory(new TaskFactory);
    string filePath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowManagerPtr mgr(new TaskFlowManager(filePath, factory, TaskResourceManagerPtr()));
    ASSERT_TRUE(mgr->init("test_graph.graph", kvMap));

    ASSERT_TRUE(mgr->loadSubGraph("", "test_suspend_flow.graph", kvMap));
    vector<string> flowIds;
    mgr->getFlowIdByTag("abc", flowIds);
    auto flow = mgr->getFlow(flowIds[0]);
    ASSERT_TRUE(flow->isFlowSuspending());
    WorkerNodes workerNodes;
    for (auto& task : mgr->getAllTask()) {
        task->syncTaskProperty(workerNodes);
    }
    flow->stepRun();
    ASSERT_TRUE(flow->isFlowSuspended());
}

void TaskFlowManagerTest::syncAllTask(TaskFlowPtr& flow)
{
    WorkerNodes workerNodes;
    flow->stepRun();
    for (auto task : flow->getTasks()) {
        task->syncTaskProperty(workerNodes);
    }
    flow->stepRun();
}

void TaskFlowManagerTest::InnerSimpleTest(TaskFlowManagerPtr& mgr)
{
    // set global kv params for taskFlow manager
    KeyValueMap kvMap;
    kvMap["cluster1"] = "config1";
    kvMap["cluster2"] = "config2";
    kvMap["cluster3"] = "config3";
    ASSERT_TRUE(mgr->init("test_graph.graph", kvMap));

    kvMap.clear();
    kvMap["cluster4"] = "config4";
    kvMap["cluster5"] = "config5";
    // ASSERT_FALSE(mgr->loadSubGraph("sub1", "test_graph.graph", kvMap)); no same flow id
    ASSERT_TRUE(mgr->loadSubGraph("sub1", "test_sub_graph.graph", kvMap));

    // set default flow params for target flow
    vector<string> flowIds;
    mgr->getFlowIdByTag("cluster1", flowIds);
    ASSERT_EQ(1u, flowIds.size());
    mgr->getFlow(flowIds[0])->setGlobalTaskParam("memory", "12580");
    mgr->getFlow(flowIds[0])->setGlobalTaskParam("parallel_num", "1");

    // declare task param in target flow
    KeyValueMap cluster1BuilderParam;
    cluster1BuilderParam["memory"] = "65535";
    cluster1BuilderParam["parallel_num"] = "10";
    mgr->declareTaskParameter(flowIds[0], "builder", cluster1BuilderParam);

    string str = mgr->serialize();
    cout << str.size() << endl;
    cout << "###########################################" << endl;
    cout << str << endl;
    cout << "###########################################" << endl;

    string rootPath = mgr->getRootPath();
    TaskFactoryPtr taskFactory = mgr->getTaskFactory();
    TaskResourceManagerPtr resMgr = mgr->getTaskResourceManager();
    mgr.reset(new TaskFlowManager(rootPath, taskFactory, resMgr));
    ASSERT_TRUE(mgr->deserialize(str));
    mgr->stepRun(); // make eof

    TaskFlowPtr flow2 = mgr->getFlow(flowIds[0]);
    syncAllTask(flow2);
    ASSERT_TRUE(flow2->isFlowFinish());
    mgr->getFlowIdByTag("cluster2", flowIds);
    TaskFlowPtr flow3 = mgr->getFlow(flowIds[0]);
    syncAllTask(flow3);
    ASSERT_TRUE(flow3->isFlowFinish());
    mgr->getFlowIdByTag("cluster3", flowIds);
    TaskFlowPtr flow4 = mgr->getFlow(flowIds[0]);
    syncAllTask(flow4);
    ASSERT_TRUE(flow4->isFlowFinish());
    mgr->getFlowIdByTag("cluster4", flowIds);
    TaskFlowPtr flow5 = mgr->getFlow(flowIds[0]);
    syncAllTask(flow5);
    ASSERT_TRUE(flow5->isFlowFinish());
    mgr->getFlowIdByTag("cluster5", flowIds);
    TaskFlowPtr flow6 = mgr->getFlow(flowIds[0]);
    syncAllTask(flow6);
    ASSERT_TRUE(flow6->isFlowFinish());
    mgr->getFlowIdByTag("abc", flowIds);
    TaskFlowPtr flow1 = mgr->getFlow(flowIds[0]);
    ASSERT_FALSE(flow1->isFlowFinish());
    mgr->stepRun();
    flow1 = mgr->getFlow(flowIds[0]);
    mgr->getFlowIdByTag("a1", flowIds);

    cout << "###########################################" << endl;
    cout << mgr->getDotString(true) << endl;
    cout << "###########################################" << endl;

    TaskFlowPtr flowA1 = mgr->getFlow(flowIds[0]);
    syncAllTask(flowA1);
    ASSERT_TRUE(flowA1->isFlowFinish());
    mgr->getFlowIdByTag("b1", flowIds);
    TaskFlowPtr flowB1 = mgr->getFlow(flowIds[0]);
    syncAllTask(flowB1);
    ASSERT_TRUE(flowB1->isFlowFinish());

    syncAllTask(flow1);
    ASSERT_TRUE(flow1->isFlowFinish());

    cout << "###########################################" << endl;
    cout << mgr->getDotString() << endl;
    cout << "###########################################" << endl;
}

TaskResourceManagerPtr TaskFlowManagerTest::createTaskResourceManager(const string& configPath)
{
    TaskResourceManagerPtr mgr(new TaskResourceManager);
    ResourceReaderPtr resourceReader(new ResourceReader(configPath));
    resourceReader->init();

    ConfigReaderAccessorPtr accessor(new ConfigReaderAccessor("simple"));
    accessor->addConfig(resourceReader, true);
    mgr->addResource(accessor);
    return mgr;
}

}} // namespace build_service::admin
