#include <iostream>
#include <string>

#include "autil/EnvUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {

class ConfigLoaderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ConfigLoaderTest::setUp() {}

void ConfigLoaderTest::tearDown() {}

TEST_F(ConfigLoaderTest, testSimple) {
    navi::NaviLoggerProvider provider;
    std::string jsonStr;
    std::string configPath = "./aios/sql/testdata/python_test";
    std::string loadParamJsonPath = configPath + "/load_param.json";
    std::string loadParam;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(loadParamJsonPath, loadParam));
    ASSERT_TRUE(KernelTesterBuilder::loadPythonConfig(
        "aios/sql/python/sql_config_loader.py", configPath, loadParam, jsonStr))
        << jsonStr;
    std::cout << jsonStr << std::endl;
    std::string configJsonPath = configPath + "/expect.json";
    std::string expectStr;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(configJsonPath, expectStr));
    ASSERT_THAT(expectStr, HasSubstr(jsonStr));
    ASSERT_EQ("", provider.getErrorMessage());
}

TEST_F(ConfigLoaderTest, testEngineConfigBuiltin) {
    navi::NaviLoggerProvider provider;
    std::string jsonStr;
    std::string configPath = "./aios/sql/testdata/python_test";
    std::string loadParamJsonPath = configPath + "/load_param.json";
    std::string loadParam;

    std::string naviQueueSize = "200";
    std::string naviProcessingSize = "500";
    EnvGuard _0("naviQueueSize", naviQueueSize);
    EnvGuard _1("naviProcessingSize", naviProcessingSize);

    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(loadParamJsonPath, loadParam));
    ASSERT_TRUE(KernelTesterBuilder::loadPythonConfig(
        "aios/sql/python/sql_config_loader.py", configPath, loadParam, jsonStr))
        << jsonStr;
    ASSERT_EQ("", provider.getErrorMessage());
    std::string expectStr = R"json(
        "builtin_task_queue": {
            "processing_size": 500,
            "queue_size": 200,
            "thread_num": 0
        })json";
    ASSERT_THAT(jsonStr, HasSubstr(expectStr));
}

TEST_F(ConfigLoaderTest, testEngineConfigExtra1) {
    navi::NaviLoggerProvider provider;
    std::string jsonStr;
    std::string configPath = "./aios/sql/testdata/python_test";
    std::string loadParamJsonPath = configPath + "/load_param.json";
    std::string loadParam;

    std::string naviExtraTaskQueue = "queue1|2|100|200;";
    EnvGuard _("naviExtraTaskQueue", naviExtraTaskQueue);

    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(loadParamJsonPath, loadParam));
    ASSERT_TRUE(KernelTesterBuilder::loadPythonConfig(
        "aios/sql/python/sql_config_loader.py", configPath, loadParam, jsonStr))
        << jsonStr;
    ASSERT_EQ("", provider.getErrorMessage());
    std::string expectStr = R"json(
        "extra_task_queue": {
            "queue1": {
                "processing_size": 200,
                "queue_size": 100,
                "thread_num": 2
            }
        })json";
    ASSERT_THAT(jsonStr, HasSubstr(expectStr));
}

TEST_F(ConfigLoaderTest, testEngineConfigExtra_Error) {
    std::string jsonStr;
    std::string configPath = "./aios/sql/testdata/python_test";
    std::string loadParamJsonPath = configPath + "/load_param.json";
    std::string loadParam;

    std::string naviExtraTaskQueue = "queue1|2|100|200;queue2|0";
    EnvGuard _("naviExtraTaskQueue", naviExtraTaskQueue);

    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(loadParamJsonPath, loadParam));
    navi::NaviLoggerProvider provider;
    ASSERT_FALSE(KernelTesterBuilder::loadPythonConfig(
        "aios/sql/python/sql_config_loader.py", configPath, loadParam, jsonStr))
        << jsonStr;
    ASSERT_THAT(provider.getErrorMessage(),
                HasSubstr("task_queue size must be 4, actual [['queue2', '0']]"));
}

} // namespace navi
