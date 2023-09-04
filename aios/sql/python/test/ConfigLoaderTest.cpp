#include <iostream>
#include <string>

#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

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
    EXPECT_TRUE(string::npos != expectStr.find(jsonStr));
    ASSERT_EQ("", provider.getErrorMessage());
}

} // namespace navi
