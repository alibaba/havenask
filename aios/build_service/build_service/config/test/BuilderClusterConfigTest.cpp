#include "build_service/test/unittest.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/util/FileUtil.h"
#include "build_service/config/ResourceReaderManager.h"

using namespace std;
using namespace build_service::util;

namespace build_service {
namespace config {

class BuilderClusterConfigTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    string _configPath;
};

void BuilderClusterConfigTest::setUp() {
    _configPath = TEST_DATA_PATH "/builderConfig/";
}

void BuilderClusterConfigTest::tearDown() {
}

TEST_F(BuilderClusterConfigTest, testInitSuccess) {
    {
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        ASSERT_TRUE(config.init("simple", *(resourceReader.get()), "full"));
        EXPECT_EQ(uint32_t(1), config.indexOptions.GetMergeConfig().mergeThreadCount);
    }
    {
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        ASSERT_TRUE(config.init("simple", *(resourceReader.get()), ""));
        EXPECT_EQ(uint32_t(2), config.indexOptions.GetMergeConfig().mergeThreadCount);
    }
}

TEST_F(BuilderClusterConfigTest, testInitFailed) {
    {
        // table name illegal
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        ASSERT_FALSE(config.init("simple3", *(resourceReader.get()), "full"));    
    }
    {
        // no table name
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        ASSERT_FALSE(config.init("simple1", *(resourceReader.get()), "full"));
    }
    {
        // build option config illegal
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        ASSERT_FALSE(config.init("simple2", *(resourceReader.get()), "full"));
    }
    {
        // online_index_config illegal
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        string mergeConfigName = "";
        ASSERT_FALSE(config.init("simple4", *(resourceReader.get()), ""));
    }
    {
        // full merge_config illegal
        BuilderClusterConfig config;
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
        string mergeConfigName = "";
        ASSERT_FALSE(config.init("simple5", *(resourceReader.get()), "full"));
    }
}


}
}
