#include "build_service/config/SrcNodeConfig.h"

#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class SrcNodeConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void SrcNodeConfigTest::setUp() {}

void SrcNodeConfigTest::tearDown() {}

TEST_F(SrcNodeConfigTest, testSimple)
{
    ResourceReader reader(GET_TEST_DATA_PATH() + "src_node_config_test");
    reader.init();
    SrcNodeConfig srcNodeConfig;
    ASSERT_TRUE(reader.getConfigWithJsonPath("simple.json", "src_node_config", srcNodeConfig));
    ASSERT_TRUE(srcNodeConfig.validate());
    ASSERT_TRUE(srcNodeConfig.needSrcNode());
    ASSERT_TRUE(srcNodeConfig.enableOrderPreserving);
    ASSERT_TRUE(srcNodeConfig.enableUpdateToAdd);
    indexlib::config::BuildConfig expectedBuildConfig;
    ASSERT_EQ(1024, srcNodeConfig.options.GetOnlineConfig().buildConfig.buildTotalMemory);
    ASSERT_EQ("cache_size=2;block_size=2097152", srcNodeConfig.blockCacheParam);
}

TEST_F(SrcNodeConfigTest, testInvalid)
{
    ResourceReader reader(GET_TEST_DATA_PATH() + "src_node_config_test");
    reader.init();
    SrcNodeConfig srcNodeConfig;
    ASSERT_TRUE(reader.getConfigWithJsonPath("invalid.json", "src_node_config", srcNodeConfig));
    ASSERT_FALSE(srcNodeConfig.validate());
}
}} // namespace build_service::config
