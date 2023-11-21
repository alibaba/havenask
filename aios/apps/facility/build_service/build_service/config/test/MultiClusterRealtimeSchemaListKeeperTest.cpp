#include "build_service/config/MultiClusterRealtimeSchemaListKeeper.h"

#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class MultiClusterRealtimeSchemaListKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void MultiClusterRealtimeSchemaListKeeperTest::setUp() {}

void MultiClusterRealtimeSchemaListKeeperTest::tearDown() {}

TEST_F(MultiClusterRealtimeSchemaListKeeperTest, testSimple)
{
    MultiClusterRealtimeSchemaListKeeper keeper;
    vector<string> clusterNames = {"cluster1"};
    uint32_t generationId = 1694504257;
    keeper.init(GET_TEMP_DATA_PATH(), clusterNames, generationId);
    {
        auto resourceReader =
            make_shared<ResourceReader>(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config/");
        ASSERT_TRUE(keeper.updateConfig(resourceReader));
        ASSERT_EQ(1, keeper._schemaListKeepers.size());
        auto& singleKeeper = keeper._schemaListKeepers[0];
        vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
        ASSERT_TRUE(singleKeeper->getSchemaList(0, 0, results));
        ASSERT_EQ(1, results.size());
    }
    {
        auto resourceReader =
            make_shared<ResourceReader>(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config_add_field/");
        ASSERT_TRUE(keeper.updateConfig(resourceReader));
        ASSERT_EQ(1, keeper._schemaListKeepers.size());
        auto& singleKeeper = keeper._schemaListKeepers[0];
        vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
        ASSERT_TRUE(singleKeeper->getSchemaList(0, 1, results));
        ASSERT_EQ(2, results.size());
    }
}

}} // namespace build_service::config
