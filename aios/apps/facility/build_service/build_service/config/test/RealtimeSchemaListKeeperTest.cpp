#include "build_service/config/RealtimeSchemaListKeeper.h"

#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class RealtimeSchemaListKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void checkSchema(const shared_ptr<indexlibv2::config::TabletSchema>& expected,
                     const shared_ptr<indexlibv2::config::TabletSchema>& current);
};

void RealtimeSchemaListKeeperTest::setUp() {}

void RealtimeSchemaListKeeperTest::tearDown() {}

void RealtimeSchemaListKeeperTest::checkSchema(const shared_ptr<indexlibv2::config::TabletSchema>& expected,
                                               const shared_ptr<indexlibv2::config::TabletSchema>& current)
{
    ASSERT_TRUE(expected);
    ASSERT_TRUE(current);
    string expectedStr;
    ASSERT_TRUE(expected->Serialize(false, &expectedStr));
    string currentStr;
    ASSERT_TRUE(current->Serialize(false, &currentStr));
    ASSERT_EQ(expectedStr, currentStr) << expectedStr << endl << currentStr << endl;
}

TEST_F(RealtimeSchemaListKeeperTest, testSimple)
{
    RealtimeSchemaListKeeper keeper;
    uint32_t generationId = 1694504257;
    string clusterName = "cluster1";
    keeper.init(GET_TEMP_DATA_PATH(), clusterName, generationId);
    ResourceReader resourceReader(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config/");
    auto schema = resourceReader.getTabletSchema(clusterName);
    ASSERT_TRUE(schema);
    ASSERT_TRUE(keeper.addSchema(schema));
    vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
    ASSERT_TRUE(keeper.getSchemaList(0, 0, results));
    ASSERT_EQ(1, results.size());
    checkSchema(schema, results[0]);
}

TEST_F(RealtimeSchemaListKeeperTest, testMultipleSchema)
{
    RealtimeSchemaListKeeper keeper;
    uint32_t generationId = 1694504257;
    string clusterName = "cluster1";
    keeper.init(GET_TEMP_DATA_PATH(), clusterName, generationId);
    vector<shared_ptr<indexlibv2::config::TabletSchema>> expectedSchemas;
    string schemaListDir =
        GET_TEMP_DATA_PATH() + clusterName + "/generation_" + to_string(generationId) + "/schema_list/";
    {
        ResourceReader resourceReader(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config/");
        auto schema = resourceReader.getTabletSchema(clusterName);
        ASSERT_TRUE(schema);
        expectedSchemas.push_back(schema);
        ASSERT_TRUE(keeper.addSchema(schema));
        ASSERT_TRUE(fslib::util::FileUtil::isExist(schemaListDir + "schema.json"));
    }
    {
        ResourceReader resourceReader(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config_add_field/");
        auto schema = resourceReader.getTabletSchema(clusterName);
        ASSERT_TRUE(schema);
        expectedSchemas.push_back(schema);
        ASSERT_TRUE(keeper.addSchema(schema));
        ASSERT_TRUE(fslib::util::FileUtil::isExist(schemaListDir + "schema.json.1"));
    }
    {
        vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
        ASSERT_TRUE(keeper.getSchemaList(0, 1, results));
        ASSERT_EQ(2, results.size());
        for (size_t i = 0; i < expectedSchemas.size(); ++i) {
            checkSchema(expectedSchemas[i], results[i]);
        }
    }
    {
        vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
        ASSERT_TRUE(keeper.getSchemaList(1, 1, results));
        ASSERT_EQ(1, results.size());
        checkSchema(expectedSchemas[1], results[0]);
    }
}

TEST_F(RealtimeSchemaListKeeperTest, testSchemaListDirExist)
{
    uint32_t generationId = 1694504257;
    string clusterName = "cluster1";
    string schemaListDir =
        GET_TEMP_DATA_PATH() + clusterName + "/generation_" + to_string(generationId) + "/schema_list/";
    ASSERT_TRUE(fslib::util::FileUtil::mkDir(schemaListDir, true));
    ASSERT_TRUE(fslib::util::FileUtil::atomicCopy(
        GET_TEST_DATA_PATH() + "update_config_when_alter_field/config/schemas/mainse_searcher_schema.json",
        schemaListDir + "schema.json"));
    RealtimeSchemaListKeeper keeper;
    keeper.init(GET_TEMP_DATA_PATH(), clusterName, generationId);
    vector<shared_ptr<indexlibv2::config::TabletSchema>> expectedSchemas;
    {
        ResourceReader resourceReader(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config/");
        auto schema = resourceReader.getTabletSchema(clusterName);
        ASSERT_TRUE(schema);
        expectedSchemas.push_back(schema);
    }
    {
        ResourceReader resourceReader(GET_TEST_DATA_PATH() + "update_config_when_alter_field/config_add_field/");
        auto schema = resourceReader.getTabletSchema(clusterName);
        ASSERT_TRUE(schema);
        expectedSchemas.push_back(schema);
        ASSERT_TRUE(keeper.addSchema(schema));
    }
    vector<shared_ptr<indexlibv2::config::TabletSchema>> results;
    ASSERT_TRUE(keeper.updateSchemaCache());
    ASSERT_TRUE(keeper.getSchemaList(0, 1, results));
    ASSERT_EQ(2, results.size());
    for (size_t i = 0; i < expectedSchemas.size(); ++i) {
        checkSchema(expectedSchemas[i], results[i]);
    }
}

}} // namespace build_service::config
