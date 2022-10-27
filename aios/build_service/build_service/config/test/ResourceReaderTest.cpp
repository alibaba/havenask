#include "build_service/test/unittest.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/field_schema.h>

using namespace std;
using namespace testing;
using namespace build_service::util;

IE_NAMESPACE_USE(config);

namespace build_service {
namespace config {

class ResourceReaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    template<typename T>
    void checkTypedValue(const std::string &jsonPath,
                         const T &expectValue);

};

void ResourceReaderTest::setUp() {
    ResourceReaderManager::GetInstance()->_readerCache.clear();
}

void ResourceReaderTest::tearDown() {
}

class UserDefineStruct : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("my_value1", myValue1);
        json.Jsonize("my_value2", myValue2);
    }
    bool operator == (const UserDefineStruct &other) const {
        return myValue1 == other.myValue1 && myValue2 == other.myValue2;
    }
public:
    int myValue1;
    string myValue2;
};

TEST_F(ResourceReaderTest, testGetConfigWithJsonPath) {
    checkTypedValue("root_str_key", string("root_str_value"));
    checkTypedValue("root_int_key", int(10));

    // check for inherit config
    checkTypedValue("base_str_key", string("base_str_value"));
    checkTypedValue("base_int_key", int(5));

    vector<int> vec;
    vec.push_back(10);
    vec.push_back(20);
    vec.push_back(30);
    checkTypedValue("root_vec_key3", vec);

    checkTypedValue("root_vec_key1[0]", string("str1"));

    checkTypedValue("root_map_key.child_map_key1.child_int_key", int(100000));
    checkTypedValue("root_map_key.child_map_key1.child_vec_key[2]", false);
    checkTypedValue("root_map_key.child_vec_key[0][1][1]", string("bcd2"));

    checkTypedValue("root_map_key.not_exist_path", string(""));

    KeyValueMap map;
    map["child_map_key3"] = "child_map_value3";
    map["child_map_key4"] = "child_map_value4";
    checkTypedValue("root_map_key.child_map_key1.child_map_key2", map);

    UserDefineStruct uds;
    uds.myValue1 = 1;
    uds.myValue2 = "aaa";
    checkTypedValue("root_map_key.user_define_struct", uds);
}

TEST_F(ResourceReaderTest, testGetAllClusters) {
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(
            TEST_DATA_PATH"resource_reader_test/multi_table_config");
    {
        vector<string> clusterNames;
        EXPECT_TRUE(resourceReader->getAllClusters("simple1", clusterNames));
        EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster1"), string("cluster2")));
    }
    {
        vector<string> clusterNames;
        EXPECT_TRUE(resourceReader->getAllClusters("simple2", clusterNames));
        EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster11"), string("cluster13")));
    }
}

TEST_F(ResourceReaderTest, testGetAllDataTables) {
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(
            TEST_DATA_PATH"resource_reader_test/multi_table_config");
    vector<string> dataTables;
    EXPECT_TRUE(resourceReader->getAllDataTables(dataTables));
    EXPECT_THAT(dataTables, UnorderedElementsAre(string("simple1"), string("simple2")));
}

TEST_F(ResourceReaderTest, testGetDataTableFromClusterName) {
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(
            TEST_DATA_PATH"resource_reader_test/multi_table_config");
    string dataTable;
    EXPECT_TRUE(resourceReader->getDataTableFromClusterName("cluster1", dataTable));
    EXPECT_EQ("simple1", dataTable);
    EXPECT_FALSE(resourceReader->getDataTableFromClusterName("not exist", dataTable));
}

TEST_F(ResourceReaderTest, testGetGenerationMeta) {
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_TRUE(resourceReader->initGenerationMeta(""));
        EXPECT_EQ("", resourceReader->getGenerationMetaValue("value"));
    }
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_TRUE(resourceReader->initGenerationMeta(
                        TEST_DATA_PATH"generation_meta_test/indexroot1"));
        EXPECT_EQ("value1", resourceReader->getGenerationMetaValue("key1"));
    }
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_FALSE(resourceReader->initGenerationMeta(
                        TEST_DATA_PATH"generation_meta_test/indexroot3"));
    }
}

TEST_F(ResourceReaderTest, testGetSchemaBySchemaTableName) {
    string configDir = TEST_DATA_PATH"resource_reader_test/config_updatable_cluster1";
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(configDir);
    ASSERT_TRUE(FileUtil::atomicCopy(configDir+"/schemas/mainse_searcher_schema.json",
                                     configDir+"/schemas/test_schema.json"));
    IndexPartitionSchemaPtr schema =
        resourceReader->getSchemaBySchemaTableName("test");
    ASSERT_TRUE(schema);
    const FieldSchemaPtr &fieldSchema = schema->GetFieldSchema();
    ASSERT_EQ(165, fieldSchema->GetFieldCount());

    // will read schema from cache
    ASSERT_TRUE(FileUtil::remove(configDir+"/schemas/test_schema.json"));
    IndexPartitionSchemaPtr schema2 =
        resourceReader->getSchemaBySchemaTableName("test");
    ASSERT_NO_THROW(schema->AssertEqual(*(schema2.get())));
}

TEST_F(ResourceReaderTest, testInitGenerationMeta) {
    {
        // test normal
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(
                TEST_DATA_PATH"resource_reader_test/multi_table_config");
        ASSERT_TRUE(resourceReader->initGenerationMeta(0, "simple1",
                        TEST_DATA_PATH"resource_reader_test/index_root"));
        ASSERT_EQ("value1", resourceReader->getGenerationMetaValue("key1"));
        ASSERT_EQ("value3", resourceReader->getGenerationMetaValue("key3"));
    }
    {
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(
                TEST_DATA_PATH"resource_reader_test/multi_table_config");
        ASSERT_FALSE(resourceReader->initGenerationMeta(0, "simple2",
                        TEST_DATA_PATH"resource_reader_test/index_root"));
    }
}


template <typename T>
void ResourceReaderTest::checkTypedValue(const string &jsonPath, const T &expectValue)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(
            TEST_DATA_PATH"/resource_reader_test/");
    T value;
    ASSERT_TRUE(resourceReader->getConfigWithJsonPath("test_json_file.json", jsonPath, value)) << jsonPath;
    EXPECT_TRUE(value == expectValue);
}

}
}
