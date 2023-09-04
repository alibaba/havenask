#include "catalog/tools/BSConfigMaker.h"

#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;

namespace catalog {

class BSConfigMakerTest : public TESTBASE {
private:
    proto::Partition createPartition() {
        proto::Partition partition;
        partition.set_partition_name("part1");
        partition.set_table_name("tb1");
        partition.set_database_name("db1");
        partition.set_catalog_name("ct1");
        partition.set_partition_type(proto::PartitionType::TABLE_PARTITION);

        auto dataVersion = partition.mutable_data_source()->mutable_data_version();
        {
            auto dataDescription = dataVersion->add_data_descriptions();
            dataDescription->set_src("file");
            dataDescription->set_data_path("dfs://test");
        }
        {
            auto dataDescription = dataVersion->add_data_descriptions();
            dataDescription->set_src("swift");
            dataDescription->set_swift_root("http://a.b.c.d:2000/swift");
            dataDescription->set_swift_topic_name("test");
            dataDescription->set_swift_start_timestamp("1688979534000000");
        }
        {
            auto dataDescription = dataVersion->add_data_descriptions();
            dataDescription->set_src("odps");
            dataDescription->set_odps_account_type("aliyun");
            dataDescription->set_odps_access_id("aaabbbcccddd");
            dataDescription->set_odps_access_key("111222333");
            dataDescription->set_odps_endpoint("http://aaa.bbb.ccc.com/api");
            dataDescription->set_odps_project_name("test");
            dataDescription->set_odps_table_name("test_table");
            dataDescription->set_odps_partition_name("ds=20230710,hh=16");
        }
        auto tableStructure = partition.mutable_table_structure();
        tableStructure->set_table_name("tb1");
        tableStructure->set_database_name("db1");
        tableStructure->set_catalog_name("ct1");
        {
            auto column = tableStructure->add_columns();
            column->set_name("pk");
            column->set_type(proto::TableStructure::Column::UINT32);
            column->set_primary_key(true);
        }
        {
            auto column = tableStructure->add_columns();
            column->set_name("price");
            column->set_type(proto::TableStructure::Column::UINT32);
            column->set_multi_value(true);
            column->set_default_value("1");
            column->set_nullable(true);
            column->set_updatable(true);
        }
        {
            auto index = tableStructure->add_indexes();
            index->set_name("pk");
            index->set_index_type(proto::TableStructure::Index::PRIMARY_KEY64);
            auto config = index->mutable_index_config();
            auto indexFields = config->add_index_fields();
            indexFields->set_field_name("pk");
        }
        {
            auto index = tableStructure->add_indexes();
            index->set_name("price");
            index->set_index_type(proto::TableStructure::Index::NUMBER);
            auto config = index->mutable_index_config();
            auto indexFields = config->add_index_fields();
            indexFields->set_field_name("price");
            config->set_compress_type(proto::TableStructure::Index::IndexConfig::LZ4);
        }
        auto tableStructureConfig = tableStructure->mutable_table_structure_config();
        {
            auto shardInfo = tableStructureConfig->mutable_shard_info();
            *shardInfo->add_shard_fields() = "pk";
            shardInfo->set_shard_func("HASH");
            shardInfo->set_shard_count(4);
        }
        tableStructureConfig->set_table_type(proto::TableType::NORMAL);
        {
            auto sortOption = tableStructureConfig->mutable_sort_option();
            sortOption->set_sort_build(true);
            {
                auto description = sortOption->add_sort_descriptions();
                description->set_sort_field("price");
                description->set_sort_pattern(proto::TableStructureConfig::SortOption::SortDescription::ASC);
            }
            {
                auto description = sortOption->add_sort_descriptions();
                description->set_sort_field("pk");
                description->set_sort_pattern(proto::TableStructureConfig::SortOption::SortDescription::DESC);
            }
        }
        return partition;
    }
    void assertFileEqual(const string &file1, const string &file2) {
        string content1;
        string content2;
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(file1, content1)) << file1;
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(file2, content2)) << file2;
        ASSERT_EQ(content1, content2);
    }
    void assertExist(const string &path) { ASSERT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isExist(path)); }
};

TEST_F(BSConfigMakerTest, testSimple) {
    auto part = createPartition();
    Partition partition;
    auto status = partition.fromProto(part);
    ASSERT_TRUE(isOk(status)) << status.message();

    auto rootDir = GET_TEMP_DATA_PATH();
    rootDir.pop_back();
    string templatePath = GET_TEST_DATA_PATH() + "bs_template";
    string configPath;
    status = BSConfigMaker::Make(partition, templatePath, rootDir, &configPath);
    ASSERT_TRUE(isOk(status)) << status.message();
    string expectedConfigPath = rootDir + "/ct1/db1/tb1/part1/ef8fdaa083cba26a73380fec9b762207/config";
    ASSERT_EQ(expectedConfigPath, configPath);
    string bsConfig = GET_TEST_DATA_PATH() + "bs_config";
    assertFileEqual(bsConfig + "/analyzer.json", configPath + "/analyzer.json");
    assertFileEqual(bsConfig + "/bs_hippo.json", configPath + "/bs_hippo.json");
    assertExist(configPath + "/build_app.json");
    assertFileEqual(bsConfig + "/clusters/tb1_cluster.json", configPath + "/clusters/tb1_cluster.json");
    assertFileEqual(bsConfig + "/data_tables/tb1_table.json", configPath + "/data_tables/tb1_table.json");
    assertFileEqual(bsConfig + "/schemas/tb1_schema.json", configPath + "/schemas/tb1_schema.json");
}

} // namespace catalog
