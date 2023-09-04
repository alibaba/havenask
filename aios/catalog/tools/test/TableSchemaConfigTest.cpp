#include "catalog/tools/TableSchemaConfig.h"

#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;

namespace catalog {

class TableSchemaConfigTest : public TESTBASE {
private:
    void addColumn(proto::TableStructure &structure,
                   const string &fieldName,
                   proto::TableStructure::Column::DataType type,
                   bool multiValue = false,
                   bool isPk = false,
                   const string &analyzer = "",
                   const string &defaultValue = "",
                   bool nullable = false,
                   bool updatable = false,
                   const string &separator = "") {
        auto column = structure.add_columns();
        column->set_name(fieldName);
        column->set_type(type);
        column->set_multi_value(multiValue);
        column->set_primary_key(isPk);
        column->set_analyzer(analyzer);
        column->set_default_value(defaultValue);
        column->set_nullable(nullable);
        column->set_updatable(updatable);
        column->set_separator(separator);
    }
    void addIndex(proto::TableStructure &structure,
                  const string &indexName,
                  proto::TableStructure::Index::IndexType indexType,
                  const vector<string> &fields,
                  const map<string, string> &indexParams = {},
                  proto::TableStructure::Index::IndexConfig::CompressType compressType =
                      proto::TableStructure::Index::IndexConfig::NONE) {
        auto index = structure.add_indexes();
        index->set_name(indexName);
        index->set_index_type(indexType);
        auto config = index->mutable_index_config();
        for (size_t i = 0; i < fields.size(); ++i) {
            auto indexFields = config->add_index_fields();
            indexFields->set_field_name(fields[i]);
            indexFields->set_boost((i + 1) * 100);
        }
        for (const auto &[key, value] : indexParams) {
            (*config->mutable_index_params())[key] = value;
        }
        config->set_compress_type(compressType);
    }
    proto::TableStructure createNormalTableStructure() {
        proto::TableStructure tableStructure;
        tableStructure.set_table_name("tb1");
        tableStructure.set_database_name("db1");
        tableStructure.set_catalog_name("ct1");
        addColumn(tableStructure, "int8", proto::TableStructure::Column::INT8, false, true);
        addColumn(tableStructure,
                  "int8s",
                  proto::TableStructure::Column::INT8,
                  true,
                  false,
                  "test_analyzer",
                  "__DEFAULT__",
                  true,
                  true,
                  "#");
        addColumn(tableStructure, "int16", proto::TableStructure::Column::INT16);
        addColumn(tableStructure, "int32", proto::TableStructure::Column::INT32);
        addColumn(tableStructure, "uint32", proto::TableStructure::Column::UINT32);
        addColumn(tableStructure, "int64", proto::TableStructure::Column::INT64);
        addColumn(tableStructure, "uint8", proto::TableStructure::Column::UINT8);
        addColumn(tableStructure, "uint16", proto::TableStructure::Column::UINT16);
        addColumn(tableStructure, "uint64", proto::TableStructure::Column::UINT64);
        addColumn(tableStructure, "float", proto::TableStructure::Column::FLOAT);
        addColumn(tableStructure, "double", proto::TableStructure::Column::DOUBLE);
        addColumn(tableStructure, "string", proto::TableStructure::Column::STRING);
        addColumn(tableStructure, "strings", proto::TableStructure::Column::STRING, true);
        addColumn(tableStructure, "text", proto::TableStructure::Column::TEXT, false, false, "simple_analyzer");
        addColumn(tableStructure, "texts", proto::TableStructure::Column::TEXT, true, false, "simple_analyzer");
        addColumn(tableStructure, "raw", proto::TableStructure::Column::RAW);
        addIndex(tableStructure, "number", proto::TableStructure::Index::NUMBER, {"int16"});
        addIndex(tableStructure,
                 "numbers",
                 proto::TableStructure::Index::NUMBER,
                 {"int8s"},
                 {},
                 proto::TableStructure::Index::IndexConfig::ZLIB);
        addIndex(tableStructure, "string", proto::TableStructure::Index::STRING, {"string"});
        addIndex(tableStructure, "strings", proto::TableStructure::Index::STRING, {"strings"});
        addIndex(tableStructure, "text", proto::TableStructure::Index::TEXT, {"text"});
        addIndex(tableStructure, "texts", proto::TableStructure::Index::TEXT, {"texts"});
        addIndex(tableStructure, "expack", proto::TableStructure::Index::EXPACK, {"text", "texts"});
        addIndex(tableStructure, "range", proto::TableStructure::Index::RANGE, {"int32"});
        addIndex(tableStructure,
                 "pack",
                 proto::TableStructure::Index::PACK,
                 {"text", "texts"},
                 {{"term_payload_flag", "1"},
                  {"doc_payload_flag", "1"},
                  {"position_list_flag", "1"},
                  {"position_payload_flag", "1"},
                  {"term_frequency_flag", "1"},
                  {"term_frequency_bitmap", "1"},
                  {"high_frequency_dictionary", "bitmap1"},
                  {"high_frequency_adaptive_dictionary", "df"},
                  {"high_frequency_term_posting_type", "both"},
                  {"index_analyzer", "simple_analyzer"},
                  {"format_version_id", "1"}});
        addIndex(tableStructure,
                 "pk",
                 proto::TableStructure::Index::PRIMARY_KEY64,
                 {"int8"},
                 {{"has_primary_key_attribute", "true"},
                  {"pk_storage_type", "sort_array"},
                  {"pk_hash_type", "default_hash"}});
        addIndex(tableStructure, "summary", proto::TableStructure::Index::SUMMARY, {"int8", "float", "string"});
        addIndex(tableStructure, "int8s", proto::TableStructure::Index::ATTRIBUTE, {"int8s"});
        addIndex(tableStructure, "int16", proto::TableStructure::Index::ATTRIBUTE, {"int16"});
        addIndex(tableStructure, "uint32", proto::TableStructure::Index::ATTRIBUTE, {"uint32"});
        addIndex(tableStructure, "double", proto::TableStructure::Index::ATTRIBUTE, {"double"});
        addIndex(tableStructure, "strings", proto::TableStructure::Index::ATTRIBUTE, {"strings"});
        std::string annParams = R"(
           {
                "distance_type": "InnerProduct",
                "builder_name": "QcBuilder",
                "searcher_name": "QcSearcher",
                "min_scan_doc_cnt": "10000",
                "dimension" : "8",
                "linear_build_threshold": "10000",
                "embedding_delimiter": "_",
                "enable_rt_build": "true",
                "is_pk_saved": "true"
            })";
        addIndex(tableStructure,
                 "ann",
                 proto::TableStructure::Index::ANN,
                 {"int32", "raw"},
                 {{"indexer", "aitheta2_indexer"}, {"parameters", annParams}});
        auto tableStructureConfig = tableStructure.mutable_table_structure_config();
        tableStructureConfig->set_table_type(proto::TableType::NORMAL);
        auto ttlOption = tableStructureConfig->mutable_ttl_option();
        ttlOption->set_enable_ttl(true);
        ttlOption->set_ttl_field_name("uint32");
        ttlOption->set_default_ttl(3600);
        return tableStructure;
    }

    proto::TableStructure createOrcTableStructure() {
        proto::TableStructure tableStructure;
        tableStructure.set_table_name("tb1");
        tableStructure.set_database_name("db1");
        tableStructure.set_catalog_name("ct1");
        addColumn(tableStructure, "int8", proto::TableStructure::Column::INT8, false, true);
        addColumn(tableStructure, "int8s", proto::TableStructure::Column::INT8, true);
        addColumn(tableStructure, "int16", proto::TableStructure::Column::INT16);
        addColumn(tableStructure, "int32", proto::TableStructure::Column::INT32);
        addColumn(tableStructure, "int64", proto::TableStructure::Column::INT64);
        addColumn(tableStructure, "uint8", proto::TableStructure::Column::UINT8);
        addColumn(tableStructure, "uint16", proto::TableStructure::Column::UINT16);
        addColumn(tableStructure, "uint32", proto::TableStructure::Column::UINT32);
        addColumn(tableStructure, "uint64", proto::TableStructure::Column::UINT64);
        addColumn(tableStructure, "float", proto::TableStructure::Column::FLOAT);
        addColumn(tableStructure, "double", proto::TableStructure::Column::DOUBLE);
        addColumn(tableStructure, "string", proto::TableStructure::Column::STRING);
        addColumn(tableStructure, "strings", proto::TableStructure::Column::STRING, true);
        addColumn(tableStructure, "raw", proto::TableStructure::Column::RAW);
        addIndex(tableStructure, "pk", proto::TableStructure::Index::PRIMARY_KEY64, {"int8"});
        addIndex(
            tableStructure, "orc", proto::TableStructure::Index::ORC, {"int8", "uint64", "float", "double", "string"});
        auto tableStructureConfig = tableStructure.mutable_table_structure_config();
        tableStructureConfig->set_table_type(proto::TableType::ORC);
        return tableStructure;
    }
};

TEST_F(TableSchemaConfigTest, testNormalTable) {
    auto structure = createNormalTableStructure();
    TableStructure tableStructure;
    ASSERT_TRUE(isOk(tableStructure.fromProto(structure)));
    string tableName = "tb1";
    TableSchemaConfig schema;
    ASSERT_TRUE(schema.init(tableName, &tableStructure));
    auto schemaStr = autil::legacy::ToJsonString(schema);
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::writeFile(GET_TEMP_DATA_PATH() + "normal_schema.json", schemaStr));
    string expectedStr;
    string file = GET_PRIVATE_TEST_DATA_PATH() + "normal_schema.json";
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(file, expectedStr));
    ASSERT_EQ(expectedStr, schemaStr);
}

TEST_F(TableSchemaConfigTest, testOrcTable) {
    auto structure = createOrcTableStructure();
    TableStructure tableStructure;
    ASSERT_TRUE(isOk(tableStructure.fromProto(structure)));
    string tableName = "tb1";
    TableSchemaConfig schema;
    ASSERT_TRUE(schema.init(tableName, &tableStructure));
    auto schemaStr = autil::legacy::ToJsonString(schema);
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::writeFile(GET_TEMP_DATA_PATH() + "orc_schema.json", schemaStr));
    string expectedStr;
    string file = GET_PRIVATE_TEST_DATA_PATH() + "orc_schema.json";
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(file, expectedStr));
    ASSERT_EQ(expectedStr, schemaStr);
}

} // namespace catalog
