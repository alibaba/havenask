#include "iquan/common/catalog/CatalogDef.h"

#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "sql/common/common.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace std;

namespace iquan {
class CatalogDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, CatalogDefTest);

TEST_F(CatalogDefTest, testGetDatabase) {
    CatalogDef catalogDef;
    ASSERT_EQ(1, catalogDef.databases.size());
    const auto &systemDb = catalogDef.database(sql::SQL_SYSTEM_DATABASE);
    ASSERT_EQ(1, catalogDef.databases.size());
    ASSERT_EQ(&systemDb, &catalogDef.databases[0]);
    catalogDef.database("db1");
    ASSERT_EQ(2, catalogDef.databases.size());
    ASSERT_EQ("db1", catalogDef.databases[1].dbName);
}

TEST_F(CatalogDefTest, testGetLocation) {
    CatalogDef catalogDef;
    ASSERT_EQ(0, catalogDef.locations.size());
    const auto &loc1 = catalogDef.location(LocationSign {1, "qrs", "qrs"});
    ASSERT_EQ(1, catalogDef.locations.size());
    ASSERT_EQ(loc1.sign.nodeName, catalogDef.locations[0].sign.nodeName);
    const auto &loc2 = catalogDef.location(LocationSign {1, "qrs", "qrs"});
    ASSERT_EQ(1, catalogDef.locations.size());
    ASSERT_EQ(&loc1, &loc2);
}

TEST_F(CatalogDefTest, testMergeCatalogFailedByDiffName) {
    CatalogDef catalogDef1;
    catalogDef1.catalogName = "default";
    CatalogDef catalogDef2;
    catalogDef2.catalogName = "catalog2";
    ASSERT_FALSE(catalogDef1.merge(catalogDef2));
    ASSERT_EQ(1, catalogDef1.databases.size());
    ASSERT_EQ(0, catalogDef1.locations.size());
}

TEST_F(CatalogDefTest, testMergeCatalogFailedByDatabase) {
    CatalogDef catalogDef1;
    catalogDef1.catalogName = "default";
    CatalogDef catalogDef2;
    catalogDef2.catalogName = "default";
    TableModel table1;
    table1.tableContent.tableName = "t1";
    table1.tableContent.tableType = "normal";
    ASSERT_TRUE(catalogDef1.database("db1").addTable(table1));
    ASSERT_TRUE(catalogDef2.database("db1").addTable(table1));
    ASSERT_FALSE(catalogDef1.merge(catalogDef2));
    ASSERT_EQ(2, catalogDef1.databases.size());
    ASSERT_EQ("db1", catalogDef1.databases[1].dbName);
    ASSERT_EQ(1, catalogDef1.databases[1].tables.size());
    ASSERT_EQ("t1", catalogDef1.databases[1].tables[0].tableName());
    ASSERT_EQ("normal", catalogDef1.databases[1].tables[0].tableType());
}

TEST_F(CatalogDefTest, testMergeCatalogFailedByLocation) {
    CatalogDef catalogDef1;
    catalogDef1.catalogName = "default";
    CatalogDef catalogDef2;
    catalogDef2.catalogName = "default";
    TableIdentity tableId = {"db1", "t1"};
    LocationSign locSign = {1, "qrs", "qrs"};
    ASSERT_TRUE(catalogDef1.location(locSign).addTableIdentity(tableId));
    ASSERT_TRUE(catalogDef2.location(locSign).addTableIdentity(tableId));
    ASSERT_FALSE(catalogDef1.merge(catalogDef2));
    ASSERT_EQ(1, catalogDef1.locations.size());
    ASSERT_EQ("qrs", catalogDef1.locations[0].sign.nodeName);
    ASSERT_EQ(1, catalogDef1.locations[0].tableIdentities.size());
    ASSERT_EQ("db1", catalogDef1.locations[0].tableIdentities[0].dbName);
    ASSERT_EQ("t1", catalogDef1.locations[0].tableIdentities[0].tableName);
}

TEST_F(CatalogDefTest, testMergeCatalogSuccess) {
    CatalogDef catalogDef1;
    catalogDef1.catalogName = "default";
    CatalogDef catalogDef2;
    catalogDef2.catalogName = "default";
    CatalogDef catalogDef3;
    catalogDef3.catalogName = "default";

    TableModel table1, table2, table3;
    table1.tableContent.tableName = "t1";
    table1.tableContent.tableType = "normal";
    table2.tableContent.tableName = "t2";
    table2.tableContent.tableType = "normal";
    table3.tableContent.tableName = "t3";
    table3.tableContent.tableType = "kv";
    ASSERT_TRUE(catalogDef1.database("db1").addTable(table1));
    ASSERT_TRUE(catalogDef2.database("db2").addTable(table2));
    ASSERT_TRUE(catalogDef3.database("db1").addTable(table3));

    TableIdentity tableId1 = {"db1", "t1"};
    TableIdentity tableId2 = {"db1", "t2"};
    TableIdentity tableId3 = {"db2", "t1"};
    LocationSign locSign = {1, "qrs", "qrs"};
    ASSERT_TRUE(catalogDef1.location(locSign).addTableIdentity(tableId1));
    ASSERT_TRUE(catalogDef2.location(locSign).addTableIdentity(tableId2));
    ASSERT_TRUE(catalogDef3.location(locSign).addTableIdentity(tableId3));

    ASSERT_TRUE(catalogDef1.merge(catalogDef2));
    ASSERT_TRUE(catalogDef1.merge(catalogDef3));
    ASSERT_EQ(
        R"json({"catalog_name":"default","databases":[{"database_name":"system","tables":[],"layer_tables":[],"functions":[]},{"database_name":"db1","tables":[{"table_content_type":"","table_content":{"table_name":"t1","table_type":"normal","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}},{"table_content_type":"","table_content":{"table_name":"t3","table_type":"kv","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}}],"layer_tables":[],"functions":[]},{"database_name":"db2","tables":[{"table_content_type":"","table_content":{"table_name":"t2","table_type":"normal","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}}],"layer_tables":[],"functions":[]}],"locations":[{"partition_cnt":1,"node_name":"qrs","node_type":"qrs","tables":[{"database_name":"db1","table_name":"t1"},{"database_name":"db1","table_name":"t2"},{"database_name":"db2","table_name":"t1"}],"equivalent_hash_fields":[],"join_info":[]}]})json",
        autil::legacy::FastToJsonString(catalogDef1));
}

} // namespace iquan
