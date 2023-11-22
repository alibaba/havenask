#include "iquan/common/catalog/DatabaseDef.h"

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
class DatabaseDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, DatabaseDefTest);

TEST_F(DatabaseDefTest, testAddTable) {
    DatabaseDef db1;
    TableModel table1;
    table1.tableContent.tableName = "t1";
    table1.tableContent.tableType = "normal";
    ASSERT_TRUE(db1.addTable(table1));
    auto table2 = table1;
    ASSERT_FALSE(db1.addTable(table2));
    ASSERT_EQ(1, db1.tables.size());
    ASSERT_EQ("t1", db1.tables[0].tableName());
    ASSERT_EQ("normal", db1.tables[0].tableType());
}

TEST_F(DatabaseDefTest, testAddLayerTable) {
    DatabaseDef db1;
    LayerTableModel table1;
    table1.layerTableName = "t1";
    ASSERT_TRUE(db1.addTable(table1));
    auto table2 = table1;
    ASSERT_FALSE(db1.addTable(table2));
    ASSERT_EQ(1, db1.layerTables.size());
    ASSERT_EQ("t1", db1.layerTables[0].layerTableName);
}

TEST_F(DatabaseDefTest, testAddFunction) {
    DatabaseDef db1;
    FunctionModel f1;
    f1.functionName = "f1";
    f1.functionType = "udf";
    ASSERT_TRUE(db1.addFunction(f1));
    auto f2 = f1;
    ASSERT_FALSE(db1.addFunction(f2));
    ASSERT_EQ(1, db1.functions.size());
    ASSERT_EQ("f1", db1.functions[0].functionName);
    ASSERT_EQ("udf", db1.functions[0].functionType);
    auto f3 = f2;
    f3.functionType = "udaf";
    ASSERT_TRUE(db1.addFunction(f3));
    ASSERT_EQ(2, db1.functions.size());
    ASSERT_EQ("f1", db1.functions[1].functionName);
    ASSERT_EQ("udaf", db1.functions[1].functionType);
}

TEST_F(DatabaseDefTest, testMergeFailedByDbName) {
    DatabaseDef db1;
    db1.dbName = "db1";
    DatabaseDef db2;
    db2.dbName = "db2";
    ASSERT_FALSE(db1.merge(db2));
}

TEST_F(DatabaseDefTest, testMergeFailedByTable) {
    DatabaseDef db1, db2;
    db1.dbName = db2.dbName = "db";
    TableModel t1;
    t1.tableContent.tableName = "t1";
    ASSERT_TRUE(db1.addTable(t1));
    ASSERT_TRUE(db2.addTable(t1));
    ASSERT_FALSE(db1.merge(db2));
    ASSERT_EQ(1, db1.tables.size());
    ASSERT_EQ("t1", db1.tables[0].tableName());
}

TEST_F(DatabaseDefTest, testMergeFailedByLayerTable) {
    DatabaseDef db1, db2;
    db1.dbName = db2.dbName = "db";
    LayerTableModel t1;
    t1.layerTableName = "t1";
    ASSERT_TRUE(db1.addTable(t1));
    ASSERT_TRUE(db2.addTable(t1));
    ASSERT_FALSE(db1.merge(db2));
    ASSERT_EQ(1, db1.layerTables.size());
    ASSERT_EQ("t1", db1.layerTables[0].layerTableName);
}

TEST_F(DatabaseDefTest, testMergeFailedByFunction) {
    DatabaseDef db1, db2;
    db1.dbName = db2.dbName = "db";
    FunctionModel f1;
    f1.functionName = "f1";
    f1.functionType = "udf";
    ASSERT_TRUE(db1.addFunction(f1));
    ASSERT_TRUE(db2.addFunction(f1));
    ASSERT_FALSE(db1.merge(db2));
    ASSERT_EQ(1, db1.functions.size());
    ASSERT_EQ("f1", db1.functions[0].functionName);
    ASSERT_EQ("udf", db1.functions[0].functionType);
}

TEST_F(DatabaseDefTest, testMergeSuccess) {
    DatabaseDef db1, db2, db3;
    db1.dbName = db2.dbName = db3.dbName = "db";
    TableModel t1, t2, t3;
    t1.tableContent.tableName = "t1";
    t2.tableContent.tableName = "t2";
    t3.tableContent.tableName = "t3";
    ASSERT_TRUE(db1.addTable(t1));
    ASSERT_TRUE(db2.addTable(t2));
    ASSERT_TRUE(db3.addTable(t3));

    LayerTableModel lt1, lt2, lt3;
    lt1.layerTableName = "t1";
    lt2.layerTableName = "t2";
    lt3.layerTableName = "t3";
    ASSERT_TRUE(db1.addTable(lt1));
    ASSERT_TRUE(db2.addTable(lt2));
    ASSERT_TRUE(db3.addTable(lt3));

    FunctionModel f1, f2, f3;
    f1.functionName = "f1";
    f1.functionType = "udf";
    f2.functionName = "f2";
    f2.functionType = "udf";
    f3.functionName = "f1";
    f3.functionType = "udaf";
    ASSERT_TRUE(db1.addFunction(f1));
    ASSERT_TRUE(db2.addFunction(f2));
    ASSERT_TRUE(db3.addFunction(f3));

    ASSERT_TRUE(db1.merge(db2));
    ASSERT_TRUE(db1.merge(db3));

    ASSERT_EQ(
        R"json({"database_name":"db","tables":[{"table_content_type":"","table_content":{"table_name":"t1","table_type":"","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}},{"table_content_type":"","table_content":{"table_name":"t2","table_type":"","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}},{"table_content_type":"","table_content":{"table_name":"t3","table_type":"","fields":[],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":0,"hash_fields":[],"hash_function":"","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}}],"layer_tables":[{"layer_table_name":"t1","content":{"layer_format":[],"layers":[],"properties":{}}},{"layer_table_name":"t2","content":{"layer_format":[],"layers":[],"properties":{}}},{"layer_table_name":"t3","content":{"layer_format":[],"layers":[],"properties":{}}}],"functions":[{"function_name":"f1","function_type":"udf","function_version":1,"is_deterministic":0,"function_content_version":"","function_content":{"prototypes":[],"properties":{}}},{"function_name":"f2","function_type":"udf","function_version":1,"is_deterministic":0,"function_content_version":"","function_content":{"prototypes":[],"properties":{}}},{"function_name":"f1","function_type":"udaf","function_version":1,"is_deterministic":0,"function_content_version":"","function_content":{"prototypes":[],"properties":{}}}]})json",
        autil::legacy::FastToJsonString(db1));
}

} // namespace iquan
