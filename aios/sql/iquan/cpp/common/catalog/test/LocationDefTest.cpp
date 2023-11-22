#include "iquan/common/catalog/LocationDef.h"

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
class LocationDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
    void appendHashField(vector<FieldIdentity> &fieldIdentities,
                         const string &dbName,
                         const string &tableName,
                         const string &fieldName) {
        fieldIdentities.emplace_back(FieldIdentity());
        auto &fieldIdentity = fieldIdentities.back();
        fieldIdentity.dbName = dbName;
        fieldIdentity.tableName = tableName;
        fieldIdentity.fieldName = fieldName;
    }
};

AUTIL_LOG_SETUP(iquan, LocationDefTest);

TEST_F(LocationDefTest, testAddTableIdentity) {
    LocationDef loc1;
    TableIdentity tid1;
    tid1.dbName = "db1";
    tid1.tableName = "t1";
    ASSERT_TRUE(loc1.addTableIdentity(tid1));
    auto tid2 = tid1;
    ASSERT_FALSE(loc1.addTableIdentity(tid2));
    ASSERT_EQ(1, loc1.tableIdentities.size());
    ASSERT_EQ("db1", loc1.tableIdentities[0].dbName);
    ASSERT_EQ("t1", loc1.tableIdentities[0].tableName);
}

TEST_F(LocationDefTest, testAddJoinInfo) {
    LocationDef loc1;
    JoinInfoDef field1;
    field1.mainTable = {"db1", "t1"};
    field1.auxTable = {"db1", "t2"};
    field1.joinField = "field1";
    ASSERT_TRUE(loc1.addJoinInfo(field1));
    auto field2 = field1;
    ASSERT_FALSE(loc1.addJoinInfo(field2));
    ASSERT_EQ(1, loc1.joinInfos.size());
    ASSERT_EQ("field1", loc1.joinInfos[0].joinField);
    ASSERT_EQ(field1.mainTable, loc1.joinInfos[0].mainTable);
    ASSERT_EQ(field1.auxTable, loc1.joinInfos[0].auxTable);
}

TEST_F(LocationDefTest, testAddHashField) {
    LocationDef loc1;
    vector<FieldIdentity> hashField1;
    appendHashField(hashField1, "db1", "t1", "field1");
    appendHashField(hashField1, "db1", "t2", "field2");
    ASSERT_TRUE(loc1.addHashField(hashField1));
    auto hashField2 = hashField1;
    ASSERT_FALSE(loc1.addHashField(hashField2));
    ASSERT_EQ(1, loc1.equilvalentHashFields.size());
    ASSERT_EQ(2, loc1.equilvalentHashFields[0].size());
    ASSERT_EQ(hashField1[0], loc1.equilvalentHashFields[0][0]);
    ASSERT_EQ(hashField1[1], loc1.equilvalentHashFields[0][1]);
}

TEST_F(LocationDefTest, testAddFunction) {
    LocationDef loc1;
    FunctionSign fsign1 = {"f1", "udf"};
    ASSERT_TRUE(loc1.addFunction(fsign1));
    auto fsign2 = fsign1;
    ASSERT_FALSE(loc1.addFunction(fsign2));
    ASSERT_EQ(1, loc1.functions.size());
    ASSERT_EQ(fsign1, loc1.functions[0]);
}

TEST_F(LocationDefTest, testMergeFailedByLocationSign) {
    LocationDef loc1, loc2;
    loc1.sign.nodeName = "qrs";
    loc2.sign.nodeName = "searcher";
    ASSERT_FALSE(loc1.merge(loc2));
}
TEST_F(LocationDefTest, testMergeFailedByTableIdentity) {
    LocationDef loc1, loc2;
    loc1.sign.nodeName = loc2.sign.nodeName = "qrs";
    TableIdentity tid1;
    tid1.dbName = "db1";
    tid1.tableName = "t1";
    ASSERT_TRUE(loc1.addTableIdentity(tid1));
    ASSERT_TRUE(loc2.addTableIdentity(tid1));
    ASSERT_FALSE(loc1.merge(loc2));
    ASSERT_EQ(1, loc1.tableIdentities.size());
    ASSERT_EQ("db1", loc1.tableIdentities[0].dbName);
    ASSERT_EQ("t1", loc1.tableIdentities[0].tableName);
}

TEST_F(LocationDefTest, testMergeFailedByJoinInfo) {
    LocationDef loc1, loc2;
    loc1.sign.nodeName = loc2.sign.nodeName = "qrs";
    JoinInfoDef field1;
    field1.mainTable = {"db1", "t1"};
    field1.auxTable = {"db1", "t2"};
    field1.joinField = "field1";
    ASSERT_TRUE(loc1.addJoinInfo(field1));
    ASSERT_TRUE(loc2.addJoinInfo(field1));
    ASSERT_FALSE(loc1.merge(loc2));
    ASSERT_EQ(1, loc1.joinInfos.size());
    ASSERT_EQ("field1", loc1.joinInfos[0].joinField);
    ASSERT_EQ(field1.mainTable, loc1.joinInfos[0].mainTable);
    ASSERT_EQ(field1.auxTable, loc1.joinInfos[0].auxTable);
}

TEST_F(LocationDefTest, testMergeFailedByHashField) {
    LocationDef loc1, loc2;
    loc1.sign.nodeName = loc2.sign.nodeName = "qrs";
    vector<FieldIdentity> hashField1;
    appendHashField(hashField1, "db1", "t1", "field1");
    appendHashField(hashField1, "db1", "t2", "field2");
    ASSERT_TRUE(loc1.addHashField(hashField1));
    ASSERT_TRUE(loc2.addHashField(hashField1));
    ASSERT_FALSE(loc1.merge(loc2));
    ASSERT_EQ(1, loc1.equilvalentHashFields.size());
    ASSERT_EQ(2, loc1.equilvalentHashFields[0].size());
    ASSERT_EQ(hashField1[0], loc1.equilvalentHashFields[0][0]);
    ASSERT_EQ(hashField1[1], loc1.equilvalentHashFields[0][1]);
}

TEST_F(LocationDefTest, testMergeFailedByFunction) {
    LocationDef loc1, loc2;
    loc1.sign.nodeName = loc2.sign.nodeName = "qrs";
    FunctionSign fsign1 = {"f1", "udf"};
    ASSERT_TRUE(loc1.addFunction(fsign1));
    ASSERT_TRUE(loc2.addFunction(fsign1));
    ASSERT_FALSE(loc1.merge(loc2));
    ASSERT_EQ(1, loc1.functions.size());
    ASSERT_EQ(fsign1, loc1.functions[0]);
}

TEST_F(LocationDefTest, testMergeSuccess) {
    LocationDef loc1, loc2, loc3;
    loc1.sign.nodeName = loc2.sign.nodeName = loc3.sign.nodeName = "qrs";

    TableIdentity tid1 = {"db1", "t1"};
    TableIdentity tid2 = {"db1", "t2"};
    TableIdentity tid3 = {"db2", "t1"};
    ASSERT_TRUE(loc1.addTableIdentity(tid1));
    ASSERT_TRUE(loc2.addTableIdentity(tid2));
    ASSERT_TRUE(loc3.addTableIdentity(tid3));

    JoinInfoDef field1, field2, field3;
    field1.mainTable = {"db1", "t1"};
    field1.auxTable = {"db1", "t2"};
    field1.joinField = "field1";
    field2.mainTable = {"db1", "t1"};
    field2.auxTable = {"db1", "t2"};
    field2.joinField = "field2";
    field3.mainTable = {"db1", "t1"};
    field3.auxTable = {"db1", "t2"};
    field3.joinField = "field3";
    ASSERT_TRUE(loc1.addJoinInfo(field1));
    ASSERT_TRUE(loc2.addJoinInfo(field2));
    ASSERT_TRUE(loc3.addJoinInfo(field3));

    vector<FieldIdentity> hashField1;
    appendHashField(hashField1, "db1", "t1", "field1");
    appendHashField(hashField1, "db1", "t1", "field2");
    vector<FieldIdentity> hashField2;
    appendHashField(hashField2, "db1", "t1", "field1");
    vector<FieldIdentity> hashField3;
    appendHashField(hashField3, "db1", "t1", "field1");
    appendHashField(hashField3, "db1", "t3", "field2");
    ASSERT_TRUE(loc1.addHashField(hashField1));
    ASSERT_TRUE(loc2.addHashField(hashField2));
    ASSERT_TRUE(loc3.addHashField(hashField3));

    FunctionSign fsign1 = {"f1", "udf"};
    FunctionSign fsign2 = {"f2", "udf"};
    FunctionSign fsign3 = {"f1", "udaf"};
    ASSERT_TRUE(loc1.addFunction(fsign1));
    ASSERT_TRUE(loc2.addFunction(fsign2));
    ASSERT_TRUE(loc3.addFunction(fsign3));

    ASSERT_TRUE(loc1.merge(loc2));
    ASSERT_TRUE(loc1.merge(loc3));
    ASSERT_EQ(
        R"json({"partition_cnt":0,"node_name":"qrs","node_type":"","tables":[{"database_name":"db1","table_name":"t1"},{"database_name":"db1","table_name":"t2"},{"database_name":"db2","table_name":"t1"}],"equivalent_hash_fields":[[{"database_name":"db1","table_name":"t1","field_name":"field1"},{"database_name":"db1","table_name":"t1","field_name":"field2"}],[{"database_name":"db1","table_name":"t1","field_name":"field1"}],[{"database_name":"db1","table_name":"t1","field_name":"field1"},{"database_name":"db1","table_name":"t3","field_name":"field2"}]],"join_info":[{"main_table":{"database_name":"db1","table_name":"t1"},"aux_table":{"database_name":"db1","table_name":"t2"},"join_field":"field1"},{"main_table":{"database_name":"db1","table_name":"t1"},"aux_table":{"database_name":"db1","table_name":"t2"},"join_field":"field2"},{"main_table":{"database_name":"db1","table_name":"t1"},"aux_table":{"database_name":"db1","table_name":"t2"},"join_field":"field3"}]})json",
        autil::legacy::FastToJsonString(loc1));
}
} // namespace iquan
