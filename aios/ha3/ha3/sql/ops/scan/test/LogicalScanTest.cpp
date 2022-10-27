#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/scan/LogicalScan.h>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace suez::turing;
using namespace matchdoc;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class LogicalScanTest : public OpTestBase {
public:
    LogicalScanTest();
    ~LogicalScanTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
    void init() {
        JsonMap attributeMap;
        attributeMap["table_type"] = string("logical");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2"])json"));
        attributeMap["output_fields_type"] = ParseJson(string(R"json(["BIGINT", "DOUBLE"])json"));
        string jsonStr = ToJsonString(attributeMap);
        JsonMap jsonMap;
        FromJsonString(jsonMap, jsonStr);
        Jsonizable::JsonWrapper wrapper(jsonMap);
        ScanInitParam param;
        ASSERT_TRUE(param.initFromJson(wrapper));
        param.bizResource = _sqlBizResource.get();
        param.queryResource = _sqlQueryResource.get();
        param.memoryPoolResource = &_memPoolResource;
        ASSERT_TRUE(logicalScan.init(param));
    }

    LogicalScan logicalScan;
};

LogicalScanTest::LogicalScanTest() {
}

LogicalScanTest::~LogicalScanTest() {
}

TEST_F(LogicalScanTest, testDoBatchScan) {
    init();
    TablePtr table = logicalScan.createTable();
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(table->getColumnCount(), 2);
    ASSERT_STREQ(table->getColumnName(0).c_str(), "$attr1");
    ASSERT_STREQ(table->getColumnName(1).c_str(), "$attr2");
    ASSERT_EQ(table->getColumnType(0).getBuiltinType(), bt_int64);
    ASSERT_EQ(table->getColumnType(1).getBuiltinType(), bt_double);
}
END_HA3_NAMESPACE(sql);