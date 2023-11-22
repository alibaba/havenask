#include <cstddef>
#include <map>
#include <memory>
#include <string>

#include "autil/TimeUtility.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Table.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace autil;

namespace sql {

class LogicalScanKernelTest : public OpTestBase {
public:
    LogicalScanKernelTest();
    ~LogicalScanKernelTest();

private:
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.LogicalTableScanKernel");
        builder.output("output0");
        legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("logical");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = legacy::json::ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"]
            = legacy::json::ParseJson(string(R"json(["$attr1", "$attr2"])json"));
        attributeMap["output_fields_internal_type"]
            = legacy::json::ParseJson(string(R"json(["BIGINT", "DOUBLE"])json"));
        string jsonStr = FastToJsonString(attributeMap);
        builder.attrs(jsonStr);
        return builder.build();
    }

    void getOutputTable(KernelTesterPtr testerPtr, table::TablePtr &outputTable, bool &eof) {
        DataPtr odata;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        outputTable = getTable(odata);
        ASSERT_TRUE(outputTable != NULL);
    }

private:
    KernelTesterPtr _testerPtr;
};

LogicalScanKernelTest::LogicalScanKernelTest() {}

LogicalScanKernelTest::~LogicalScanKernelTest() {}

TEST_F(LogicalScanKernelTest, testSimple) {
    // navi::NaviLoggerProvider provider("WARN");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    table::TablePtr table;
    bool eof = false;
    ASSERT_NO_FATAL_FAILURE(getOutputTable(testerPtr, table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, table->getRowCount());
    ASSERT_EQ(table->getColumnCount(), 2);
    ASSERT_EQ("attr1", table->getColumn(0)->getName());
    ASSERT_EQ("attr2", table->getColumn(1)->getName());
    ASSERT_EQ(matchdoc::bt_int64, table->getColumn(0)->getType().getBuiltinType());
    ASSERT_EQ(matchdoc::bt_double, table->getColumn(1)->getType().getBuiltinType());
}

} // namespace sql
