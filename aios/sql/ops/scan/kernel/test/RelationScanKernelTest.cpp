#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "ha3/common/ColumnTerm.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "suez/turing/navi/TableInfoR.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::common;
using namespace indexlib::testlib;
using namespace indexlib::partition;

namespace sql {

static const string DefaultBuildNode = R"json({
    "table_name":"mainTable",
    "db_name":"default",
    "catalog_name":"default",
    "index_infos":{
        "$pk":{
            "type":"primarykey64",
            "name":"pk"
        }
    },
    "output_fields_internal":[
        "$joinid",
        "$pk",
        "$price"
    ],
    "use_nest_table":false,
    "table_type":"normal",
    "batch_size":10,
    "hash_fields":[
        "pk"
    ],
    "push_down_ops":[
        {
            "attrs":{
                "output_field_exprs":{
                    "$pk":"$joinTable.pk"
                }
            },
            "op_name":"CalcOp"
        }
    ]
})json";

class RelationScanKernelTest : public OpTestBase {
public:
    RelationScanKernelTest();
    ~RelationScanKernelTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _attributeMap.clear();
    }

private:
    void prepareTableInfo() {
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoWithRel = TableInfoConfigurator::createFromIndexApp(
            _tableName, _id2IndexAppMap.begin()->second);
    }

    void prepareInvertedTable() override {
        _tableName = "mainTable";
        // main
        {
            auto schema = IndexlibPartitionCreator::CreateSchema("mainTable",
                                                                 "pk:string;joinid:int32", // fields
                                                                 "pk:primarykey64:pk",     // indexs
                                                                 "joinid", // attributes
                                                                 "",       // summarys
                                                                 "");      // truncateProfile
            string docStrs = "cmd=add,pk=pk0,joinid=0;"
                             "cmd=add,pk=pk1,joinid=0;"
                             "cmd=add,pk=pk2,joinid=1;"
                             "cmd=add,pk=pk3,joinid=1;"
                             "cmd=add,pk=pk4,joinid=2;"
                             "cmd=add,pk=pk5,joinid=3;"
                             "cmd=add,pk=pk6,joinid=4;";
            auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
                schema, _testPath + "mainTable", docStrs);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        // join
        {
            auto schema = IndexlibPartitionCreator::CreateSchema("joinTable",
                                                                 "pk:int32;price:int32", // fields
                                                                 "pk:primarykey64:pk",   // indexs
                                                                 "price;pk", // attributes
                                                                 "",         // summarys
                                                                 "");        // truncateProfile
            string docStrs = "cmd=add,pk=0,price=0;"
                             "cmd=add,pk=3,price=1;"
                             "cmd=add,pk=2,price=2;"
                             "cmd=add,pk=1,price=3;"
                             "cmd=add,pk=4,price=4;";
            auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
                schema, _testPath + "joinTable", docStrs);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        vector<JoinField> joinFields;
        joinFields.push_back(JoinField("joinid", "joinTable", false, true));
        _joinMap["mainTable"] = joinFields;
    }
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.ScanKernel");
        builder.output("output0");
        builder.attrs(DefaultBuildNode);
        return builder.build();
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount) << TableUtil::toString(outputTable);
        auto column = outputTable->getColumn(field);
        auto name = column->getColumnSchema()->getName();
        ASSERT_TRUE(column != NULL);
        for (size_t i = 0; i < rowCount; i++) {
            ASSERT_EQ(expect[i], column->toString(i));
        }
    }

    void getOutputTable(KernelTesterPtr testerPtr, TablePtr &outputTable, bool &eof) {
        DataPtr odata;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        outputTable = getTable(odata);
        ASSERT_TRUE(outputTable != NULL);
    }

protected:
    AUTIL_LOG_DECLARE();

private:
    KernelTesterPtr _testerPtr;
};

AUTIL_LOG_SETUP(sql, RelationScanKernelTest);

RelationScanKernelTest::RelationScanKernelTest() {}

RelationScanKernelTest::~RelationScanKernelTest() {}

TEST_F(RelationScanKernelTest, testSimple) {
    navi::NaviLoggerProvider provider("WARN");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr table;
    bool eof = false;
    getOutputTable(testerPtr, table, eof);
    EXPECT_TRUE(eof);
    checkOutputColumn<int32_t>(table, "joinid", {0, 0, 1, 1, 2, 3, 4});
    checkOutputColumn<int32_t>(table, "price", {0, 0, 3, 3, 2, 1, 4});
}

} // namespace sql
