#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class ExecCorrelateKernelTest : public OpTestBase {
public:
    ExecCorrelateKernelTest();
    ~ExecCorrelateKernelTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _table.reset();
    }

private:
    void prepareTable() {
        _poolPtr.reset(new autil::mem_pool::Pool());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            _allocator, docs, "arr", {{1, 2}, {1}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            _allocator, docs, "arr0", {{1, 2, 3, 4}, {1, 2, 3}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            _allocator, docs, "arr1", {{1, 2, 3}, {1, 2}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            _allocator, docs, "arr2", {{1}, {}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
            _allocator, docs, "arr3", {{"ab", "abc"}, {"bcd"}}));
        _table.reset(new Table(docs, _allocator));
    }
    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.ExecCorrelateKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        builder.logLevel("schedule");
        return builder.build();
    }

    void getOutputTable(KernelTester &tester, TablePtr &outputTable) {
        DataPtr outputData;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        outputTable = getTable(outputData);
        ASSERT_TRUE(outputTable != nullptr);
    }

public:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
};

ExecCorrelateKernelTest::ExecCorrelateKernelTest() {}

ExecCorrelateKernelTest::~ExecCorrelateKernelTest() {}

TEST_F(ExecCorrelateKernelTest, testSimpleProcess) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$arr", "$ai"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "ARRAY", "INTEGER"])json");
    _attributeMap["uncollect_attrs"] = ParseJson(R"json([{"output_fields":["$ai"],
        "nest_field_names": ["$arr"],
        "with_ordinality": false,
        "output_field_exprs": {"$ai": "$arr"},
        "output_fields_type": ["INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1]}])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(3, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "id", {3, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "ai", {1, 2, 1}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputMultiColumn<int32_t>(outputTable, "arr", {{1, 2}, {1, 2}, {1}}));
}

TEST_F(ExecCorrelateKernelTest, testReuseInput) {
    // reuse is empty
    {
        _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$arr", "$ai"])json");
        _attributeMap["output_fields_type"]
            = ParseJson(R"json(["INTEGER", "ARRAY", "INTEGER"])json");
        _attributeMap["uncollect_attrs"] = ParseJson(R"json([{"output_fields":["$ai"],
        "nest_field_names": ["$arr"],
        "with_ordinality": false,
        "output_field_exprs": {"$ai": "$arr"},
        "output_fields_type": ["INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1]}])json");
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(3, outputTable->getRowCount());
        ASSERT_EQ(3, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "id", {3, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "ai", {1, 2, 1}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int32_t>(outputTable, "arr", {{1, 2}, {1, 2}, {1}}));

        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(_table, "arr", {{1, 2}, {1}}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int32_t>(_table, "arr1", {{1, 2, 3}, {1, 2}}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(_table, "arr2", {{1}, {}}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<string>(_table, "arr3", {{"ab", "abc"}, {"bcd"}}));
    }

    // reuse input
    {
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$arr", "$ai"])json");
        _attributeMap["output_fields_type"]
            = ParseJson(R"json(["INTEGER", "ARRAY", "INTEGER"])json");
        _attributeMap["uncollect_attrs"] = ParseJson(R"json([{"output_fields":["$ai"],
        "nest_field_names": ["$arr"],
        "with_ordinality": false,
        "output_field_exprs": {"$ai": "$arr"},
        "output_fields_type": ["INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1]}])json");
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(3, outputTable->getRowCount());
        ASSERT_EQ(3, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "id", {3, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "ai", {1, 2, 1}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int32_t>(outputTable, "arr", {{1, 2}, {1, 2}, {1}}));

        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(_table, "arr", {{1, 2}, {1}}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<int32_t>(_table, "arr1", {{1, 2, 3}, {1, 2}}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(_table, "arr2", {{1}, {}}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<string>(_table, "arr3", {{"ab", "abc"}, {"bcd"}}));
    }
}

TEST_F(ExecCorrelateKernelTest, testSimpleProcessWithErrorType) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$arr", "$ai"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "STRING", "INTEGER"])json");
    _attributeMap["uncollect_attrs"] = ParseJson(R"json([{"output_fields":["$ai"],
        "nest_field_names": ["$arr"],
        "with_ordinality": false,
        "output_field_exprs": {"$ai": "$arr"},
        "output_fields_type": ["INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1]}])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_FALSE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
}

TEST_F(ExecCorrelateKernelTest, testSimpleProcessWithOffset) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$arr", "$ai", "$offset"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["INTEGER", "ARRAY", "INTEGER", "INTEGER"])json");
    _attributeMap["uncollect_attrs"] = ParseJson(R"json([{"output_fields":["$ai", "$offset"],
        "nest_field_names": ["$arr"],
        "with_ordinality": true,
        "output_field_exprs": {"$ai": "$arr", "$offset": "$ORDINALITY"},
        "output_fields_type": ["INTEGER", "INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1]}])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "id", {3, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "ai", {1, 2, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "offset", {1, 2, 1}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputMultiColumn<int32_t>(outputTable, "arr", {{1, 2}, {1, 2}, {1}}));
}

TEST_F(ExecCorrelateKernelTest, testMultiCorrelateField) {
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$id", "$arr0","$arr3", "$a0", "$a1", "$a3", "$offset"])json");
    _attributeMap["output_fields_type"] = ParseJson(
        R"json(["INTEGER", "ARRAY", "ARRAY", "INTEGER", "INTEGER", "STRING", "INTEGER"])json");
    _attributeMap["uncollect_attrs"]
        = ParseJson(R"json([{"output_fields":["$a0", "$a1", "$a3", "$offset"],
        "nest_field_names": ["$arr0", "$arr1", "$arr3"],
        "with_ordinality": true,
        "output_field_exprs": {"$a0": "$arr0", "$a1": "$arr1", "$a3": "$arr3", "$offset": "$ORDINALITY"},
        "output_fields_type": ["INTEGER", "INTEGER", "STRING", "INTEGER"],
        "table_name": "tbl", "nest_field_counts": [1, 1, 1]}])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(7, outputTable->getRowCount());
    ASSERT_EQ(7, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "id", {3, 3, 3, 3, 4, 4, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "a0", {1, 2, 3, 4, 1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "a1", {1, 2, 3, 0, 1, 2, 0}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn<int32_t>(outputTable, "offset", {1, 2, 3, 4, 1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn<string>(outputTable, "a3", {"ab", "abc", "", "", "bcd", "", ""}));
}
} // namespace sql
