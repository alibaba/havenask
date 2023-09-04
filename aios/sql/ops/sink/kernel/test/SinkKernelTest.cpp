#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class SinkKernelTest : public OpTestBase {
public:
    SinkKernelTest();
    ~SinkKernelTest();

public:
    void setUp() override {}
    void tearDown() override {
        _table.reset();
    }

private:
    void checkOutput(const vector<uint32_t> &expect, DataPtr data) {
        TableDataPtr tableData = std::dynamic_pointer_cast<TableData>(data);
        auto outputTable = tableData->getTable();
        ASSERT_TRUE(outputTable != NULL);
        ASSERT_EQ(expect.size(), outputTable->getRowCount());
        auto column = outputTable->getColumn("id");
        ASSERT_TRUE(column != NULL);
        auto id = column->getColumnData<uint32_t>();
        ASSERT_TRUE(id != NULL);
        for (size_t i = 0; i < expect.size(); i++) {
            ASSERT_EQ(expect[i], id->get(i));
        }
        checkDependentTable(_table, outputTable);
    }

    void prepareTable() {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
            _allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _table.reset(new Table(docs, _allocator));
    }
    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.SinkKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
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
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
};

SinkKernelTest::SinkKernelTest() {}

SinkKernelTest::~SinkKernelTest() {}

TEST_F(SinkKernelTest, testSimpleProcess) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputMultiColumn<char>(outputTable, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
    checkDependentTable(_table, outputTable);
}

TEST_F(SinkKernelTest, testReuseInputs) {
    _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputMultiColumn<char>(outputTable, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
    // check input
    checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4});
    checkOutputColumn<int64_t>(_table, "a", {5, 6, 7, 8});
    checkOutputColumn(_table, "b", {"b1", "b2", "b3", "b4"});
    checkOutputMultiColumn<char>(_table, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}});
}

TEST_F(SinkKernelTest, testBatchInput) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    DataPtr outputData;
    bool eof = false;
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        ASSERT_FALSE(eof);
        checkOutput({1, 2, 3, 4}, outputData);
    }

    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        ASSERT_TRUE(eof);
        checkOutput({1, 2, 3, 4}, outputData);
    }
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testBatchInputEof) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    DataPtr outputData;
    bool eof = false;
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        ASSERT_FALSE(eof);
        checkOutput({1, 2, 3, 4}, outputData);
    }
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        ASSERT_FALSE(eof);
        checkOutput({1, 2, 3, 4}, outputData);
        ASSERT_TRUE(tester.setInputEof("input0"));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData == nullptr);
        ASSERT_TRUE(eof);
    }
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testNoInput) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    ASSERT_FALSE(eof);
    checkOutput({1, 2, 3, 4}, outputData);
    ASSERT_TRUE(tester.setInput("input0", nullptr));
    ASSERT_TRUE(tester.compute());

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    ASSERT_TRUE(eof);
    checkOutput({1, 2, 3, 4}, outputData);
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testKernelComputeFailed) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    ASSERT_FALSE(eof);
    checkOutput({1, 2, 3, 4}, outputData);
    navi::DataPtr data(new navi::Data(TableType::TYPE_ID));
    ASSERT_TRUE(tester.setInput("input0", data));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("invalid input table", tester.getErrorMessage());
    ASSERT_FALSE(tester.compute());
}

} // namespace sql
