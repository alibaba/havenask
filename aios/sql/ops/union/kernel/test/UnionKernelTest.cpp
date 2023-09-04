#include "sql/ops/union/UnionKernel.h"

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
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
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

class UnionKernelTest : public OpTestBase {
public:
    UnionKernelTest();
    ~UnionKernelTest();

public:
    void setUp() override {}
    void tearDown() override {
        _table.reset();
        _tableOther.reset();
    }

private:
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

    void prepareTableOther() {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocatorOther, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocatorOther, docs, "id", {1, 2, 3, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocatorOther, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocatorOther, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
            _allocatorOther, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _tableOther.reset(new Table(docs, _allocatorOther));
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.UnionKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

    void getOutputTable(KernelTester &tester, TablePtr &outputTable) {
        outputTable.reset();
        DataPtr outputData;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        if (outputData != nullptr) {
            outputTable = getTable(outputData);
            ASSERT_TRUE(outputTable != nullptr);
        }
    }

public:
    MatchDocAllocatorPtr _allocator;
    MatchDocAllocatorPtr _allocatorOther;
    TablePtr _table;
    TablePtr _tableOther;
};

UnionKernelTest::UnionKernelTest() {}

UnionKernelTest::~UnionKernelTest() {}

TEST_F(UnionKernelTest, testSimpleProcess) {
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
}

TEST_F(UnionKernelTest, testReuseInput) {
    _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    auto *kernel = dynamic_cast<UnionKernel *>(testerPtr->getKernel());
    ASSERT_NE(nullptr, kernel);

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4}));
    ASSERT_EQ(nullptr, kernel->_table);

    prepareTableOther();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_tableOther)), true));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_tableOther, "id", {1, 2, 3, 5}));
    ASSERT_EQ(nullptr, kernel->_table);

    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(UnionKernelTest, testBatchInput) {
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    auto *kernel = dynamic_cast<UnionKernel *>(testerPtr->getKernel());
    ASSERT_NE(nullptr, kernel);

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(_table, outputTable);
    ASSERT_EQ(nullptr, kernel->_table);

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(_table, outputTable);
    ASSERT_EQ(nullptr, kernel->_table);

    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(UnionKernelTest, testNoInput) {
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    auto *kernel = dynamic_cast<UnionKernel *>(testerPtr->getKernel());
    ASSERT_NE(nullptr, kernel);

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(_table, outputTable);
    ASSERT_EQ(nullptr, kernel->_table);

    ASSERT_TRUE(tester.setInput("input0", nullptr));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(nullptr, outputTable);
    ASSERT_EQ(nullptr, kernel->_table);

    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(_table, outputTable);
    ASSERT_EQ(nullptr, kernel->_table);

    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(UnionKernelTest, testKernelDoComputeFailed) {
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(tester, outputTable));
    ASSERT_EQ(_table, outputTable);

    navi::DataPtr data(new navi::Data(TableType::TYPE_ID));
    ASSERT_TRUE(tester.setInput("input0", data));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("invalid input table", tester.getErrorMessage());
    ASSERT_FALSE(tester.compute());
}

TEST_F(UnionKernelTest, testDoCompute) {
    navi::NaviLoggerProvider provider;
    UnionKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(TableDataPtr(new TableData(_table))));
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(TableDataPtr(new TableData(_table))));
    TablePtr outputTable = kernel._table;
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 1, 2, 3, 4}));
    ASSERT_EQ("", provider.getErrorMessage());
}

TEST_F(UnionKernelTest, testDoComputeInvalidInput) {
    navi::NaviLoggerProvider provider;
    UnionKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(TableDataPtr(new TableData(_table))));
    ASSERT_FALSE(kernel.doCompute(nullptr));
    ASSERT_EQ("invalid input table", provider.getErrorMessage());
}

TEST_F(UnionKernelTest, testDoComputeSchemaMismatch) {
    navi::NaviLoggerProvider provider;
    UnionKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(TableDataPtr(new TableData(_table))));
    prepareTable();
    _table->declareColumn<int32_t>("new column");
    ASSERT_FALSE(kernel.doCompute(TableDataPtr(new TableData(_table))));
    ASSERT_EQ("merge input table failed", provider.getErrorMessage());
}

} // namespace sql
