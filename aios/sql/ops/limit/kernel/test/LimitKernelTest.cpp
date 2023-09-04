#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
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
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnData.h"
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

class LimitKernelTest : public OpTestBase {
public:
    LimitKernelTest();
    ~LimitKernelTest();

public:
    void setUp() override {}
    void tearDown() override {
        _attributeMap.clear();
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
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.LimitKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

public:
    MatchDocAllocatorPtr _allocator;
};

LimitKernelTest::LimitKernelTest() {}

LimitKernelTest::~LimitKernelTest() {}

TEST_F(LimitKernelTest, testLimit) {
    _attributeMap["limit"] = Any(3);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
    auto table = createTable(_allocator, docs);
    ASSERT_TRUE(tester.setInput("input0", table, true));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2, 3}, odata);
    auto inputTable = getTable(table);
    auto outputTable = getTable(odata);
    ASSERT_TRUE(inputTable);
    ASSERT_TRUE(outputTable);
    checkDependentTable(inputTable, outputTable);

    ASSERT_TRUE(eof);
}

TEST_F(LimitKernelTest, testLimitNotEof) {
    _attributeMap["limit"] = Any(4);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, false));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2, 3, 4}, odata);
    ASSERT_TRUE(eof);
}

TEST_F(LimitKernelTest, testLimitEof) {
    _attributeMap["limit"] = Any(3);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    ASSERT_TRUE(tester.setInput("input0", nullptr, true));

    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata == NULL);
    ASSERT_TRUE(eof);
}

TEST_F(LimitKernelTest, testReuseInput) {
    // reuse is empty
    {
        _attributeMap["limit"] = Any(3);
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        ASSERT_FALSE(tester.hasError());
        DataPtr table;
        {
            vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, docs, "id", {1, 2, 3, 4}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, docs, "a", {3, 2, 4, 1}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
                _allocator, docs, "b", {1, 2, 1, 2}));
            table = createTable(_allocator, docs);
            ASSERT_TRUE(tester.setInput("input0", table, true));
        }
        ASSERT_TRUE(tester.compute());
        ASSERT_EQ(EC_NONE, tester.getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != NULL);
        checkOutput({1, 2, 3}, odata);

        // check input
        checkOutputColumn<uint32_t>(getTable(table), "id", {1, 2, 3});
        checkOutputColumn<uint32_t>(getTable(table), "a", {3, 2, 4});
        checkOutputColumn<int64_t>(getTable(table), "b", {1, 2, 1});
    }
    // reuse input
    {
        _attributeMap["limit"] = Any(3);
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        ASSERT_FALSE(tester.hasError());
        DataPtr table;
        {
            vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, docs, "id", {1, 2, 3, 4}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, docs, "a", {3, 2, 4, 1}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
                _allocator, docs, "b", {1, 2, 1, 2}));
            table = createTable(_allocator, docs);
            ASSERT_TRUE(tester.setInput("input0", table, true));
        }
        ASSERT_TRUE(tester.compute());
        ASSERT_EQ(EC_NONE, tester.getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != NULL);
        checkOutput({1, 2, 3}, odata);
        ASSERT_TRUE(eof);
        // check input
        checkOutputColumn<uint32_t>(getTable(table), "id", {1, 2, 3, 4});
        checkOutputColumn<uint32_t>(getTable(table), "a", {3, 2, 4, 1});
        checkOutputColumn<int64_t>(getTable(table), "b", {1, 2, 1, 2});
        auto inputTable = getTable(table);
        auto outputTable = getTable(odata);
        ASSERT_TRUE(inputTable);
        ASSERT_TRUE(outputTable);
        checkDependentTable(inputTable, outputTable);
    }
}

TEST_F(LimitKernelTest, testLimitOversize) {
    _attributeMap["limit"] = Any(5);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());

    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2, 3, 4}, odata);
    ASSERT_TRUE(eof);
}

TEST_F(LimitKernelTest, testBatchLimit) {
    _attributeMap["limit"] = Any(3);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2}, odata);
    ASSERT_FALSE(eof);

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
    auto table = createTable(_allocator, docs);
    ASSERT_TRUE(tester.setInput("input0", table, true));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({3}, odata);
    auto inputTable = getTable(table);
    auto outputTable = getTable(odata);
    ASSERT_TRUE(inputTable);
    ASSERT_TRUE(outputTable);
    checkDependentTable(inputTable, outputTable);
}

TEST_F(LimitKernelTest, testBatchLimitOversize) {
    _attributeMap["limit"] = Any(10);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_FALSE(eof);
    checkOutput({1, 2}, odata);
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({3, 4}, odata);
}

TEST_F(LimitKernelTest, testLimitWithOffset) {
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(3);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_TRUE(eof);
    checkOutput({4}, odata);
}

TEST_F(LimitKernelTest, testLimitWithOffsetNoData) {
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(4);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_TRUE(eof);
    checkOutput({}, odata);
}

TEST_F(LimitKernelTest, testLimitWithOffsetHasData) {
    _attributeMap["limit"] = Any(2);
    _attributeMap["offset"] = Any(2);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_FALSE(tester.getOutput("output0", odata, eof));
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_TRUE(eof);
    checkOutput({3, 4}, odata);
}

TEST_F(LimitKernelTest, testLimitWithOffsetHasData2) {
    _attributeMap["limit"] = Any(4);
    _attributeMap["offset"] = Any(1);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_FALSE(eof);
    checkOutput({2}, odata);
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_FALSE(eof);
    checkOutput({3, 4}, odata);
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {5, 6}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    ASSERT_TRUE(eof);
    checkOutput({5}, odata);
}

} // namespace sql
