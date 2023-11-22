#include "sql/ops/agg/AggKernel.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/common.h"
#include "sql/ops/agg/AggBase.h"
#include "sql/ops/agg/AggLocal.h"
#include "sql/ops/agg/Aggregator.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/common.h"
#include "table/ColumnComparator.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class AggKernelTest : public OpTestBase {
public:
    AggKernelTest();
    ~AggKernelTest();

public:
    void setUp() override {}
    void tearDown() override {}

private:
    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.AggKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

public:
    MatchDocAllocatorPtr _allocator;
};

AggKernelTest::AggKernelTest() {}

AggKernelTest::~AggKernelTest() {}

TEST_F(AggKernelTest, testSimplePartial) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$countA", "$sumA", "$countB", "$sumB", "$b"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        },
        {
            "name" : "AVG",
            "input" : ["$b"],
            "output" : ["$countB", "$sumB"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError()) << testerPtr->getErrorMessage();
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(5, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<int64_t>(table, "b");
    ColumnAscComparator<int64_t> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countA", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(table, "sumA", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countB", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "b", {1, 2}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testSimpleNormal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$avgA", "$sumB", "$b", "$minA", "$maxA", "$count", "$idA"])json");
    _attributeMap["output_fields_type"] = ParseJson(
        R"json(["DOUBLE", "BIGINT", "BIGINT", "INTEGER", "INTEGER", "BIGINT", "INTEGER"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("NORMAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$avgA"],
            "type" : "NORMAL"
        },
        {
            "name" : "SUM",
            "input" : ["$b"],
            "output" : ["$sumB"],
            "type" : "NORMAL"
        },
        {
            "name" : "MIN",
            "input" : ["$a"],
            "output" : ["$minA"],
            "type" : "NORMAL"
        },
        {
            "name" : "MAX",
            "input" : ["$a"],
            "output" : ["$maxA"],
            "type" : "NORMAL"
        },
        {
            "name" : "COUNT",
            "input" : [],
            "output" : ["$count"],
            "type" : "NORMAL"
        },
        {
            "name" : "ARBITRARY",
            "input" : ["$a"],
            "output" : ["$idA"],
            "type" : "NORMAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(7, table->getColumnCount());
    cout << TableUtil::toString(table) << endl;
    auto columnData = TableUtil::getColumnData<int64_t>(table, "b");
    ColumnAscComparator<int64_t> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgA", {3.5, 4.0 / 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "minA", {3, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "maxA", {4, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "count", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "idA", {3, 2}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testMultiFieldGroupKeyNormal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$avgA", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["DOUBLE", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("NORMAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$avgA"],
            "type" : "NORMAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int64_t>(
            _allocator, docs, "b", {{1, 1}, {2, 2}, {1, 1}, {2, 2}, {2, 2}}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<MultiInt64>(table, "b");
    ASSERT_NE(nullptr, columnData);
    ColumnAscComparator<MultiInt64> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgA", {3.5, 4.0 / 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int64_t>(table, "b", {{1, 1}, {2, 2}}));
}

TEST_F(AggKernelTest, testBatchedNormal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$b", "$maxA", "$count"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "INTEGER", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("NORMAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "MAX",
            "input" : ["$a"],
            "output" : ["$maxA"],
            "type" : "NORMAL"
        },
        {
            "name" : "COUNT",
            "input" : [],
            "output" : ["$count"],
            "type" : "NORMAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;

    const size_t nGroups = 1000;
    const size_t nPerBatch = nGroups / 2;
    auto currentGroupNums = [](int b) { return static_cast<size_t>(b % 4 + 1); };

    vector<uint32_t> idVec;
    vector<uint32_t> aVec;
    vector<int64_t> bVec;

    for (size_t i = 0, idx = 0; i < nGroups; ++i) {
        for (size_t j = 0; j < currentGroupNums(i); ++j) {
            idVec.push_back(idx++);
            aVec.push_back(i - j);
            bVec.push_back(i);
        }
    }

    for (size_t startIdx = 0, n = idVec.size(); startIdx < n; startIdx += nPerBatch) {
        size_t endIdx = std::min(startIdx + nPerBatch, n);
        vector<uint32_t> currIdVec(idVec.begin() + startIdx, idVec.begin() + endIdx);
        vector<uint32_t> currAVec(aVec.begin() + startIdx, aVec.begin() + endIdx);
        vector<int64_t> currBVec(bVec.begin() + startIdx, bVec.begin() + endIdx);
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, currIdVec.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", currIdVec));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", currAVec));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", currBVec));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, endIdx == n));
        ASSERT_TRUE(tester.compute()); // kernel compute success
    }

    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(nGroups, table->getRowCount());
    ASSERT_EQ(3, table->getColumnCount());

    auto columnData = TableUtil::getColumnData<int64_t>(table, "b");
    ColumnAscComparator<int64_t> comparator(columnData);
    TableUtil::sort(table, &comparator);
    vector<uint32_t> expectedMaxA;
    vector<int64_t> expectedCount;
    for (size_t i = 0; i < nGroups; ++i) {
        expectedMaxA.push_back(i);
        expectedCount.push_back(currentGroupNums(i));
    }
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(table, "maxA", expectedMaxA));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(table, "count", expectedCount));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testSimpleGlocal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$avgB", "$avgA"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["DOUBLE", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("FINAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "output" : ["$avgA"],
            "input" : ["$countA", "$sumA"],
            "type" : "FINAL"
        },
        {
            "name" : "AVG",
            "output" : ["$avgB"],
            "input" : ["$countB", "$sumB"],
            "type" : "FINAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "countA", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint64_t>(_allocator, docs, "sumA", {7, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "countB", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "sumB", {2, 6}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());

    auto columnData = TableUtil::getColumnData<double>(table, "avgA");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgA", {4.0 / 3, 3.5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgB", {2, 1}));
    ASSERT_EQ("avgB", table->getColumn(0)->getName());
    ASSERT_EQ("avgA", table->getColumn(1)->getName());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testNoGroupKey) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$countA", "$sumA", "$countB", "$sumB"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["BIGINT", "INTEGER", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        },
        {
            "name" : "AVG",
            "input" : ["$b"],
            "output" : ["$countB", "$sumB"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json([])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(4, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countA", {5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(table, "sumA", {11}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countB", {5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {8}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testBatchInput) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$countA", "$sumA", "$countB", "$sumB", "$b"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        },
        {
            "name" : "AVG",
            "input" : ["$b"],
            "output" : ["$countB", "$sumB"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(5, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<int64_t>(table, "b");
    ColumnAscComparator<int64_t> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countA", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(table, "sumA", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countB", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "b", {1, 2}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testBatchNoGroupKey) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$countA", "$sumA", "$countB", "$sumB"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        },
        {
            "name" : "AVG",
            "input" : ["$b"],
            "output" : ["$countB", "$sumB"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json([])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(4, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countA", {5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(table, "sumA", {11}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countB", {5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {8}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testComputeInput) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$countA", "$sumA", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        ASSERT_TRUE(tester.setInput("input0", nullptr));
        ASSERT_TRUE(tester.compute()); // kernel compute failed
        auto ec = tester.getErrorCode();
        ASSERT_EQ(EC_NONE, ec);
        ASSERT_EQ("", tester.getErrorMessage());
    }
}

TEST_F(AggKernelTest, testDoComputeFailed) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$countA", "$sumA", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$d"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute failed
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);
    ASSERT_NE("", tester.getErrorMessage());
}

TEST_F(AggKernelTest, testFinalize) {
    {
        navi::NaviLoggerProvider provider;
        AggKernel kernel;
        kernel._aggBase.reset(new AggLocal(getMemPoolResource()));
        DataPtr data;
        ASSERT_EQ(EC_ABORT, kernel.finalize(data));
        ASSERT_FALSE(data.get());
    }
    {
        navi::NaviLoggerProvider provider;
        AggKernel kernel;
        AggLocal *aggLocal = new AggLocal(getMemPoolResource());
        kernel._aggBase.reset(aggLocal);
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = getTable(createTable(_allocator, docs));
        ASSERT_EQ("id", table->getColumn(0)->getName());
        ASSERT_EQ("a", table->getColumn(1)->getName());
        ASSERT_EQ("b", table->getColumn(2)->getName());

        aggLocal->_localAggregator._table = table;
        _naviRHelper.kernelConfig(R"json(
        {
            "output_fields" : ["a", "id", "b"],
            "output_fields_type" : ["INTEGER", "INTEGER", "BIGINT"]
        }
        )json");
        ASSERT_TRUE(_naviRHelper.getOrCreateRes(kernel._calcTableR));
        ASSERT_TRUE(_naviRHelper.getOrCreateRes(kernel._sqlSearchInfoCollectorR));
        DataPtr data;
        ASSERT_EQ(EC_NONE, kernel.finalize(data));
        auto outputTable = getTable(data);

        ASSERT_EQ(5, outputTable->getRowCount());
        ASSERT_EQ(3, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "b", {1, 2, 1, 2, 2}));
        ASSERT_EQ("a", outputTable->getColumn(0)->getName());
        ASSERT_EQ("id", outputTable->getColumn(1)->getName());
        ASSERT_EQ("b", outputTable->getColumn(2)->getName());
        ASSERT_EQ("", provider.getErrorMessage());
    }
}

TEST_F(AggKernelTest, testFinalizeProject) {
    navi::NaviLoggerProvider provider;
    AggKernel kernel;
    AggLocal *aggLocal = new AggLocal(getMemPoolResource());
    kernel._aggBase.reset(aggLocal);
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
    auto table = getTable(createTable(_allocator, docs));
    ASSERT_EQ("id", table->getColumn(0)->getName());
    ASSERT_EQ("a", table->getColumn(1)->getName());
    ASSERT_EQ("b", table->getColumn(2)->getName());
    table->clearRows();

    aggLocal->_localAggregator._table = table;
    _naviRHelper.kernelConfig(R"json(
    {
        "output_fields" : ["a", "id", "b"],
        "output_fields_type" : ["INTEGER", "INTEGER", "BIGINT"]
    }
    )json");
    ASSERT_TRUE(_naviRHelper.getOrCreateRes(kernel._calcTableR));
    ASSERT_TRUE(_naviRHelper.getOrCreateRes(kernel._sqlSearchInfoCollectorR));
    DataPtr data;
    ASSERT_EQ(EC_NONE, kernel.finalize(data));
    auto outputTable = getTable(data);

    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(3, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "a", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "b", {}));
    ASSERT_EQ("a", outputTable->getColumn(0)->getName());
    ASSERT_EQ("id", outputTable->getColumn(1)->getName());
    ASSERT_EQ("b", outputTable->getColumn(2)->getName());
    ASSERT_EQ("", provider.getErrorMessage());
}

TEST_F(AggKernelTest, testAvgPartial) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$countA", "$sumA", "$countB", "$sumB", "$b"])json");
    _attributeMap["output_fields_type"]
        = ParseJson(R"json(["BIGINT", "BIGINT", "BIGINT", "BIGINT", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$countA", "$sumA"],
            "type" : "PARTIAL"
        },
        {
            "name" : "AVG",
            "input" : ["$b"],
            "output" : ["$countB", "$sumB"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(5, table->getColumnCount());

    auto columnData = TableUtil::getColumnData<double>(table, "b");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countA", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(table, "sumA", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(table, "countB", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "sumB", {2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1, 2}));
}

TEST_F(AggKernelTest, testAvgGlobal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$avgB", "$avgA"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["DOUBLE", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("FINAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "output" : ["$avgA"],
            "input" : ["$countA", "$sumA"],
            "type" : "FINAL"
        },
        {
            "name" : "AVG",
            "output" : ["$avgB"],
            "input" : ["$countB", "$sumB"],
            "type" : "FINAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "countA", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint64_t>(_allocator, docs, "sumA", {7, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "countB", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "sumB", {2, 6}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<double>(table, "avgA");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgA", {4.0 / 3, 7.0 / 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgB", {2, 1}));
    ASSERT_EQ("avgB", table->getColumn(0)->getName());
    ASSERT_EQ("avgA", table->getColumn(1)->getName());
}

TEST_F(AggKernelTest, testCountPartial) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$count", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "COUNT",
            "input" : [],
            "output" : ["$count"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<double>(table, "b");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "count", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1, 2}));
}

TEST_F(AggKernelTest, testCountGlobal) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$count"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["DOUBEL"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("FINAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "COUNT",
            "output" : ["$count"],
            "input" : ["$count"],
            "type" : "FINAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "count", {2, 3}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(1, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "count", {5}));
    ASSERT_EQ("count", table->getColumn(0)->getName());
}

TEST_F(AggKernelTest, testIdentity) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(1, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<double>(table, "b");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1, 2}));
}

TEST_F(AggKernelTest, testMax) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$max", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "MAX",
            "input" : ["$a"],
            "output" : ["$max"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<double>(table, "b");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(table, "max", {4, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1, 2}));
}

TEST_F(AggKernelTest, testMin) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$min", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "MIN",
            "input" : ["$a"],
            "output" : ["$min"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    auto columnData = TableUtil::getColumnData<double>(table, "b");
    ColumnAscComparator<double> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(table, "min", {3, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1, 2}));
}

TEST_F(AggKernelTest, testGroupKeyLimit) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$sum", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sum"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap["hints"]
        = ParseJson(R"json({"AGG_ATTR":{"groupKeyLimit":"1", "stopExceedLimit":"false"}})json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "b", {1}));
}

TEST_F(AggKernelTest, testMemoryLimit) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"] = ParseJson(R"json(["$sum", "$b"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT", "DOUBLE"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("PARTIAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sum"],
            "type" : "PARTIAL"
        }
    ])json");
    _attributeMap["hints"] = ParseJson(R"json({"AGG_ATTR":{"memoryLimit": "1"}})json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr != nullptr);
    auto &tester = *testerPtr;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<double>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table, true));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.hasError());
    string expect = "agg pool used too many bytes, limit=[1],";
    ASSERT_NE(string::npos, tester.getErrorMessage().find(expect));

    // confirm check interval
    auto *kernel = (AggKernel *)tester.getKernel();
    auto *aggLocal = dynamic_cast<AggLocal *>(kernel->_aggBase.get());
    ASSERT_NE(nullptr, aggLocal);
    ASSERT_EQ(5, aggLocal->_localAggregator._aggregateCnt);
}

TEST_F(AggKernelTest, testReuseInput) {
    KernelTesterBuilder builder;
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$avgA", "$sumB", "$b", "$minA", "$maxA", "$count", "$idA"])json");
    _attributeMap["output_fields_type"] = ParseJson(
        R"json(["DOUBLE", "BIGINT", "BIGINT", "INTEGER", "INTEGER", "BIGINT", "INTEGER"])json");
    _attributeMap[AGG_SCOPE_ATTRIBUTE] = string("NORMAL");
    _attributeMap[AGG_FUNCTION_ATTRIBUTE] = ParseJson(R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$avgA"],
            "type" : "NORMAL"
        },
        {
            "name" : "SUM",
            "input" : ["$b"],
            "output" : ["$sumB"],
            "type" : "NORMAL"
        },
        {
            "name" : "MIN",
            "input" : ["$a"],
            "output" : ["$minA"],
            "type" : "NORMAL"
        },
        {
            "name" : "MAX",
            "input" : ["$a"],
            "output" : ["$maxA"],
            "type" : "NORMAL"
        },
        {
            "name" : "COUNT",
            "input" : [],
            "output" : ["$count"],
            "type" : "NORMAL"
        },
        {
            "name" : "ARBITRARY",
            "input" : ["$a"],
            "output" : ["$idA"],
            "type" : "NORMAL"
        }
    ])json");
    _attributeMap[AGG_GROUP_BY_KEY_ATTRIBUTE] = ParseJson(R"json(["$b"])json");
    _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto &tester = *testerPtr;
    DataPtr inputeData;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        inputeData = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", inputeData, true));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    auto table = getTable(outputData);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(7, table->getColumnCount());
    cout << TableUtil::toString(table) << endl;
    auto columnData = TableUtil::getColumnData<int64_t>(table, "b");
    ColumnAscComparator<int64_t> comparator(columnData);
    TableUtil::sort(table, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "avgA", {3.5, 4.0 / 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "sumB", {2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "minA", {3, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "maxA", {4, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "count", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(table, "idA", {3, 2}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
    // check input
    checkOutputColumn<int32_t>(getTable(inputeData), "id", {1, 2, 3, 4, 5});
    checkOutputColumn<int32_t>(getTable(inputeData), "a", {3, 2, 4, 1, 1});
    checkOutputColumn<int64_t>(getTable(inputeData), "b", {1, 2, 1, 2, 2});
}

} // namespace sql
