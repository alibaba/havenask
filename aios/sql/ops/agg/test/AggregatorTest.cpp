#include "sql/ops/agg/Aggregator.h"

#include <cstdint>
#include <ext/alloc_traits.h>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/PoolVector.h"
#include "build_service/config/AgentGroupConfig.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFuncDesc.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "sql/ops/agg/builtin/CountAggFunc.h"
#include "sql/ops/agg/builtin/MaxAggFunc.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnComparator.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

namespace sql {
class AggFunc;
} // namespace sql

using namespace std;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace build_service::config;

namespace sql {

class AggregatorTest : public OpTestBase {
public:
    AggregatorTest();
    ~AggregatorTest();

public:
    void setUp() override {
        _aggFuncFactoryR = _naviRHelper.getOrCreateRes<AggFuncFactoryR>();
    }

public:
    void prepareTable() {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "c", {1, 2, 1, 2, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<bool>(
            _allocator, docs, "bool", {true, true, false, false, false}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocator, docs, "counta", {1, 2, 1, 2, 2}));
        _table = getTable(createTable(_allocator, docs));
    }

public:
    MatchDocAllocatorPtr _allocator;
    Aggregator _aggregator;
    TablePtr _table;
    AggFuncFactoryR *_aggFuncFactoryR = nullptr;
};

AggregatorTest::AggregatorTest()
    : _aggregator(AggFuncMode::AGG_FUNC_MODE_LOCAL, getMemPoolResource()) {
    _aggregator._table.reset(new Table(_poolPtr));
}

AggregatorTest::~AggregatorTest() {}

TEST_F(AggregatorTest, testCreateAggFuncLocal) {
    prepareTable();
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "IDENTITY", {"a"}, {"a"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MIN", {"a"}, {"mina"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MAX", {"a"}, {"maxa"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MAX", {"a"}, {"maxb"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "COUNT", {}, {"counta"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "SUM", {"a"}, {"suma"}, 4));
    ASSERT_TRUE(_aggregator.createAggFunc(
        _aggFuncFactoryR, _table, "AVG", {"a"}, {"count('a')", "'a' + 1"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
        _aggFuncFactoryR, _table, "AVG", {"d"}, {"count('a')", "'a' + 1"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
        _aggFuncFactoryR, _table, "AGG", {"a"}, {"count('a')", "'a' + 1"}));

    ASSERT_EQ(7, _aggregator._aggFilterArgs.size());
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[0]);
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[1]);
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[2]);
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[3]);
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[4]);
    ASSERT_EQ(4, _aggregator._aggFilterArgs[5]);
    ASSERT_EQ(-1, _aggregator._aggFilterArgs[6]);
}

TEST_F(AggregatorTest, testCreateAggFuncGlobal) {
    prepareTable();
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_GLOBAL;
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "IDENTITY", {"a"}, {"a"}, 4));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MIN", {"a"}, {"mina"}, 1));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MAX", {"a"}, {"maxa"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "MAX", {"a"}, {"maxb"}));
    ASSERT_TRUE(
        _aggregator.createAggFunc(_aggFuncFactoryR, _table, "COUNT", {"counta"}, {"counta"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "SUM", {"a"}, {"suma"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "AVG", {"b", "c"}, {"avg"}));
    ASSERT_FALSE(
        _aggregator.createAggFunc(_aggFuncFactoryR, _table, "AVG", {"count('a')", "c"}, {"avg"}));
    ASSERT_FALSE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "AGG", {"b", "c"}, {"avg"}));
    ASSERT_FALSE(_aggregator.createAggFunc(_aggFuncFactoryR, _table, "AVG", {"b"}, {"avg"}));
    ASSERT_EQ(7, _aggregator._aggFilterArgs.size());
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_EQ(-1, _aggregator._aggFilterArgs[i]);
    }
}

TEST_F(AggregatorTest, testInitSuccess) {
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(_aggregator.init(
        _aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"}, _table));
    ASSERT_EQ(3, _aggregator._aggFuncVec.size());
    ASSERT_EQ(3, _aggregator._accumulatorVec.size());
}

TEST_F(AggregatorTest, testInitGroupKey) {
    prepareTable();
    std::vector<AggFuncDesc> aggFuncDesc;
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_GLOBAL;
    ASSERT_TRUE(_aggregator.init(_aggFuncFactoryR, aggFuncDesc, {"a", "b"}, {"b"}, _table));
    ASSERT_EQ(1, _aggregator._aggFuncVec.size());
}

TEST_F(AggregatorTest, testInitDuplicateColumnName) {
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sum($a)"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_FALSE(_aggregator.init(
        _aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sum($a)"}, _table));
}

TEST_F(AggregatorTest, testInitShowGroupKeyFailed) {
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "SUMA",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_FALSE(_aggregator.init(_aggFuncFactoryR, aggFuncDesc, {"e"}, {"e", "$sumA"}, _table));
}

TEST_F(AggregatorTest, testInitCreateFunctionFailed) {
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "SUMA",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_FALSE(_aggregator.init(_aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$sumA"}, _table));
}

TEST_F(AggregatorTest, testAggregateSuccess) {
    NaviLoggerProvider provider("WARN");
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(_aggregator.init(
        _aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"}, _table));
    ASSERT_EQ(3, _aggregator._aggFuncVec.size());
    ASSERT_EQ(3, _aggregator._accumulatorVec.size());
    ASSERT_TRUE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    auto output = _aggregator.getTable();
    cout << TableUtil::toString(output) << endl;
    auto columnData = TableUtil::getColumnData<size_t>(output, "b");
    ColumnAscComparator<size_t> comparator(columnData);
    TableUtil::sort(output, &comparator);

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "count($a)", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sum($a)", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sumA", {7, 4}));
}

TEST_F(AggregatorTest, testAggregateGetColumnFailed) {
    NaviLoggerProvider provider("WARN");
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(_aggregator.init(
        _aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"}, _table));
    _aggregator._aggFilterArgs = {1};
    ASSERT_FALSE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    ASSERT_EQ("column [1] is not bool type", provider.getErrorMessage());
}

TEST_F(AggregatorTest, testAggregateInitInputFailed) {
    prepareTable();
    auto *func = new MaxAggFunc<size_t>({"not exist"}, {"count"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    _aggregator._aggFuncVec.emplace_back(func);
    _aggregator._accumulatorVec.emplace_back(_aggregator._aggregatorPoolPtr.get());
    ASSERT_FALSE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
}

TEST_F(AggregatorTest, testDoAggregate) {
    prepareTable();
    auto *func = new CountAggFunc({}, {"count(a)"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    _aggregator._aggFuncVec.emplace_back(func);
    _aggregator._accumulatorVec.emplace_back(_aggregator._aggregatorPoolPtr.get());
    std::vector<table::ColumnData<bool> *> aggFilterColumn = {nullptr};
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(0), 0, aggFilterColumn));
    ASSERT_EQ(1, _aggregator._accumulatorIdxMap.size());
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(1), 0, aggFilterColumn));
    ASSERT_EQ(1, _aggregator._accumulatorIdxMap.size());
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(2), 1, aggFilterColumn));
    ASSERT_EQ(2, _aggregator._accumulatorIdxMap.size());
}

TEST_F(AggregatorTest, testDoAggregateWithFilterArg) {
    prepareTable();
    auto *func = new CountAggFunc({}, {"count(a)"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    _aggregator._aggFuncVec.emplace_back(func);
    _aggregator._accumulatorVec.emplace_back(_aggregator._aggregatorPoolPtr.get());
    std::vector<table::ColumnData<bool> *> aggFilterColumn
        = {_table->getColumn(4)->getColumnData<bool>()};
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(0), 0, aggFilterColumn));
    ASSERT_EQ(1, _aggregator._accumulatorVec.size());

    ASSERT_EQ(1, _aggregator._accumulatorIdxMap.size());
    ASSERT_EQ(1, _aggregator._accumulatorVec[0].size());
    ASSERT_EQ(1, ((CountAccumulator *)_aggregator._accumulatorVec[0][0])->value);
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(1), 1, aggFilterColumn));
    ASSERT_EQ(2, _aggregator._accumulatorIdxMap.size());
    ASSERT_EQ(2, _aggregator._accumulatorVec[0].size());
    ASSERT_EQ(1, ((CountAccumulator *)_aggregator._accumulatorVec[0][1])->value);
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(2), 2, aggFilterColumn));
    ASSERT_EQ(3, _aggregator._accumulatorIdxMap.size());
    ASSERT_EQ(3, _aggregator._accumulatorVec[0].size());
    ASSERT_EQ(0, ((CountAccumulator *)_aggregator._accumulatorVec[0][2])->value);
}

TEST_F(AggregatorTest, testAggregateWithFilterArg) {
    NaviLoggerProvider provider("WARN");
    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "SUM",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
   ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(_aggregator.init(
        _aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"}, _table));
    ASSERT_EQ(3, _aggregator._aggFuncVec.size());
    ASSERT_EQ(3, _aggregator._accumulatorVec.size());
    _aggregator._aggFilterArgs = {4, -1, -1};
    ASSERT_TRUE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));

    // repeat aggregate
    prepareTable();
    _table->getColumn("bool")->getColumnData<bool>()->set(_table->getRow(2), true);
    ASSERT_TRUE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    auto output = _aggregator.getTable();
    cout << TableUtil::toString(output) << endl;
    auto columnData = TableUtil::getColumnData<size_t>(output, "b");
    ColumnAscComparator<size_t> comparator(columnData);
    TableUtil::sort(output, &comparator);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "count($a)", {3, 2}))
        << TableUtil::toString(_table);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sum($a)", {10, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sumA", {14, 8}));
}

TEST_F(AggregatorTest, testGetNullTable) {
    prepareTable();
    _aggregator._table = nullptr;
    ASSERT_EQ(nullptr, _aggregator.getTable());
}

TEST_F(AggregatorTest, testExtraPlugin) {
    // ATTENTION:
    // make sure `aggregator` destructed before `aggFuncMananger`,
    // otherwise access AggFunc created from plugins in ~Aggregator will make core dump
    NaviResourceHelper naviHelper;
    naviHelper.setModules(
        {"./aios/sql/testdata/sql_agg_func/plugins/libsql_test_agg_plugin_lib.so"});
    auto aggFuncFactoryR = naviHelper.getOrCreateRes<AggFuncFactoryR>();
    ASSERT_TRUE(aggFuncFactoryR);

    Aggregator aggregator(AggFuncMode::AGG_FUNC_MODE_LOCAL, getMemPoolResource());

    prepareTable();
    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$a"],
            "output" : ["$count($a)", "$sum($a)"],
            "type" : "PARTIAL"
        },
        {
            "name" : "sum2",
            "input" : ["$a"],
            "output" : ["$sumA"],
            "type" : "PARTIAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);
    aggregator._mode = AggFuncMode::AGG_FUNC_MODE_LOCAL;
    ASSERT_TRUE(aggregator.init(
        aggFuncFactoryR, aggFuncDesc, {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"}, _table));
    ASSERT_EQ(3, aggregator._aggFuncVec.size());
    ASSERT_EQ(3, aggregator._accumulatorVec.size());
    ASSERT_TRUE(aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    auto output = aggregator.getTable();
    cout << TableUtil::toString(output) << endl;
    auto columnData = TableUtil::getColumnData<size_t>(output, "b");
    ColumnAscComparator<size_t> comparator(columnData);
    TableUtil::sort(output, &comparator);

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "count($a)", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sum($a)", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sumA", {14, 8}));
}

} // namespace sql
