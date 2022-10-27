#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/agg/Aggregator.h>
#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>
#include <ha3/sql/ops/agg/builtin/MaxAggFunc.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/rank/OneDimColumnComparator.h>
#include <ha3/config/SqlAggPluginConfig.h>
#include <autil/legacy/RapidJsonCommon.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);

class AggregatorTest : public OpTestBase {
public:
    AggregatorTest();
    ~AggregatorTest();
public:
    void setUp() override {
        _aggFuncManager.reset(new AggFuncManager());
        _aggFuncManager->init("", SqlAggPluginConfig());
    }
    void tearDown() override {
        _aggregator.reset();
    }
public:
    void prepareTable() {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "b", {1, 2, 1, 2, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "c", {1, 2, 1, 2, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<bool>(_allocator, docs, "bool", {true, true, false, false, false}));
        _table = getTable(createTable(_allocator, docs));
    }
public:
    MatchDocAllocatorPtr _allocator;
    Aggregator _aggregator;
    AggFuncManagerPtr _aggFuncManager;
    TablePtr _table;
};

AggregatorTest::AggregatorTest()
    : _aggregator(true, &_pool, &_memPoolResource)
{
}

AggregatorTest::~AggregatorTest() {
}

TEST_F(AggregatorTest, testCreateAggFuncLocal) {
    prepareTable();
    _aggregator._isLocal = true;
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "IDENTITY", {"a"}, {"a"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MIN", {"a"}, {"mina"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MAX", {"a"}, {"maxa"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MAX", {"a"}, {"maxb"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "COUNT", {}, {"counta"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "SUM", {"a"}, {"suma"}, 4));
    ASSERT_TRUE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AVG", {"a"}, {"count('a')", "'a' + 1"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AVG", {"d"}, {"count('a')", "'a' + 1"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AGG", {"a"}, {"count('a')", "'a' + 1"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AVG", {"a"}, {"count('a')"}));

    ASSERT_EQ(7, _aggregator._aggFilterColumn.size());
    ASSERT_FALSE(_aggregator._aggFilterColumn[0]);
    ASSERT_FALSE(_aggregator._aggFilterColumn[1]);
    ASSERT_FALSE(_aggregator._aggFilterColumn[2]);
    ASSERT_FALSE(_aggregator._aggFilterColumn[3]);
    ASSERT_FALSE(_aggregator._aggFilterColumn[4]);
    ASSERT_TRUE(_aggregator._aggFilterColumn[5]);
    ASSERT_FALSE(_aggregator._aggFilterColumn[6]);
}

TEST_F(AggregatorTest, testCreateAggFuncAggFilterFailed) {
    navi::NaviLoggerProvider provider;
    prepareTable();
    _aggregator._isLocal = true;
    ASSERT_FALSE(_aggregator.createAggFunc(_aggFuncManager, _table, "MIN", {"a"}, {"mina"}, 1));
    ASSERT_EQ("column [1] is not bool type", provider.getErrorMessage());
}

TEST_F(AggregatorTest, testCreateAggFuncGlobal) {
    prepareTable();
    _aggregator._isLocal = false;
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "IDENTITY", {"a"}, {"a"}, 4));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MIN", {"a"}, {"mina"}, 1));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MAX", {"a"}, {"maxa"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "MAX", {"a"}, {"maxb"}));
    ASSERT_TRUE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "COUNT", {"b"}, {"counta"}));
    ASSERT_TRUE(_aggregator.createAggFunc(_aggFuncManager, _table, "SUM", {"a"}, {"suma"}));
    ASSERT_TRUE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AVG", {"b", "c"}, {"avg"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AVG", {"count('a')", "c"}, {"avg"}));
    ASSERT_FALSE(_aggregator.createAggFunc(
                    _aggFuncManager, _table, "AGG", {"b", "c"}, {"avg"}));
    ASSERT_FALSE(_aggregator.createAggFunc(_aggFuncManager, _table, "AVG", {"b"}, {"avg"}));
    ASSERT_EQ(7, _aggregator._aggFilterColumn.size());
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_FALSE(_aggregator._aggFilterColumn[i]);
    }
}

TEST_F(AggregatorTest, testInitNormal) {
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._isLocal = true;
    ASSERT_TRUE(_aggregator.init(_aggFuncManager, aggFuncDesc,
                                 {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"},
                                 _table));
}

TEST_F(AggregatorTest, testInitGroupKey) {
    prepareTable();
    std::vector<AggFuncDesc> aggFuncDesc;
    _aggregator._isLocal = true;
    ASSERT_TRUE(_aggregator.init(_aggFuncManager, aggFuncDesc, {"a", "b"}, {"b"},
                                 _table));
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._isLocal = true;
    ASSERT_FALSE(_aggregator.init(_aggFuncManager, aggFuncDesc,
                                  {"b"}, {"b", "$count($a)", "$sum($a)", "$sum($a)"},
                                 _table));
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._isLocal = true;
    ASSERT_FALSE(_aggregator.init(_aggFuncManager, aggFuncDesc, {"e"}, {"e", "$sumA"},
                                  _table));
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._isLocal = true;
    ASSERT_FALSE(_aggregator.init(_aggFuncManager, aggFuncDesc, {"b"}, {"b", "$sumA"},
                                  _table));
}

TEST_F(AggregatorTest, testAggregateNormal) {
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    _aggregator._isLocal = true;
    ASSERT_TRUE(_aggregator.init(_aggFuncManager, aggFuncDesc,
                                 {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"},
                                 _table));
    ASSERT_TRUE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    auto output = _aggregator.getTable();
    cout << TableUtil::toString(output) << endl;
    auto columnData = TableUtil::getColumnData<size_t>(output, "b");
    OneDimColumnAscComparator<size_t> comparator(columnData);
    TableUtil::sort(output, &comparator);

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "count($a)", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sum($a)", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sumA", {7, 4}));
}

TEST_F(AggregatorTest, testAggregateInitInputFailed) {
    prepareTable();
    _aggregator._aggFuncVec.emplace_back(new MaxAggFunc<size_t>({"not exist"}, {"count"}, true));
    ASSERT_FALSE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
}

TEST_F(AggregatorTest, testDoAggregate) {
    prepareTable();
    _aggregator._aggFuncVec.emplace_back(new CountAggFunc({}, {"count(a)"}, true));
    _aggregator._aggFilterColumn.emplace_back(nullptr);
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(0), 0));
    ASSERT_EQ(1, _aggregator._accumulators.size());
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(1), 0));
    ASSERT_EQ(1, _aggregator._accumulators.size());
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(2), 1));
    ASSERT_EQ(2, _aggregator._accumulators.size());
}

TEST_F(AggregatorTest, testDoAggregateWithFilterArg) {
    prepareTable();
    _aggregator._aggFuncVec.emplace_back(new CountAggFunc({}, {"count(a)"}, true));
    _aggregator._aggFilterColumn.emplace_back(_table->getColumn(4)->getColumnData<bool>());
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(0), 0));
    ASSERT_EQ(1, _aggregator._accumulators.size());
    ASSERT_EQ(1, ((CountAccumulator *)_aggregator._accumulatorVec[0])->value);
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(1), 1));
    ASSERT_EQ(2, _aggregator._accumulators.size());
    ASSERT_EQ(1, ((CountAccumulator *)_aggregator._accumulatorVec[1])->value);
    ASSERT_TRUE(_aggregator.doAggregate(_table->getRow(2), 2));
    ASSERT_EQ(3, _aggregator._accumulators.size());
    ASSERT_EQ(0, ((CountAccumulator *)_aggregator._accumulatorVec[2])->value);
}

TEST_F(AggregatorTest, testGetNullTable) {
    prepareTable();
    _aggregator._table = nullptr;
    ASSERT_EQ(nullptr, _aggregator.getTable());
}

TEST_F(AggregatorTest, testExtraPlugin) {
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
    FromJsonString(aggFuncDesc, aggFuncsStr);
    // init agg func manager
    _aggFuncManager.reset(new AggFuncManager());
    suez::ResourceReaderPtr resource(new suez::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func/"));
    std::string content;
    resource->getFileContent(content, "sql.json");
    SqlAggPluginConfig config;
    FromJsonString(config, content);
    _aggFuncManager->init(resource->getConfigPath(), config);
    _aggregator._isLocal = true;
    ASSERT_TRUE(_aggregator.init(_aggFuncManager, aggFuncDesc,
                                 {"b"}, {"b", "$count($a)", "$sum($a)", "$sumA"},
                                 _table));
    ASSERT_TRUE(_aggregator.aggregate(_table, {1, 2, 1, 2, 2}));
    auto output = _aggregator.getTable();
    cout << TableUtil::toString(output) << endl;
    auto columnData = TableUtil::getColumnData<size_t>(output, "b");
    OneDimColumnAscComparator<size_t> comparator(columnData);
    TableUtil::sort(output, &comparator);

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "b", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(output, "count($a)", {2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sum($a)", {7, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(output, "sumA", {14, 8}));
}

END_HA3_NAMESPACE(sql);
