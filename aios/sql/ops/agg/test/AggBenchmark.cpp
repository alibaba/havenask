#include <algorithm>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "build_service/config/AgentGroupConfig.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/engine/ResourceMap.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/agg/AggFuncDesc.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "sql/ops/agg/AggNormal.h"
#include "sql/ops/agg/SqlAggPluginConfig.h" // IWYU pragma: keep
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace build_service::config;

namespace sql {

class AggBenchmark : public benchmark::Fixture {
public:
    AggBenchmark()
        : _poolPtr(new autil::mem_pool::Pool)
        , _matchDocUtil(_poolPtr) {}

public:
    void SetUp(const ::benchmark::State &state) {
        _aggFuncFactoryR = _naviHelper.getOrCreateRes<AggFuncFactoryR>();
        ASSERT_NE(nullptr, _aggFuncFactoryR);
        _graphMemoryPoolR = _naviHelper.getOrCreateRes<navi::GraphMemoryPoolR>();
        ASSERT_NE(nullptr, _graphMemoryPoolR);
    }

    void createTableForAvg(TablePtr &table, size_t total, size_t group) {
        MatchDocAllocatorPtr allocator;
        auto matchDocs = _matchDocUtil.createMatchDocs(allocator, total);
        vector<size_t> gkValues(matchDocs.size());
        vector<double> rawValues(matchDocs.size());
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            gkValues[i] = i % group;
            rawValues[i] = i;
        }
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, matchDocs, "gk", gkValues));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, matchDocs, "raw", rawValues));
        table.reset(new table::Table(matchDocs, allocator));
    }

    void createTableForGather(TablePtr &table) {
        MatchDocAllocatorPtr allocator;
        auto matchDocs = _matchDocUtil.createMatchDocs(allocator, 50);
        vector<size_t> gkValues(matchDocs.size());
        vector<string> rawValues(matchDocs.size());
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            gkValues[i] = i;
            rawValues[i] = string("aaa") + to_string(i);
        }
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, matchDocs, "gk", gkValues));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, matchDocs, "raw", rawValues));
        table.reset(new table::Table(matchDocs, allocator));
    }

private:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::MatchDocUtil _matchDocUtil;
    navi::ResourceMapPtr _testResourceMap;
    navi::GraphMemoryPoolR *_graphMemoryPoolR = nullptr;
    navi::NaviResourceHelper _naviHelper;
    AggFuncFactoryR *_aggFuncFactoryR = nullptr;
};

BENCHMARK_F(AggBenchmark, testAvgAggNormal)(benchmark::State &state) {
    TablePtr inputTable;
    ASSERT_NO_FATAL_FAILURE(createTableForAvg(inputTable, 5000, 50));

    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$raw"],
            "output" : ["$avg"],
            "type" : "NORMAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);

    for (auto _ : state) {
        state.PauseTiming();
        AggNormal aggNormal(_graphMemoryPoolR);
        ASSERT_TRUE(aggNormal.init(_aggFuncFactoryR, {"gk"}, {"avg", "gk"}, aggFuncDesc));
        state.ResumeTiming();

        for (size_t i = 0; i < 100; ++i) {
            ASSERT_TRUE(aggNormal.compute(inputTable));
        }
        ASSERT_TRUE(aggNormal.finalize());
        TablePtr table = aggNormal.getTable();
        ASSERT_NE(nullptr, table);
        benchmark::DoNotOptimize(table);
    }
}

BENCHMARK_F(AggBenchmark, testAvgAggNormal2)(benchmark::State &state) {
    TablePtr inputTable;
    ASSERT_NO_FATAL_FAILURE(createTableForAvg(inputTable, 2000000, 1000000));

    string aggFuncsStr = R"json([
        {
            "name" : "AVG",
            "input" : ["$raw"],
            "output" : ["$avg"],
            "type" : "NORMAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);

    for (auto _ : state) {
        state.PauseTiming();
        AggNormal aggNormal(_graphMemoryPoolR);
        ASSERT_TRUE(aggNormal.init(_aggFuncFactoryR, {"gk"}, {"avg", "gk"}, aggFuncDesc));
        state.ResumeTiming();

        for (size_t i = 0; i < 2; ++i) {
            ASSERT_TRUE(aggNormal.compute(inputTable));
        }
        ASSERT_TRUE(aggNormal.finalize());
        TablePtr table = aggNormal.getTable();
        ASSERT_NE(nullptr, table);
        benchmark::DoNotOptimize(table);
    }
}

BENCHMARK_F(AggBenchmark, testGatherAggNormal)(benchmark::State &state) {
    TablePtr inputTable;
    ASSERT_NO_FATAL_FAILURE(createTableForGather(inputTable));

    string aggFuncsStr = R"json([
        {
            "name" : "GATHER",
            "input" : ["$raw"],
            "output" : ["$out"],
            "type" : "NORMAL"
        }
    ])json";
    std::vector<AggFuncDesc> aggFuncDesc;
    FastFromJsonString(aggFuncDesc, aggFuncsStr);

    for (auto _ : state) {
        state.PauseTiming();
        AggNormal aggNormal(_graphMemoryPoolR);
        ASSERT_TRUE(aggNormal.init(_aggFuncFactoryR, {"gk"}, {"out", "gk"}, aggFuncDesc));
        state.ResumeTiming();

        for (size_t i = 0; i < 10; ++i) {
            ASSERT_TRUE(aggNormal.compute(inputTable));
        }
        ASSERT_TRUE(aggNormal.finalize());
        TablePtr table = aggNormal.getTable();
        ASSERT_NE(nullptr, table);
        benchmark::DoNotOptimize(table);
    }
}

} // namespace sql
