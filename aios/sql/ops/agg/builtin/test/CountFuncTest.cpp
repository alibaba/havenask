#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "sql/ops/agg/builtin/CountAggFunc.h"
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

namespace sql {

class CountFuncTest : public OpTestBase {
public:
    CountFuncTest();
    ~CountFuncTest();

public:
    void setUp() override {}
    void tearDown() override {}

public:
    MatchDocAllocatorPtr _allocator;
};

CountFuncTest::CountFuncTest() {}

CountFuncTest::~CountFuncTest() {}

TEST_F(CountFuncTest, testCountFuncCreatorLocal) {
    CountAggFuncCreator creator;
    {
        auto func = dynamic_cast<CountAggFunc *>(
            creator.createFunction({}, {}, {"count"}, AggFuncMode::AGG_FUNC_MODE_LOCAL));
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                                           {"a"},
                                           {"count"},
                                           AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func
            = creator.createFunction({}, {}, {"count", "sum"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(CountFuncTest, testCountFuncCreatorGlobal) {
    CountAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<int64_t>::getValueType();
    {
        auto func = dynamic_cast<CountAggFunc *>(creator.createFunction(
            {countType}, {"count"}, {"count"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL));
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        delete func;
    }
    {
        auto func
            = creator.createFunction({countType, ValueTypeHelper<autil::MultiChar>::getValueType()},
                                     {"count", "sum"},
                                     {"count"},
                                     AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<int32_t>::getValueType()},
                                           {"count"},
                                           {"count"},
                                           AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction(
            {countType}, {"count"}, {"count", "b"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(CountFuncTest, testLocal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    CountAggFuncCreator creator;
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction({}, {}, {"count"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<CountAccumulator *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(1, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(2, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "count", {3}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("count");
        auto func = creator.createFunction({}, {}, {"count"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(CountFuncTest, testGlobal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<size_t>(_allocator, docs, "key", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "count", {2, 3}));
        table = getTable(createTable(_allocator, docs));
    }
    CountAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<int64_t>::getValueType();
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
            {countType}, {"count"}, {"count"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_TRUE(func->initMergeInput(table));
        auto acc = static_cast<CountAccumulator *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->merge(table->getRow(0), acc));
        ASSERT_EQ(2, acc->value);
        ASSERT_TRUE(func->merge(table->getRow(1), acc));
        ASSERT_EQ(5, acc->value);
        ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "count", {5}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
            {countType}, {"countA"}, {"count"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("count");
        auto func = creator.createFunction(
            {countType}, {"count"}, {"count"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_FALSE(func->initResultOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

} // namespace sql
