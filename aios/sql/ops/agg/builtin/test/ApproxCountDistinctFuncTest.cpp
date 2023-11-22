#include "sql/ops/agg/builtin/ApproxCountDistinctFunc.h"

#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"

using namespace autil;
using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

class ApproxCountDistinctFuncTest : public OpTestBase {
public:
    ApproxCountDistinctFuncTest() = default;
    ~ApproxCountDistinctFuncTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}

protected:
    template <typename T>
    void testLocalMode(int32_t valueCount) {
        std::vector<T> values(valueCount);
        std::iota(values.begin(), values.end(), 1);

        TablePtr table;
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, values.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<T>(_allocator, docs, "id", values));
        table = getTable(createTable(_allocator, docs));

        HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, _poolPtr.get());
        ASSERT_TRUE(ctx != NULL);
        for (size_t i = 0; i < values.size(); i++) {
            ASSERT_TRUE(
                Hyperloglog::hllCtxAdd(ctx, (unsigned char *)&values[i], sizeof(T), _poolPtr.get())
                != -1);
        }

        ApproxCountDistinctFuncCreator creator;
        ValueType inputType = ValueTypeHelper<T>::getValueType();
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
            {inputType}, {"id"}, {"hllCtx"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<ApproxCountDistinctAccumulator *>(
            func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(acc != nullptr);
        for (size_t i = 0; i < values.size(); i++) {
            ASSERT_TRUE(func->collect(table->getRow(i), acc));
        }
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        checkHllCtxOutputColumn(outputTable, "hllCtx", ctx);

        DELETE_AND_SET_NULL(func);
        POOL_DELETE_CLASS(ctx);
    }

protected:
    MatchDocAllocatorPtr _allocator;
};

TEST_F(ApproxCountDistinctFuncTest, testLocal) {
    testLocalMode<uint8_t>(10);
    testLocalMode<uint16_t>(100);
    testLocalMode<uint32_t>(500000);
    testLocalMode<uint64_t>(500000);
    testLocalMode<int8_t>(10);
    testLocalMode<int16_t>(100);
    testLocalMode<int32_t>(500000);
    testLocalMode<int64_t>(500000);
}

TEST_F(ApproxCountDistinctFuncTest, testString) {
    std::vector<std::string> values;
    for (size_t i = 0; i < 100000; i++) {
        values.emplace_back(std::to_string(i));
    }
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, values.size());
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "id", values));

    auto table = getTable(createTable(_allocator, docs));

    HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, _poolPtr.get());
    ASSERT_TRUE(ctx != NULL);
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_TRUE(Hyperloglog::hllCtxAdd(
                        ctx, (unsigned char *)values[i].data(), values[i].size(), _poolPtr.get())
                    != -1);
    }

    ApproxCountDistinctFuncCreator creator;
    ValueType inputType = ValueTypeHelper<autil::MultiChar>::getValueType();
    TablePtr outputTable(new Table(_poolPtr));
    auto func
        = creator.createFunction({inputType}, {"id"}, {"hllCtx"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc
        = static_cast<ApproxCountDistinctAccumulator *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(acc != nullptr);
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_TRUE(func->collect(table->getRow(i), acc));
    }
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    checkHllCtxOutputColumn(outputTable, "hllCtx", ctx);

    DELETE_AND_SET_NULL(func);
    POOL_DELETE_CLASS(ctx);
}

TEST_F(ApproxCountDistinctFuncTest, testInvalidColumn) {
    TablePtr table;
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
    table = getTable(createTable(_allocator, docs));

    TablePtr outputTable(new Table(_poolPtr));
    ApproxCountDistinctFuncCreator creator;
    ValueType inputType = ValueTypeHelper<uint32_t>::getValueType();
    auto func = creator.createFunction(
        {inputType}, {"invalid_column"}, {"hllCtx"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_FALSE(func->initCollectInput(table));
    DELETE_AND_SET_NULL(func);
}

TEST_F(ApproxCountDistinctFuncTest, testGlobal) {
    HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, _poolPtr.get());
    ASSERT_TRUE(ctx != NULL);
    for (size_t i = 0; i < 500000; i++) {
        ASSERT_TRUE(
            Hyperloglog::hllCtxAdd(ctx, (unsigned char *)&i, sizeof(int64_t), _poolPtr.get())
            != -1);
    }
    TablePtr table = std::make_shared<Table>(_poolPtr);
    table->batchAllocateRow(1);
    auto column = table->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(column != nullptr);
    auto columnData = column->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData != nullptr);
    columnData->set(0, *ctx);

    ApproxCountDistinctFuncCreator creator;
    ValueType inputType = ValueTypeHelper<autil::HllCtx>::getValueType();

    TablePtr outputTable(new Table(_poolPtr));
    auto func = creator.createFunction(
        {inputType}, {"ctx_field"}, {"output"}, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    ASSERT_TRUE(func->initResultOutput(outputTable));
    ASSERT_TRUE(func->initMergeInput(table));
    auto acc
        = static_cast<ApproxCountDistinctAccumulator *>(func->createAccumulator(_poolPtr.get()));

    ASSERT_TRUE(func->merge(table->getRow(0), acc));
    ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn<uint64_t>(outputTable, "output", {autil::Hyperloglog::hllCtxCount(ctx)}));

    DELETE_AND_SET_NULL(func);
    POOL_DELETE_CLASS(ctx);
}

} // namespace sql
