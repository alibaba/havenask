#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/AvgAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class AvgFuncTest : public OpTestBase {
public:
    AvgFuncTest();
    ~AvgFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
};

AvgFuncTest::AvgFuncTest() {
}

AvgFuncTest::~AvgFuncTest() {
}

TEST_F(AvgFuncTest, testAvgFuncCreatorLocal) {
    AvgAggFuncCreator creator;
    {
        auto func = dynamic_cast<AvgAggFunc<uint32_t, uint64_t> *>(
                creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                        {"a"}, {"count", "sum"}, true));
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"a"}, {"count", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<uint32_t>::getValueType(), ValueType()},
                {"a", "b"}, {"count", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<autil::MultiInt16>::getValueType()},
                {"a"}, {"count", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(AvgFuncTest, testAvgFuncCreatorGlobal) {
    AvgAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<size_t>::getValueType();
    {
        auto func = dynamic_cast<AvgAggFunc<int, int64_t> *>(creator.createFunction(
                        {countType, ValueTypeHelper<int64_t>::getValueType()},
                        {"count", "sum"}, {"a"}, false));
        ASSERT_TRUE(nullptr != func);
        delete func;
    }
    {
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"count", "sum"}, {"a"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({countType, ValueTypeHelper<int32_t>::getValueType()},
                {"count", "sum"}, {"a"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<autil::MultiInt64>::getValueType()},
                {"count", "sum"}, {"a"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<int64_t>::getValueType()},
                {"sum"}, {"a"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({countType, ValueTypeHelper<int64_t>::getValueType()},
                {"count", "sum"}, {"a", "b"}, false);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(AvgFuncTest, testLocal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    AvgAggFuncCreator creator;
    ValueType inputType = ValueTypeHelper<uint32_t>::getValueType();
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {inputType}, {"a"}, {"count", "sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<AvgAccumulator<uint64_t> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(1, acc->count);
        ASSERT_EQ(3, acc->sum);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(2, acc->count);
        ASSERT_EQ(5, acc->sum);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(3, acc->count);
        ASSERT_EQ(9, acc->sum);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(outputTable, "count", {3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint64_t>(outputTable, "sum", {9}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {inputType}, {"b"}, {"count", "sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {ValueTypeHelper<double>::getValueType()}, {"a"}, {"count", "sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("count");
        auto func = creator.createFunction(
                {inputType}, {"a"}, {"count", "sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(AvgFuncTest, testGlobal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "key", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "countA", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint64_t>(_allocator, docs, "sumA", {7, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    AvgAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<size_t>::getValueType();
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<uint64_t>::getValueType()},
                {"countA", "sumA"}, {"avgA"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_TRUE(func->initMergeInput(table));
        auto acc = static_cast<AvgAccumulator<uint64_t> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->merge(table->getRow(0), acc));
        ASSERT_EQ(2, acc->count);
        ASSERT_EQ(7, acc->sum);
        ASSERT_TRUE(func->merge(table->getRow(1), acc));
        ASSERT_EQ(5, acc->count);
        ASSERT_EQ(11, acc->sum);
        ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(outputTable, "avgA", {11.0 / 5}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<uint64_t>::getValueType()},
                {"count", "sumA"}, {"avgA"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<int64_t>::getValueType()},
                {"countA", "sumA"}, {"avgA"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("avgA");
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<int64_t>::getValueType()},
                {"countA", "sumA"}, {"avgA"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initResultOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(AvgFuncTest, testBigNumber) {
    TablePtr table;
    int32_t maxint = std::numeric_limits<int>::max();
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {maxint, maxint, maxint}));
        table = getTable(createTable(_allocator, docs));
    }
    AvgAggFuncCreator creator;
    ValueType inputType = ValueTypeHelper<int32_t>::getValueType();
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = creator.createFunction(
            {inputType}, {"a"}, {"count", "sum"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc = static_cast<AvgAccumulator<uint64_t> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(1, acc->count);
    ASSERT_EQ(maxint, acc->sum);
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(2, acc->count);
    ASSERT_EQ(2l * maxint, acc->sum);
    ASSERT_TRUE(func->collect(table->getRow(2), acc));
    ASSERT_EQ(3, acc->count);
    ASSERT_EQ(3l * maxint, acc->sum);
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(outputTable, "count", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sum", {3l * maxint}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

END_HA3_NAMESPACE(sql);
