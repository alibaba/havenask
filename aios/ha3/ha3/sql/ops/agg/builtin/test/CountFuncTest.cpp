#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class CountFuncTest : public OpTestBase {
public:
    CountFuncTest();
    ~CountFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
};

CountFuncTest::CountFuncTest() {
}

CountFuncTest::~CountFuncTest() {
}

TEST_F(CountFuncTest, testCountFuncCreatorLocal) {
    CountAggFuncCreator creator;
    {
        auto func = dynamic_cast<CountAggFunc *>(
                creator.createFunction({}, {}, {"count"}, true));
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"count"}, true);
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = creator.createFunction({}, {}, {"count", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(CountFuncTest, testCountFuncCreatorGlobal) {
    CountAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<uint64_t>::getValueType();
    {
        auto func = dynamic_cast<CountAggFunc *>(creator.createFunction(
                        {countType}, {"count"}, {"count"}, false));
        ASSERT_TRUE(nullptr != func);
        delete func;
    }
    {
        auto func = creator.createFunction(
                {countType, ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"count", "sum"}, {"count"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({ValueTypeHelper<int32_t>::getValueType()},
                {"count"}, {"count"}, false);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = creator.createFunction({countType}, {"count"}, {"count", "b"}, false);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(CountFuncTest, testLocal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    CountAggFuncCreator creator;
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction({}, {}, {"count"}, true);
        ASSERT_TRUE(nullptr != func);
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
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(outputTable, "count", {3}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("count");
        auto func = creator.createFunction(
                {}, {}, {"count"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(CountFuncTest, testGlobal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "key", {1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<size_t>(_allocator, docs, "count", {2, 3}));
        table = getTable(createTable(_allocator, docs));
    }
    CountAggFuncCreator creator;
    ValueType countType = ValueTypeHelper<size_t>::getValueType();
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction(
                {countType}, {"count"}, {"count"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_TRUE(func->initMergeInput(table));
        auto acc = static_cast<CountAccumulator *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->merge(table->getRow(0), acc));
        ASSERT_EQ(2, acc->value);
        ASSERT_TRUE(func->merge(table->getRow(1), acc));
        ASSERT_EQ(5, acc->value);
        ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<size_t>(outputTable, "count", {5}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = creator.createFunction({countType}, {"countA"}, {"count"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("count");
        auto func = creator.createFunction({countType}, {"count"}, {"count"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initResultOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

END_HA3_NAMESPACE(sql);
