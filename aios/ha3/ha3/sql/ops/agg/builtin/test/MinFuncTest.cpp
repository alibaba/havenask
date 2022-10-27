#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/MinAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class MinFuncTest : public OpTestBase {
public:
    MinFuncTest();
    ~MinFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
    MinAggFuncCreator _creator;
};

MinFuncTest::MinFuncTest() {
}

MinFuncTest::~MinFuncTest() {
}

TEST_F(MinFuncTest, testMinFuncCreatorLocal) {
    auto vt = ValueTypeHelper<uint32_t>::getValueType();
    {
        auto func = dynamic_cast<MinAggFunc<uint32_t> *>(
                _creator.createFunction({vt}, {"a"}, {"min"}, true));
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = dynamic_cast<MinAggFunc<uint32_t> *>(
                _creator.createFunction({vt, vt}, {"a", "b"}, {"min"}, true));
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"a"}, {"min"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"min", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(MinFuncTest, testLocal) {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"min"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<MinAccumulator<uint32_t> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(2, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(2, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "min", {2}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"b"}, {"min"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint16_t>::getValueType()},
                {"a"}, {"min"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("min");
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"min"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(MinFuncTest, testNegative) {
    TablePtr table;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {-3, -2, -4}));
    table = getTable(createTable(_allocator, docs));
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction({ValueTypeHelper<int32_t>::getValueType()},
            {"a"}, {"min"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc = static_cast<MinAccumulator<int32_t> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(-3, acc->value);
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(-3, acc->value);
    ASSERT_TRUE(func->collect(table->getRow(2), acc));
    ASSERT_EQ(-4, acc->value);
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "min", {-4}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

TEST_F(MinFuncTest, testMulti) {
    TablePtr table;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "a",
                    {{2, 1}, {2, 3}, {2, 3, -1}}));
    table = getTable(createTable(_allocator, docs));
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction({ValueTypeHelper<autil::MultiInt32>::getValueType()},
            {"a"}, {"min"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc = static_cast<MinAccumulator<autil::MultiInt32> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 1}, acc->value));
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 1}, acc->value));
    ASSERT_TRUE(func->collect(table->getRow(2), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 1}, acc->value));
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(outputTable, "min", {{2, 1}}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

END_HA3_NAMESPACE(sql);
