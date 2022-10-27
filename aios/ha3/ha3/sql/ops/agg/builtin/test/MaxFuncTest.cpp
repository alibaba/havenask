#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/MaxAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class MaxFuncTest : public OpTestBase {
public:
    MaxFuncTest();
    ~MaxFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
    MaxAggFuncCreator _creator;
};

MaxFuncTest::MaxFuncTest() {
}

MaxFuncTest::~MaxFuncTest() {
}

TEST_F(MaxFuncTest, testMaxFuncCreatorLocal) {
    auto vt = ValueTypeHelper<uint32_t>::getValueType();
    {
        auto func = dynamic_cast<MaxAggFunc<uint32_t> *>(
                _creator.createFunction({vt}, {"a"}, {"max"}, true));
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    {
        auto func = dynamic_cast<MaxAggFunc<uint32_t> *>(
                _creator.createFunction({vt, vt}, {"a", "b"}, {"max"}, true));
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"a"}, {"max"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"max", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

TEST_F(MaxFuncTest, testLocal) {
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
                {"a"}, {"max"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<MaxAccumulator<uint32_t> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(4, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "max", {4}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"b"}, {"max"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint16_t>::getValueType()},
                {"a"}, {"max"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("max");
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"max"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(MaxFuncTest, testNegative) {
    TablePtr table;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {-3, -2, -4}));
    table = getTable(createTable(_allocator, docs));
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction({ValueTypeHelper<int32_t>::getValueType()},
            {"a"}, {"max"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc = static_cast<MaxAccumulator<int32_t> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(-3, acc->value);
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(-2, acc->value);
    ASSERT_TRUE(func->collect(table->getRow(2), acc));
    ASSERT_EQ(-2, acc->value);
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "max", {-2}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

TEST_F(MaxFuncTest, testMulti) {
    TablePtr table;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "a",
                    {{2, 1}, {2, 3}, {2, 3, -1}}));
    table = getTable(createTable(_allocator, docs));
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction({ValueTypeHelper<autil::MultiInt32>::getValueType()},
            {"a"}, {"max"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    ASSERT_TRUE(func->initCollectInput(table));
    auto acc = static_cast<MaxAccumulator<autil::MultiInt32> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 1}, acc->value));
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 3}, acc->value));
    ASSERT_TRUE(func->collect(table->getRow(2), acc));
    ASSERT_NO_FATAL_FAILURE(checkMultiValue<int32_t>({2, 3, -1}, acc->value));
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(outputTable, "max", {{2, 3, -1}}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

END_HA3_NAMESPACE(sql);
