#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/SumAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class SumFuncTest : public OpTestBase {
public:
    SumFuncTest();
    ~SumFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
    SumAggFuncCreator _creator;
private:
    template<typename T, typename V>
    void testCreateFuncLocal();
    template<typename T, typename V>
    void testLocalNormalInt();
    template<typename T, typename V>
    void testLocalNormalFloat();
};

SumFuncTest::SumFuncTest() {
}

SumFuncTest::~SumFuncTest() {
}

template<typename T, typename V>
void SumFuncTest::testCreateFuncLocal() {
    auto vt = ValueTypeHelper<T>::getValueType();
    auto func = dynamic_cast<SumAggFunc<T, V> *>(
            _creator.createFunction({vt}, {"a"}, {"sum"}, true));
    ASSERT_TRUE(nullptr != func);
    DELETE_AND_SET_NULL(func);
}

TEST_F(SumFuncTest, testSumFuncCreatorLocal) {
    testCreateFuncLocal<int8_t, int64_t>();
    testCreateFuncLocal<int16_t, int64_t>();
    testCreateFuncLocal<int32_t, int64_t>();
    testCreateFuncLocal<int64_t, int64_t>();
    testCreateFuncLocal<uint8_t, uint64_t>();
    testCreateFuncLocal<uint16_t, uint64_t>();
    testCreateFuncLocal<uint32_t, uint64_t>();
    testCreateFuncLocal<uint64_t, uint64_t>();
    testCreateFuncLocal<float, float>();
    testCreateFuncLocal<double, double>();
    {
        auto vt = ValueTypeHelper<uint32_t>::getValueType();
        auto func = _creator.createFunction({vt, vt}, {"a", "b"}, {"sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<autil::MultiChar>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<autil::MultiInt16>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"sum", "sum"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

template<typename T, typename V>
void SumFuncTest::testLocalNormalInt() {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<T>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<T>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<T>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<SumAccumulator<V> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(5, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(9, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<V>(outputTable, "sum", {9}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
}

template<typename T, typename V>
void SumFuncTest::testLocalNormalFloat() {
    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<T>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<T>(_allocator, docs, "a", {3.1, 2.2, 4.3}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<T>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<SumAccumulator<V> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_DOUBLE_EQ((V)3.1, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_DOUBLE_EQ((V)5.3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_DOUBLE_EQ((V)9.6, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<V>(outputTable, "sum", {(V)9.6}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(SumFuncTest, testLocal) {
    testLocalNormalInt<int8_t, int64_t>();
    testLocalNormalInt<int16_t, int64_t>();
    testLocalNormalInt<int32_t, int64_t>();
    testLocalNormalInt<int64_t, int64_t>();
    testLocalNormalInt<uint8_t, uint64_t>();
    testLocalNormalInt<uint16_t, uint64_t>();
    testLocalNormalInt<uint32_t, uint64_t>();
    testLocalNormalInt<uint64_t, uint64_t>();
    testLocalNormalFloat<float, float>();
    testLocalNormalFloat<double, double>();

    TablePtr table;
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"b"}, {"sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint16_t>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("sum");
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"sum"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

END_HA3_NAMESPACE(sql);
