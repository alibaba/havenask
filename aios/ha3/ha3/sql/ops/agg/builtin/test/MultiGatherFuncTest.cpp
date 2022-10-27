#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/builtin/MultiGatherAggFunc.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);

class MultiGatherAggFuncTest : public OpTestBase {
public:
    MultiGatherAggFuncTest();
    ~MultiGatherAggFuncTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
public:
    MatchDocAllocatorPtr _allocator;
    MultiGatherAggFuncCreator _creator;
public:
    template <typename T>
    void testLocalInt();
    template <typename T>
    void testLocalFloat();
    template <typename T>
    void testGlobalInt();
    template <typename T>
    void testGlobalFloat();

    void testLocalStr();
    void testGlobalStr();
};

MultiGatherAggFuncTest::MultiGatherAggFuncTest() {
}

MultiGatherAggFuncTest::~MultiGatherAggFuncTest() {
}

TEST_F(MultiGatherAggFuncTest, testMultiGatherAggFuncCreator) {
    auto vt = ValueTypeHelper<MultiString>::getValueType();
    {
        auto func = dynamic_cast<MultiGatherAggFunc<MultiChar> *>(
                _creator.createFunction({vt}, {"a"}, {"gather"}, false));
        ASSERT_TRUE(nullptr != func);
        func = dynamic_cast<MultiGatherAggFunc<MultiChar> *>(
                _creator.createFunction({vt}, {"a"}, {"gather"}, true));
        ASSERT_TRUE(nullptr != func);
        DELETE_AND_SET_NULL(func);
    }
    // inputFields != 1
    {
        auto func = _creator.createFunction({vt, vt}, {"a", "b"}, {"gather"}, false);
        ASSERT_EQ(nullptr, func);
        func = _creator.createFunction({vt, vt}, {"a", "b"}, {"gather"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({}, {}, {"gather"}, false);
        ASSERT_EQ(nullptr, func);
        func = _creator.createFunction({}, {}, {"gather"}, true);
        ASSERT_EQ(nullptr, func);
    }
    // outputFields != 1
    {
        auto func = _creator.createFunction({vt}, {"a"}, {"gather1", "gather2"}, false);
        ASSERT_EQ(nullptr, func);
        func = _creator.createFunction({vt}, {"a"}, {"gather1", "gather2"}, true);
        ASSERT_EQ(nullptr, func);
    }
    {
        auto func = _creator.createFunction({vt}, {"a"}, {}, false);
        ASSERT_EQ(nullptr, func);
        func = _creator.createFunction({vt}, {"a"}, {}, true);
        ASSERT_EQ(nullptr, func);
    }
    // inputType is single Value
    {
        auto func = _creator.createFunction(
                {ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"gather"}, false);
        ASSERT_EQ(nullptr, func);
        func = _creator.createFunction(
                {ValueTypeHelper<uint32_t>::getValueType()},
                {"a"}, {"gather"}, true);
        ASSERT_EQ(nullptr, func);
    }
}

template <typename T>
void MultiGatherAggFuncTest::testLocalInt() {
    TablePtr table;
    vector<T> data1 = {1, 2};
    vector<T> data2 = {3, 4};
    MultiValueType<T> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<T> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));

    MultiGatherAggFuncCreator creator;
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = creator.createFunction(
            {ValueTypeHelper<MultiValueType<T> >::getValueType()},
            {"a"}, {"gather"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initCollectInput(table));
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    auto acc = static_cast<MultiGatherAccumulator<T> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));

    vector<T> data = {1, 2, 3, 4};
    MultiValueType<T> multiVal;
    multiVal.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<T> >(outputTable, "gather", {multiVal}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

template <typename T>
void MultiGatherAggFuncTest::testLocalFloat() {
    TablePtr table;
    vector<T> data1 = {1.1, 2.1};
    vector<T> data2 = {3.1, 4.1};
    MultiValueType<T> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<T> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));

    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction(
            {ValueTypeHelper<MultiValueType<T> >::getValueType()},
            {"a"}, {"gather"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initCollectInput(table));
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    auto acc = static_cast<MultiGatherAccumulator<T> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));

    vector<T> data = {1.1, 2.1, 3.1, 4.1};
    MultiValueType<T> multiVal;
    multiVal.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<T> >(outputTable, "gather", {multiVal}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

void MultiGatherAggFuncTest::testLocalStr() {
    TablePtr table;
    vector<string> dataStr1 = {"str1","str2"};
    vector<string> dataStr2 = {"str3","str4"};
    MultiValueType<MultiChar> multiStr1, multiStr2;
    multiStr1.init(MultiValueCreator::createMultiValueBuffer(dataStr1, _poolPtr.get()));
    multiStr2.init(MultiValueCreator::createMultiValueBuffer(dataStr2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<MultiChar> >(
                    _allocator, docs, "str", {multiStr1, multiStr2}));
    table = getTable(createTable(_allocator, docs));
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction(
            {ValueTypeHelper<MultiString>::getValueType()},
            {"str"}, {"gather"}, true);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initCollectInput(table));
    ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
    auto acc = static_cast<MultiGatherAccumulator<MultiChar> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->collect(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->collect(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));

    vector<string> data = {"str1","str2","str3","str4"};
    MultiString multiStr;
    multiStr.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<MultiChar> >(outputTable, "gather", {multiStr}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

TEST_F(MultiGatherAggFuncTest, testLocalNormal) {
    testLocalInt<uint8_t>();
    testLocalInt<int8_t>();
    testLocalInt<uint16_t>();
    testLocalInt<int16_t>();
    testLocalInt<uint32_t>();
    testLocalInt<int32_t>();
    testLocalInt<uint64_t>();
    testLocalInt<int64_t>();
    testLocalFloat<float>();
    testLocalFloat<double>();
    testLocalStr();
}

TEST_F(MultiGatherAggFuncTest, testLocalInvalid) {
    TablePtr table;
    vector<uint32_t> data1 = {1, 2};
    vector<uint32_t> data2 = {3, 4};
    MultiValueType<uint32_t> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<uint32_t> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<uint32_t> >::getValueType()},
                {"b"}, {"gather"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initCollectInput(table));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<int64_t> >::getValueType()},
                {"a"}, {"gather"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initCollectInput(table));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<MultiValueType<uint32_t> >("gather");
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<uint32_t> >::getValueType()},
                {"a"}, {"gather"}, true);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

template <typename T>
void MultiGatherAggFuncTest::testGlobalInt() {
    TablePtr table;
    vector<T> data1 = {1, 2};
    vector<T> data2 = {3, 4};
    MultiValueType<T> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<T> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));

    MultiGatherAggFuncCreator creator;
    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = creator.createFunction(
            {ValueTypeHelper<MultiValueType<T> >::getValueType()},
            {"a"}, {"gather"}, false);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initResultOutput(outputTable));
    ASSERT_TRUE(func->initMergeInput(table));
    auto acc = static_cast<MultiGatherAccumulator<T> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->merge(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->merge(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));

    vector<T> data = {1, 2, 3, 4};
    MultiValueType<T> multiVal;
    multiVal.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<T> >(outputTable, "gather", {multiVal}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

template <typename T>
void MultiGatherAggFuncTest::testGlobalFloat() {
    TablePtr table;
    vector<T> data1 = {1.1, 2.1};
    vector<T> data2 = {3.1, 4.1};
    MultiValueType<T> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<T> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));

    // test normal
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction(
            {ValueTypeHelper<MultiValueType<T> >::getValueType()},
            {"a"}, {"gather"}, false);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initResultOutput(outputTable));
    ASSERT_TRUE(func->initMergeInput(table));
    auto acc = static_cast<MultiGatherAccumulator<T> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->merge(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->merge(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));

    vector<T> data = {1.1, 2.1, 3.1, 4.1};
    MultiValueType<T> multiVal;
    multiVal.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<T> >(outputTable, "gather", {multiVal}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

void MultiGatherAggFuncTest::testGlobalStr() {
    TablePtr table;
    vector<string> dataStr1 = {"str1","str2"};
    vector<string> dataStr2 = {"str3","str4"};
    MultiValueType<MultiChar> multiStr1, multiStr2;
    multiStr1.init(MultiValueCreator::createMultiValueBuffer(dataStr1, _poolPtr.get()));
    multiStr2.init(MultiValueCreator::createMultiValueBuffer(dataStr2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<MultiChar> >(
                    _allocator, docs, "str", {multiStr1, multiStr2}));
    table = getTable(createTable(_allocator, docs));
    TablePtr outputTable(new Table(_poolPtr));
    auto func = _creator.createFunction(
            {ValueTypeHelper<MultiString>::getValueType()},
            {"str"}, {"gather"}, false);
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->initResultOutput(outputTable));
    ASSERT_TRUE(func->initMergeInput(table));
    auto acc = static_cast<MultiGatherAccumulator<MultiChar> *>(func->createAccumulator(_poolPtr.get()));
    ASSERT_TRUE(func->merge(table->getRow(0), acc));
    ASSERT_EQ(2, acc->poolVector.size());
    ASSERT_TRUE(func->merge(table->getRow(1), acc));
    ASSERT_EQ(4, acc->poolVector.size());
    ASSERT_TRUE(func->outputResult(acc, outputTable->allocateRow()));

    vector<string> data = {"str1","str2","str3","str4"};
    MultiString multiStr;
    multiStr.init(MultiValueCreator::createMultiValueBuffer(data, _poolPtr.get()));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<MultiValueType<MultiChar> >(outputTable, "gather", {multiStr}));
    POOL_DELETE_CLASS(acc);
    DELETE_AND_SET_NULL(func);
}

TEST_F(MultiGatherAggFuncTest, testGlobalNormal) {
    testGlobalInt<uint8_t>();
    testGlobalInt<int8_t>();
    testGlobalInt<uint16_t>();
    testGlobalInt<int16_t>();
    testGlobalInt<uint32_t>();
    testGlobalInt<int32_t>();
    testGlobalInt<uint64_t>();
    testGlobalInt<int64_t>();
    testGlobalFloat<float>();
    testGlobalFloat<double>();
    testGlobalStr();
}

TEST_F(MultiGatherAggFuncTest, testGlobalInvalid) {
    TablePtr table;
    vector<uint32_t> data1 = {1, 2};
    vector<uint32_t> data2 = {3, 4};
    MultiValueType<uint32_t> multiVal1, multiVal2;
    multiVal1.init(MultiValueCreator::createMultiValueBuffer(data1, _poolPtr.get()));
    multiVal2.init(MultiValueCreator::createMultiValueBuffer(data2, _poolPtr.get()));
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<uint32_t> >(
                    _allocator, docs, "a", {multiVal1, multiVal2}));
    table = getTable(createTable(_allocator, docs));
    {
        // invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<uint32_t> >::getValueType()},
                {"b"}, {"gather"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<int64_t> >::getValueType()},
                {"a"}, {"gather"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->initResultOutput(outputTable));
        ASSERT_FALSE(func->initMergeInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<MultiValueType<uint32_t> >("gather");
        auto func = _creator.createFunction(
                {ValueTypeHelper<MultiValueType<uint32_t> >::getValueType()},
                {"a"}, {"gather"}, false);
        ASSERT_TRUE(nullptr != func);
        ASSERT_FALSE(func->initResultOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

END_HA3_NAMESPACE(sql);
