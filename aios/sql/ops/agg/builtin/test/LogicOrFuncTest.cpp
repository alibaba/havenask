#include <algorithm>
#include <cstdint>
#include <iosfwd>
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
#include "sql/ops/agg/builtin/LogicAggFunc.h"
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

class LogicOrFuncTest : public OpTestBase {
public:
    LogicOrFuncTest();
    ~LogicOrFuncTest();

public:
    void setUp() override {}
    void tearDown() override {}

public:
    MatchDocAllocatorPtr _allocator;
    LogicOrAggFuncCreator _creator;

private:
    template <typename T>
    void testCreateFuncLocalNormal();
    template <typename T>
    void testCreateFuncLocalAbnormal();
    template <typename T>
    void testLocalNormal();
};

LogicOrFuncTest::LogicOrFuncTest() {}

LogicOrFuncTest::~LogicOrFuncTest() {}

template <typename T>
void LogicOrFuncTest::testCreateFuncLocalNormal() {
    auto vt = ValueTypeHelper<T>::getValueType();
    auto func = dynamic_cast<LogicAggFunc<T> *>(
        _creator.createFunction({vt}, {"a"}, {"logic_or"}, AggFuncMode::AGG_FUNC_MODE_LOCAL));
    ASSERT_TRUE(nullptr != func);
    ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
    ASSERT_EQ(0, func->_initAccVal);
    DELETE_AND_SET_NULL(func);
}

template <typename T>
void LogicOrFuncTest::testCreateFuncLocalAbnormal() {
    auto vt = ValueTypeHelper<T>::getValueType();
    auto func
        = _creator.createFunction({vt}, {"a"}, {"logic_or"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_EQ(nullptr, func);
}

TEST_F(LogicOrFuncTest, testLogicOrFuncCreatorLocal) {
    testCreateFuncLocalNormal<uint8_t>();
    testCreateFuncLocalNormal<uint16_t>();
    testCreateFuncLocalNormal<uint32_t>();
    testCreateFuncLocalNormal<uint64_t>();

    testCreateFuncLocalAbnormal<int8_t>();
    testCreateFuncLocalAbnormal<int16_t>();
    testCreateFuncLocalAbnormal<int32_t>();
    testCreateFuncLocalAbnormal<int64_t>();
    testCreateFuncLocalAbnormal<float>();
    testCreateFuncLocalAbnormal<double>();
    testCreateFuncLocalAbnormal<string>();
    testCreateFuncLocalAbnormal<autil::MultiInt8>();
    testCreateFuncLocalAbnormal<autil::MultiInt16>();
    testCreateFuncLocalAbnormal<autil::MultiInt32>();
    testCreateFuncLocalAbnormal<autil::MultiInt64>();

    // input fields size != 1
    {
        auto vt = ValueTypeHelper<uint32_t>::getValueType();
        auto func = _creator.createFunction(
            {vt, vt}, {"a", "b"}, {"logic_or"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_EQ(nullptr, func);
    }
    // output fields size != 1
    {
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                                            {"a"},
                                            {"logic_or", "logic_or"},
                                            AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_EQ(nullptr, func);
    }
}

template <typename T>
void LogicOrFuncTest::testLocalNormal() {
    TablePtr table;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<T>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<T>(_allocator, docs, "a", {1, 3, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test normal
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<T>::getValueType()},
                                            {"a"},
                                            {"logic_or"},
                                            AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_TRUE(func->initCollectInput(table));
        auto acc = static_cast<LogicAccumulator<T> *>(func->createAccumulator(_poolPtr.get()));
        ASSERT_TRUE(func->collect(table->getRow(0), acc));
        ASSERT_EQ(1, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(1), acc));
        ASSERT_EQ(3, acc->value);
        ASSERT_TRUE(func->collect(table->getRow(2), acc));
        ASSERT_EQ(7, acc->value);
        ASSERT_TRUE(func->outputAccumulator(acc, outputTable->allocateRow()));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<T>(outputTable, "logic_or", {7}));
        POOL_DELETE_CLASS(acc);
        DELETE_AND_SET_NULL(func);
    }
}

TEST_F(LogicOrFuncTest, testLocal) {
    testLocalNormal<uint8_t>();
    testLocalNormal<uint16_t>();
    testLocalNormal<uint32_t>();
    testLocalNormal<uint64_t>();

    TablePtr table;
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "key", {1, 1, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 3, 4}));
        table = getTable(createTable(_allocator, docs));
    }
    {
        // test invalid input column name
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                                            {"b"},
                                            {"logic_or"},
                                            AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // test invalid input column type
        TablePtr outputTable(new Table(_poolPtr));
        auto func = _creator.createFunction({ValueTypeHelper<uint16_t>::getValueType()},
                                            {"a"},
                                            {"logic_or"},
                                            AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_TRUE(func->initAccumulatorOutput(outputTable));
        ASSERT_FALSE(func->initCollectInput(table));
        DELETE_AND_SET_NULL(func);
    }
    {
        // duplicate output column name
        TablePtr outputTable(new Table(_poolPtr));
        outputTable->declareColumn<int>("logic_or");
        auto func = _creator.createFunction({ValueTypeHelper<uint32_t>::getValueType()},
                                            {"a"},
                                            {"logic_or"},
                                            AggFuncMode::AGG_FUNC_MODE_LOCAL);
        ASSERT_TRUE(nullptr != func);
        ASSERT_TRUE(func->init(_poolPtr.get(), _poolPtr.get()));
        ASSERT_FALSE(func->initAccumulatorOutput(outputTable));
        DELETE_AND_SET_NULL(func);
    }
}

} // namespace sql
