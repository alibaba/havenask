#include "sql/ops/join/JoinBase.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "sql/ops/join/InnerJoin.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace table;

namespace sql {

class JoinBaseTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

private:
    void checkColumn(const TablePtr &table, string columnName, BuiltinType type, bool isMulti) {
        auto tableColumn = table->getColumn(columnName);
        ASSERT_TRUE(tableColumn != NULL);
        auto schema = tableColumn->getColumnSchema();
        ASSERT_TRUE(schema != NULL);
        auto vt = schema->getType();
        ASSERT_EQ(type, vt.getBuiltinType());
        ASSERT_EQ(isMulti, vt.isMultiValue());
    }
    void constructJoinParam(JoinBaseParamR &joinParamR,
                            const std::vector<std::string> &leftInputs,
                            const std::vector<std::string> &rightInputs,
                            const std::vector<std::string> &outputs) {
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(joinParamR._joinInfoR));
        joinParamR._leftInputFields = leftInputs;
        joinParamR._rightInputFields = rightInputs;
        joinParamR._outputFields = outputs;
        joinParamR._poolPtr = _poolPtr;
        joinParamR._pool = _poolPtr.get();
    }
};

void JoinBaseTest::setUp() {}

void JoinBaseTest::tearDown() {}

TEST_F(JoinBaseTest, testAppendColumns) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1", "3"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2, 4}}));
    TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
    TablePtr outputTable(new Table(_poolPtr));
    { // input filed not exist
        JoinBaseParamR param;
        param._outputFields.push_back("out_muid");
        param._leftInputFields.push_back("muidxxx");
        InnerJoin joinBase(param);
        ASSERT_FALSE(joinBase.appendColumns(inputTable, outputTable));
    }
    {
        JoinBaseParamR param;
        param._outputFields = {"out_uid", "out_cid", "out_muid"};
        param._leftInputFields = {"uid", "cid", "muid"};
        InnerJoin joinBase(param);
        ASSERT_TRUE(joinBase.appendColumns(inputTable, outputTable));
        ASSERT_EQ(3, outputTable->getColumnCount());
        checkColumn(outputTable, "out_uid", bt_uint32, false);
        checkColumn(outputTable, "out_cid", bt_string, false);
        checkColumn(outputTable, "out_muid", bt_int32, true);
    }
}

TEST_F(JoinBaseTest, testEvaluateColumns) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2, 4}}));
    TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
    TablePtr outputTable(new Table(_poolPtr));

    JoinBaseParamR param;
    param._outputFields = {"out_uid", "out_cid", "out_muid"};
    param._leftInputFields = {"uid", "cid", "muid"};
    InnerJoin joinBase(param);

    ASSERT_TRUE(joinBase.appendColumns(inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateLeftTableColumns({1, 0}, inputTable, 0, outputTable));
    {
        auto column = outputTable->getColumn("out_uid");
        auto columnData = column->getColumnData<uint32_t>();
        ASSERT_EQ(1, columnData->get(0));
        ASSERT_EQ(0, columnData->get(1));
    }
    {
        auto column = outputTable->getColumn("out_cid");
        auto columnData = column->getColumnData<MultiChar>();
        auto value0 = columnData->get(0);
        auto value1 = columnData->get(1);
        ASSERT_EQ("3333", string(value0.data(), value0.size()));
        ASSERT_EQ("1111", string(value1.data(), value1.size()));
    }
    {
        auto column = outputTable->getColumn("out_muid");
        auto columnData = column->getColumnData<MultiInt32>();
        ASSERT_EQ(2, columnData->get(0).size());
        ASSERT_EQ(2, columnData->get(0)[0]);
        ASSERT_EQ(4, columnData->get(0)[1]);
        ASSERT_EQ(2, columnData->get(1).size());
        ASSERT_EQ(1, columnData->get(1)[0]);
        ASSERT_EQ(3, columnData->get(1)[1]);
    }
}

TEST_F(JoinBaseTest, testEvaluateJoinedTable) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
    TablePtr leftTable = Table::fromMatchDocs(leftDocs, allocator);

    vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, rightDocs, "cid", {10, 11}));
    TablePtr rightTable = Table::fromMatchDocs(rightDocs, allocator);

    TablePtr outputTable(new Table(_poolPtr));

    JoinBaseParamR param;
    ASSERT_NO_FATAL_FAILURE(constructJoinParam(param, {"uid"}, {"cid"}, {"out_uid", "out_cid"}));
    InnerJoin joinBase(param);
    ASSERT_TRUE(joinBase.initJoinedTable(leftTable, rightTable, outputTable));

    ASSERT_TRUE(joinBase.evaluateJoinedTable({0, 1}, {0, 0}, leftTable, rightTable, outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {0, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {10, 10});

    ASSERT_TRUE(joinBase.evaluateJoinedTable({0, 1}, {1, 1}, leftTable, rightTable, outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {0, 1, 0, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {10, 10, 11, 11});
}

TEST_F(JoinBaseTest, testEvaluateEmptyColumns) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, docs, "uint", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(allocator, docs, "str", {"a", "b"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, docs, "multi", {{0, 1}, {1}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        allocator, docs, "multiStr", {{"ab", "b"}, {"cde"}}));
    TablePtr inputTable = Table::fromMatchDocs(docs, allocator);
    JoinBaseParamR param;
    ASSERT_NO_FATAL_FAILURE(constructJoinParam(param, {}, {}, {}));
    param._outputFields = {"out_uint", "out_str", "out_multi", "out_multiStr"};
    param._leftInputFields = {"uint", "str", "multi", "multiStr"};
    InnerJoin joinBase(param);

    TablePtr outputTable(new Table(_poolPtr));
    ASSERT_TRUE(joinBase.appendColumns(inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateEmptyColumns(0, 0, outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uint", {0, 0});
    checkOutputColumn<string>(outputTable, "out_str", {"", ""});
    checkOutputMultiColumn<int32_t>(outputTable, "out_multi", {{}, {}});
    checkOutputMultiColumn<string>(outputTable, "out_multiStr", {{}, {}});
}

TEST_F(JoinBaseTest, testEvaluateEmptyColumnsWithDefaultValue) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<bool>(allocator, docs, "bool", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int>(allocator, docs, "int", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<double>(allocator, docs, "double", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<float>(allocator, docs, "float", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(allocator, docs, "int64", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int16_t>(allocator, docs, "int16", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int8_t>(allocator, docs, "int8", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(allocator, docs, "str", {"a", "b"}));
    TablePtr inputTable = Table::fromMatchDocs(docs, allocator);
    JoinBaseParamR param;
    ASSERT_NO_FATAL_FAILURE(constructJoinParam(param, {}, {}, {}));
    param._defaultValue = {{"INTEGER", "2"},
                           {"BOOLEAN", "true"},
                           {"DOUBLE", "1.1"},
                           {"FLOAT", "2.2"},
                           {"BIGINT", "1000000000000"},
                           {"VARCHAR", "aa"},
                           {"SMALLINT", "10"},
                           {"TINYINT", "20"}};
    param._outputFields = {"out_bool",
                           "out_int",
                           "out_double",
                           "out_float",
                           "out_int64",
                           "out_int16",
                           "out_int8",
                           "out_str"};
    param._leftInputFields = {"bool", "int", "double", "float", "int64", "int16", "int8", "str"};

    InnerJoin joinBase(param);
    TablePtr outputTable(new Table(_poolPtr));
    ASSERT_TRUE(joinBase.appendColumns(inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateEmptyColumns(0, 0, outputTable));
    checkOutputColumn<bool>(outputTable, "out_bool", {1, 1});
    checkOutputColumn<int32_t>(outputTable, "out_int", {2, 2});
    checkOutputColumn<double>(outputTable, "out_double", {1.1, 1.1});
    checkOutputColumn<float>(outputTable, "out_float", {2.2, 2.2});
    checkOutputColumn<int64_t>(outputTable, "out_int64", {1000000000000l, 1000000000000l});
    checkOutputColumn<int16_t>(outputTable, "out_int16", {10, 10});
    checkOutputColumn<int8_t>(outputTable, "out_int8", {20, 20});
    checkOutputColumn<string>(outputTable, "out_str", {"aa", "aa"});
}

} // namespace sql
