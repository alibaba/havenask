#include <ha3/sql/ops/test/OpTestBase.h>
#include "ha3/sql/ops/join/InnerJoin.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);

class JoinBaseTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    void checkColumn(const TablePtr &table, string columnName, BuiltinType type, bool isMulti) {
        ColumnPtr tableColumn = table->getColumn(columnName);
        ASSERT_TRUE(tableColumn != NULL);
        auto schema = tableColumn->getColumnSchema();
        ASSERT_TRUE(schema != NULL);
        auto vt = schema->getType();
        ASSERT_EQ(type, vt.getBuiltinType());
        ASSERT_EQ(isMulti, vt.isMultiValue());
    }

    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, JoinBaseTest);

void JoinBaseTest::setUp() {
}

void JoinBaseTest::tearDown() {
}

TEST_F(JoinBaseTest, testGetFieldPairs) {
    vector<string> leftInputFields = {"1", "2"};
    vector<string> rightInputFields = {"3"};
    vector<string> outputFields = {"1", "2", "30"};
    InnerJoin joinBase({leftInputFields, rightInputFields, outputFields, nullptr, {}, nullptr, {}});
    ASSERT_EQ(2, joinBase._leftFields.size());
    ASSERT_EQ(1, joinBase._rightFields.size());
    ASSERT_EQ("1", joinBase._leftFields[0].first);
    ASSERT_EQ("1", joinBase._leftFields[0].second);
    ASSERT_EQ("2", joinBase._leftFields[1].first);
    ASSERT_EQ("2", joinBase._leftFields[1].second);
    ASSERT_EQ("30", joinBase._rightFields[0].first);
    ASSERT_EQ("3", joinBase._rightFields[0].second);
}

TEST_F(JoinBaseTest, testAppendColumns) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(allocator, leftDocs, "cid", {"1", "3"}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(allocator, leftDocs, "muid", {{1, 3}, {2, 4}}));
    TablePtr inputTable(new Table(leftDocs, allocator));
    TablePtr outputTable(new Table(_poolPtr));
    InnerJoin joinBase({{}, {}, {}, nullptr, {}, nullptr, {}});
    {// input filed not exist
        vector<pair<string, string> >fields;
        fields.emplace_back("out_muid", "muidxxx");
        ASSERT_FALSE(joinBase.appendColumns(fields, inputTable, outputTable));
    }
    {
        vector<pair<string, string> >fields;
        fields.emplace_back("out_uid", "uid");
        fields.emplace_back("out_cid", "cid");
        fields.emplace_back("out_muid", "muid");
        ASSERT_TRUE(joinBase.appendColumns(fields, inputTable, outputTable));
        ASSERT_EQ(3, outputTable->getColumnCount());
        checkColumn(outputTable, "out_uid", bt_uint32, false);
        checkColumn(outputTable, "out_cid", bt_string, false);
        checkColumn(outputTable, "out_muid", bt_int32, true);
    }
}

TEST_F(JoinBaseTest, testEvaluateColumns) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(allocator, leftDocs, "muid", {{1, 3}, {2, 4}}));
    TablePtr inputTable(new Table(leftDocs, allocator));
    TablePtr outputTable(new Table(_poolPtr));

    vector<pair<string, string> >fields;
    fields.emplace_back("out_uid", "uid");
    fields.emplace_back("out_cid", "cid");
    fields.emplace_back("out_muid", "muid");
    InnerJoin joinBase({{}, {}, {}, nullptr, {}, nullptr, {}});

    ASSERT_TRUE(joinBase.appendColumns(fields, inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateColumns(fields, {1, 0}, inputTable, 0, outputTable));
    {
        ColumnPtr column = outputTable->getColumn("out_uid");
        auto columnData = column->getColumnData<uint32_t>();
        ASSERT_EQ(1, columnData->get(0));
        ASSERT_EQ(0, columnData->get(1));
    }
    {
        ColumnPtr column = outputTable->getColumn("out_cid");
        auto columnData = column->getColumnData<MultiChar>();
        auto &value0 = columnData->get(0);
        auto &value1 = columnData->get(1);
        ASSERT_EQ("3333", string(value0.data(), value0.size()));
        ASSERT_EQ("1111", string(value1.data(), value1.size()));
    }
    {
        ColumnPtr column = outputTable->getColumn("out_muid");
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
    vector<MatchDoc> leftDocs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
    TablePtr leftTable(new Table(leftDocs, allocator));

    vector<MatchDoc> rightDocs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(allocator, rightDocs, "cid", {10, 11}));
    TablePtr rightTable(new Table(rightDocs, allocator));

    TablePtr outputTable(new Table(_poolPtr));

    JoinInfo joinInfo;
    InnerJoin joinBase({{"uid"}, {"cid"}, {"out_uid", "out_cid"}, &joinInfo, {}, nullptr, {}});
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
    vector<MatchDoc> docs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(
                    allocator, docs, "uint", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                    allocator, docs, "str", {"a", "b"}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(
                    allocator, docs, "multi", {{0, 1}, {1}}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<string>(
                    allocator, docs, "multiStr", {{"ab", "b"}, {"cde"}}));
    TablePtr inputTable(new Table(docs, allocator));
    vector<pair<string, string>> fields = {{"out_uint", "uint"},
                                           {"out_str", "str"},
                                           {"out_multi", "multi"},
                                           {"out_multiStr", "multiStr"}};
    InnerJoin joinBase({{}, {}, {}, nullptr, {}, &_pool, {}});

    TablePtr outputTable(new Table(_poolPtr));
    ASSERT_TRUE(joinBase.appendColumns(fields, inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateEmptyColumns(fields, 0, outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uint", {0, 0});
    checkOutputColumn<string>(outputTable, "out_str", {"", ""});
    checkOutputMultiColumn<int32_t>(outputTable, "out_multi", {{}, {}});
    checkOutputMultiColumn<string>(outputTable, "out_multiStr", {{}, {}});
}

TEST_F(JoinBaseTest, testEvaluateEmptyColumnsWithDefaultValue) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> docs = std::move(getMatchDocs(allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<bool>(
                    allocator, docs, "bool", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int>(
                    allocator, docs, "int", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<double>(
                    allocator, docs, "double", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(
                    allocator, docs, "float", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                    allocator, docs, "int64", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(
                    allocator, docs, "int16", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int8_t>(
                    allocator, docs, "int8", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                    allocator, docs, "str", {"a", "b"}));
    TablePtr inputTable(new Table(docs, allocator));
    vector<pair<string, string>> fields = {{"out_bool", "bool"},
                                           {"out_int", "int"},
                                           {"out_double", "double"},
                                           {"out_float", "float"},
                                           {"out_int64", "int64"},
                                           {"out_int16", "int16"},
                                           {"out_int8", "int8"},
                                           {"out_str", "str"}};
    InnerJoin joinBase({{}, {}, {}, nullptr, {}, &_pool, {}});
    joinBase.setDefaultValue(
            {
                {"INTEGER", "2"},
                {"BOOLEAN", "true"},
                {"DOUBLE", "1.1"},
                {"FLOAT", "2.2"},
                {"BIGINT", "1000000000000"},
                {"VARCHAR", "aa"},
                {"SMALLINT", "10"},
                {"TINYINT", "20"}});
    TablePtr outputTable(new Table(_poolPtr));
    ASSERT_TRUE(joinBase.appendColumns(fields, inputTable, outputTable));
    outputTable->batchAllocateRow(2);
    ASSERT_TRUE(joinBase.evaluateEmptyColumns(fields, 0, outputTable));
    checkOutputColumn<bool>(outputTable, "out_bool", {1, 1});
    checkOutputColumn<int32_t>(outputTable, "out_int", {2, 2});
    checkOutputColumn<double>(outputTable, "out_double", {1.1, 1.1});
    checkOutputColumn<float>(outputTable, "out_float", {2.2, 2.2});
    checkOutputColumn<int64_t>(outputTable, "out_int64", {1000000000000l, 1000000000000l});
    checkOutputColumn<int16_t>(outputTable, "out_int16", {10, 10});
    checkOutputColumn<int8_t>(outputTable, "out_int8", {20, 20});
    checkOutputColumn<string>(outputTable, "out_str", {"aa", "aa"});
}

END_HA3_NAMESPACE();
