#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/calc/CalcKernel.h>
#include <navi/engine/KernelTesterBuilder.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class CalcKernelTest : public OpTestBase {
public:
    CalcKernelTest();
    ~CalcKernelTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _table.reset();
    }
private:
    void prepareTable() {
        _poolPtr.reset(new autil::mem_pool::Pool());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<char>(_allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _table.reset(new Table(docs, _allocator));
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("CalcKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }

    void getOutputTable(KernelTester &tester, TablePtr &outputTable) {
        DataPtr outputData;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        outputTable = getTable(outputData);
        ASSERT_TRUE(outputTable != nullptr);
    }


public:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;

};

CalcKernelTest::CalcKernelTest() {
}

CalcKernelTest::~CalcKernelTest() {
}

TEST_F(CalcKernelTest, testSimpleProcess) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : "$id", "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":">", "params":["$id", 1]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {6, 7, 8}));
}

TEST_F(CalcKernelTest, testOutputExprs) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":">", "params":["$id", 1]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {12, 13, 14}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {6, 7, 8}));
}

TEST_F(CalcKernelTest, testOutputExprsWithIn) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"IN", "params":["$id", 1, 2]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
}

TEST_F(CalcKernelTest, testOutputExprsWithHaIn) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"ha_in", "params":["$id", "1|2"], "type":"UDF"})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
}

TEST_F(CalcKernelTest, testOutputExprsWithInString) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"IN", "params":["$b", "b1", "b2"]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
}

TEST_F(CalcKernelTest, testOutputExprsWithHaInString) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"ha_in", "params":["$b", "b1|b2"], "type":"UDF"})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
}

TEST_F(CalcKernelTest, testOutputExprsWithNotIn) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"NOT IN", "params":["$id", 1, 2]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {13, 14}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {7, 8}));
}

TEST_F(CalcKernelTest, testBatchInput) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$id" : "$id", "$new_a" : "$a"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"<=", "params":["$id", 2]})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
    }
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.setInputEof("input0"));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6}));
    }
}

TEST_F(CalcKernelTest, testAlias) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$id" : "$a", "$a" : "$id"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$a"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "a", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "id", {5, 6, 7, 8}));
}

TEST_F(CalcKernelTest, testMultiAlias) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$a" : "$id"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$a"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "a", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
}

TEST_F(CalcKernelTest, testMultiValue) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$b" : "$id", "$c" : "$b"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$b", "$c"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "b", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "c", {"b1", "b2", "b3", "b4"}));
}

TEST_F(CalcKernelTest, testOutputConstValue) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$new_a" : "$a", "$b" : "bb", "$c" : 123, "$e" : 7.77})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a", "$b", "$c", "$e"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"bb", "bb", "bb", "bb"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "c", {123, 123, 123, 123}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(outputTable, "e", {7.77, 7.77, 7.77, 7.77}));
}

TEST_F(CalcKernelTest, testOutputConstValueWithCast) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$id": {"op":"CAST","cast_type":"BIGINT","type":"UDF","params":[12]}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "id", {12, 12, 12, 12}));
}

TEST_F(CalcKernelTest, testOutputNullNumberWithCast) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$i1": {"op":"CAST","cast_type":"BIGINT","type":"UDF","params":[null]}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$i1"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BIGINT"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "i1", {0,0,0,0}));
}

TEST_F(CalcKernelTest, testOutputNullIntWithCast) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$i1": {"op":"CAST","cast_type":"INTEGER","type":"UDF","params":[null]}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$i1"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "i1", {0,0,0,0}));
}

TEST_F(CalcKernelTest, testOutputNullStringWithCast) {
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$i1": {"op":"CAST","cast_type":"VARCHAR","type":"UDF","params":[null]}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$i1"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["VARCHAR"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "i1", {"","","",""}));
}

TEST_F(CalcKernelTest, testSkipCopyTable) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$a", "$b", "$c"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(_table, outputTable);
}

TEST_F(CalcKernelTest, testCopyTable) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$a", "$b"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(_table != outputTable);
}

TEST_F(CalcKernelTest, testCopyTable2) {
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$b", "$a", "$c"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(_table != outputTable);
}

TEST_F(CalcKernelTest, testOutputExprsWithSimpleCaseWhen) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : {"type":"OTHER","params":[{"type":"OTHER","params":["$id",3],"op":"<"},"$a",{"type":"OTHER","params":["$a",8],"op":"="},{"cast_type":"BIGINT","type":"UDF","params":["0"],"op":"CAST"},"$id"],"op":"CASE"}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "BIGINT"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12, 13, 14}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6, 3, 0}));
}

// TODO: to support until
// ExprVisitor::visit(...) calls to ExprGenerateVisitor::visitOtherOp(...)
// never totally depends on SyntaxParser
TEST_F(CalcKernelTest, testOutputExprsWithComplexCaseWhen) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : {"op" : "+", "params":["$a", {"type":"OTHER","params":[{"type":"OTHER","params":["$id",3],"op":"<"},"$a",{"type":"OTHER","params":["$a",8],"op":"="},{"cast_type":"BIGINT","type":"UDF","params":["0"],"op":"CAST"},"$id"],"op":"CASE"}]}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "BIGINT"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    DataPtr outputData;
    bool eof = false;
    ASSERT_FALSE(tester.getOutput("output0", outputData, eof));
    /**    
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12, 13, 14}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {10, 12, 10, 8}));
    */
}

TEST_F(CalcKernelTest, testConditionWithBoolCaseWhen) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : {"type":"OTHER","params":[{"type":"OTHER","params":["$id",3],"op":"<"},"$a",{"type":"OTHER","params":["$a",8],"op":"="},{"cast_type":"BIGINT","type":"UDF","params":["0"],"op":"CAST"},"$id"],"op":"CASE"}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "BIGINT"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":"OR","params":[{"op":"<=","params":["$id",2]},{"op":"CASE","params":[{"op":">","params":["$id",2],"type":"OTHER"},{"op":"<","params":["$a",8],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}],"type":"OTHER"})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {11, 12, 13}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {5, 6, 3}));
}


// TODO: to support until
// ExprVisitor::visit(...) calls to ExprGenerateVisitor::visitOtherOp(...)
// never totally depends on SyntaxParser
TEST_F(CalcKernelTest, testConditionWithNotBoolCaseWhen) {
    _attributeMap["output_field_exprs"] =
        ParseJson(R"json({"$id" : {"op" : "+", "params":["$id", 10]}, "$new_a" : {"type":"OTHER","params":[{"type":"OTHER","params":["$id",3],"op":"<"},"$a",{"type":"OTHER","params":["$a",8],"op":"="},{"cast_type":"BIGINT","type":"UDF","params":["0"],"op":"CAST"},"$id"],"op":"CASE"}})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$new_a"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["INTEGER", "BIGINT"])json");
    _attributeMap["condition"] = ParseJson(R"json({"op":">","params":[{"op":"CASE","params":[{"op":"=","params":["$b","b3"],"type":"OTHER"},"$a","$id"],"type":"OTHER"},5],"type":"OTHER"})json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    DataPtr outputData;
    bool eof = false;
    ASSERT_FALSE(tester.getOutput("output0", outputData, eof));
    /**    
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {13}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "new_a", {3}));
    */
}


END_HA3_NAMESPACE(sql);
