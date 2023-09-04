#include "sql/ops/calc/CalcTableR.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "fmt/printf.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/tester/NaviResourceHelper.h"
#include "navi/util/NaviTestPool.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ExprUtil.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class CalcTableRTest : public TESTBASE {
public:
    CalcTableRTest();
    ~CalcTableRTest();

private:
    void prepareTable() {
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
            _allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _table.reset(new Table(docs, _allocator));
        _exprCreator.reset(new MatchDocsExpressionCreator(
            _poolPtr.get(), _allocator.get(), &_funcCreator, nullptr, nullptr, nullptr));
    }

    void prepareCalcTable(const std::vector<std::string> &outputFields = {},
                          const std::vector<std::string> &outputFieldsType = {},
                          const std::string &conditionJson = "",
                          const std::string &outputExprsJson = "") {
        std::string kernelConfig
            = fmt::sprintf(R"json({
"output_fields": %s,
"output_fields_type": %s,
"condition": %s,
"output_field_exprs": %s
})json",
                           FastToJsonString(outputFields).c_str(),
                           FastToJsonString(outputFieldsType).c_str(),
                           conditionJson.empty() ? "\"\"" : conditionJson.c_str(),
                           outputFieldsType.empty() ? "\"\"" : outputExprsJson.c_str());
        _calcTable.reset(new CalcTableR());
        _naviRes.reset(new navi::NaviResourceHelper());
        _naviRes->kernelConfig(kernelConfig);
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_graphMemoryPoolR));
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_calcInitParamR));
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_suezCavaAllocatorR));
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_traceAdapterR));
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_functionInterfaceCreatorR));
        ASSERT_TRUE(_naviRes->getOrCreateRes(_calcTable->_cavaPluginManagerR));
    }

private:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::MatchDocUtil _matchDocUtil;
    std::unique_ptr<navi::NaviResourceHelper> _naviRes;
    std::unique_ptr<CalcTableR> _calcTable;
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
    FunctionInterfaceCreator _funcCreator;
    MatchDocsExpressionCreatorPtr _exprCreator;
};

CalcTableRTest::CalcTableRTest()
    : _poolPtr(new autil::mem_pool::PoolAsan()) {}

CalcTableRTest::~CalcTableRTest() {}

TEST_F(CalcTableRTest, testFilterTable) {
    {
        ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "{\"op\":\">\", \"params\":[\"$AVG(t2.price)\", 1]}";
        ConditionParser parser(_poolPtr.get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(true);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {2, 3, 4}));
    }
    {
        ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "{\"op\":\">\", \"params\":[\"$AVG(t2.price)\", 1]}";
        ConditionParser parser(_poolPtr.get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(false);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {1, 2, 3, 4}));
    }
    {
        ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
        _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "";
        ConditionParser parser(_poolPtr.get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(true);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {1, 2, 3, 4}));
    }
}

TEST_F(CalcTableRTest, testFilterTableWith$) {
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    _allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = _allocator->batchAllocate(4);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "$f1", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
        _allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    _table.reset(new Table(docs, _allocator));

    string conditionStr = "{\"op\":\">\", \"params\":[\"$$f1\", 1]}";
    ConditionParser parser(_poolPtr.get());
    ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
    ASSERT_TRUE(_calcTable->filterTable(_table));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "$f1", {2, 3, 4}));
}

TEST_F(CalcTableRTest, testCloneColumn) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    auto poolPtr = _poolPtr;
    std::vector<CalcTableR::ColumnDataTuple> vec;
    {
        TablePtr output(new Table(poolPtr));
        ASSERT_TRUE(_calcTable->cloneColumnData(_table->getColumn("id"), output, "a", vec));
        ASSERT_TRUE(_calcTable->cloneColumnData(_table->getColumn("c"), output, "c", vec));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->calcTableCloumn(output, vec));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<char>(
            output, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        vec.clear();
    }
    {
        TablePtr output(new Table(poolPtr));
        ASSERT_TRUE(_calcTable->cloneColumnData(_table->getColumn("id"), output, "a", vec));
        ASSERT_FALSE(_calcTable->cloneColumnData(_table->getColumn("id"), output, "a", vec));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->calcTableCloumn(output, vec));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        vec.clear();
    }
    {
        TablePtr output(new Table(poolPtr));
        ASSERT_TRUE(_calcTable->cloneColumnData(_table->getColumn("id"), output, "a", vec));
        ASSERT_TRUE(_calcTable->cloneColumnData(_table->getColumn("id"), output, "b", vec));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->calcTableCloumn(output, vec));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<uint32_t>(output, "b", {1, 2, 3, 4}));
        vec.clear();
    }
}

TEST_F(CalcTableRTest, testAddAlaisMap) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    unordered_map<string, string> alaisMap = {{"_a_", "a"}, {"_id_", "id"}};
    ASSERT_TRUE(_allocator->getReferenceAliasMap() == nullptr);
    CalcTableR::addAliasMap(_allocator.get(), alaisMap);
    ASSERT_TRUE(_allocator->getReferenceAliasMap() != nullptr);
    ASSERT_EQ(2, _allocator->getReferenceAliasMap()->size());

    unordered_map<string, string> alaisMap2 = {{"_a1_", "a"}, {"_id_", "id"}};
    CalcTableR::addAliasMap(_allocator.get(), alaisMap2);
    ASSERT_TRUE(_allocator->getReferenceAliasMap() != nullptr);
    ASSERT_EQ(3, _allocator->getReferenceAliasMap()->size());
}

TEST_F(CalcTableRTest, testNeedCopyTable) {
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    TablePtr table(new Table(_poolPtr));
    {
        _calcTable->_exprsMap = {{"a", ExprEntity()}};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    _calcTable->_exprsMap = {};
    table->declareColumn<int>("a");
    table->declareColumn<int>("b");
    {
        _calcTable->_calcInitParamR->outputFields = {"a"};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    {
        _calcTable->_calcInitParamR->outputFields = {"b", "a"};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    {
        _calcTable->_calcInitParamR->outputFields = {"a", "b"};
        ASSERT_FALSE(_calcTable->needCopyTable(table));
    }
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_emptyStr_emptyType) {
    std::string outputType;
    std::string exprStr;
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    navi::NaviLoggerProvider provider("INFO");
    auto expr = _calcTable->createAttributeExpression(
        outputType, exprStr, _exprCreator.get(), _poolPtr.get());
    ASSERT_FALSE(expr);
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "expr string and output type both empty", provider));
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_emptyStrNullValue_unknownType) {
    std::string outputType("not_exist");
    std::string exprStr;
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    navi::NaviLoggerProvider provider("INFO");
    auto expr = _calcTable->createAttributeExpression(
        outputType, exprStr, _exprCreator.get(), _poolPtr.get());
    ASSERT_FALSE(expr);
    std::string expected("output type [not_exist] can not be unknown when expr string is empty");
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, expected, provider));
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_emptyStrNullValue) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    {
        std::string outputType("INTEGER");
        std::string exprStr;
        auto expr = _calcTable->createAttributeExpression(
            outputType, exprStr, _exprCreator.get(), _poolPtr.get());
        ASSERT_TRUE(expr);
        ASSERT_EQ("0", expr->getOriginalString());
    }
    {
        std::string outputType("CHAR");
        std::string exprStr;
        auto expr = _calcTable->createAttributeExpression(
            outputType, exprStr, _exprCreator.get(), _poolPtr.get());
        ASSERT_TRUE(expr);
        ASSERT_EQ("", expr->getOriginalString());
    }
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_parseExprFailed) {
    std::string outputType("");
    std::string exprStr("+a!");
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    navi::NaviLoggerProvider provider("INFO");
    auto expr = _calcTable->createAttributeExpression(
        outputType, exprStr, _exprCreator.get(), _poolPtr.get());
    ASSERT_FALSE(expr);
    std::string expected("parse syntax [+a!] failed");
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, expected, provider));
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_constType) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    {
        std::string outputType("INTEGER");
        std::string exprStr("123");
        auto expr = _calcTable->createAttributeExpression(
            outputType, exprStr, _exprCreator.get(), _poolPtr.get());
        ASSERT_TRUE(expr);
        ASSERT_EQ("123", expr->getOriginalString());
    }
    {
        std::string outputType("CHAR");
        std::string exprStr("\"123a\"");
        auto expr = _calcTable->createAttributeExpression(
            outputType, exprStr, _exprCreator.get(), _poolPtr.get());
        ASSERT_TRUE(expr);
        ASSERT_EQ("\"123a\"", expr->getOriginalString());
    }
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_createExprFailed) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    std::string outputType("INTEGER");
    std::string exprStr("x + 1");
    navi::NaviLoggerProvider provider("INFO");
    auto expr = _calcTable->createAttributeExpression(
        outputType, exprStr, _exprCreator.get(), _poolPtr.get());
    ASSERT_FALSE(expr) << expr->getOriginalString();
    std::string expected("create attribute expression [x + 1] failed");
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, expected, provider));
}

TEST_F(CalcTableRTest, testCreateAttributeExpression_createSuccess) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable());
    std::string outputType("INTEGER");
    std::string exprStr("a + 1");
    auto expr = _calcTable->createAttributeExpression(
        outputType, exprStr, _exprCreator.get(), _poolPtr.get());
    ASSERT_TRUE(expr);
}

TEST_F(CalcTableRTest, testDoProjectReuseTable) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    string outputExpr = R"({"$score" : {"op" : "+", "params":["$a", 10], "type":"OTHER"}})";
    ASSERT_NO_FATAL_FAILURE(
        prepareCalcTable({"a", "score"}, {"INTEGER", "INTEGER"}, "", outputExpr));
    CalcTableR *calcTable = nullptr;
    ASSERT_TRUE(_naviRes->getOrCreateRes(calcTable));
    calcTable->setReuseTable(true);
    ASSERT_TRUE(calcTable->doProjectReuseTable(_table, _exprCreator.get()));
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(_table, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn<int64_t>(_table, "score", {15, 16, 17, 18}));
}

TEST_F(CalcTableRTest, testDoProjectReuseTableFailed) {
    ASSERT_NO_FATAL_FAILURE(prepareTable());
    string outputExpr = R"json({"$b" : {"op" : "+", "params":["$a", 10], "type":"OTHER"}})json";
    ASSERT_NO_FATAL_FAILURE(prepareCalcTable({"a", "b"}, {"INTEGER", "INTEGER"}, "", outputExpr));
    CalcTableR *calcTable = nullptr;
    ASSERT_TRUE(_naviRes->getOrCreateRes(calcTable));
    calcTable->setReuseTable(true);
    ASSERT_FALSE(calcTable->doProjectReuseTable(_table, _exprCreator.get()));
}

} // namespace sql
