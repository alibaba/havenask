#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Resource.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class TvfKernelTest : public OpTestBase {
public:
    TvfKernelTest();
    ~TvfKernelTest();

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
        vector<MatchDoc> docs = _allocator->batchAllocate(2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<char>(
            _allocator, docs, "mc", {{'c', '1'}, {'2'}}));
        _table.reset(new Table(docs, _allocator));
    }
    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.TvfKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
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

TvfKernelTest::TvfKernelTest() {}

TvfKernelTest::~TvfKernelTest() {}

TEST_F(TvfKernelTest, testSimpleProcess) {
    _attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
}

TEST_F(TvfKernelTest, testReuseInput) {
    {
        _attributeMap["invocation"] = ParseJson(
            R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
        _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
        // check input
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(_table, "mc", {{'c', '1'}, {'2'}}));
    }
    {
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        _attributeMap["invocation"] = ParseJson(
            R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
        _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        TablePtr outputTable;
        getOutputTable(tester, outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
        // check input
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(_table, "mc", {{'c', '1'}, {'2'}}));
    }
}

TEST_F(TvfKernelTest, testInvocationWithCastSimpleProcess) {
    _attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["1", {"params":"@table#0", "op": "CAST"}], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
}

TEST_F(TvfKernelTest, testTvf_init_failed) {
    _attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["abc", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError()) << tester;
    ASSERT_EQ("init tvf function [printTableTvf] failed.", tester.getErrorMessage());
}

TEST_F(TvfKernelTest, testtvf_not_exist) {
    _attributeMap["invocation"]
        = ParseJson(R"json({"op" : "not_exist", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError()) << tester;
    ASSERT_EQ("create tvf funcition [not_exist] failed.", tester.getErrorMessage());
}

TEST_F(TvfKernelTest, testCheckOutputFailed) {
    _attributeMap["invocation"] = ParseJson(
        R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id_new", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);

    ASSERT_EQ("output field [id_new] not found.", tester.getErrorMessage());
}

class ComputeErrorTvfFunc : public TvfFunc {
public:
    bool init(TvfFuncInitContext &context) override {
        return true;
    };
    bool compute(const TablePtr &input, bool eof, TablePtr &output) override {
        return false;
    }
};

class ComputeErrorTvfFuncCreator : public TvfFuncCreatorR {
public:
    ComputeErrorTvfFuncCreator()
        : TvfFuncCreatorR("", {"computeErrorTvf", "computeErrorTvf"}) {}

public:
    std::string getResourceName() const override {
        return RESOURCE_ID;
    }

public:
    static const std::string RESOURCE_ID;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override {
        return new ComputeErrorTvfFunc();
    }
};
const std::string ComputeErrorTvfFuncCreator::RESOURCE_ID = "tvf_test.compute_error_tvf";
REGISTER_RESOURCE(ComputeErrorTvfFuncCreator);

TEST_F(TvfKernelTest, testTvfComputeFailed) {
    TvfFuncCreatorRPtr creator(new ComputeErrorTvfFuncCreator());
    _attributeMap["invocation"]
        = ParseJson(R"json({"op" : "computeErrorTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);
    ASSERT_EQ("tvf [computeErrorTvf] compute failed.", tester.getErrorMessage());
}

class OutputEmptyTvfFunc : public TvfFunc {
public:
    bool init(TvfFuncInitContext &context) override {
        return true;
    };
    bool compute(const TablePtr &input, bool eof, TablePtr &output) override {
        return true;
    }
};

class OutputEmptyTvfFuncCreator : public TvfFuncCreatorR {
public:
    OutputEmptyTvfFuncCreator()
        : TvfFuncCreatorR("", {"outputEmptyTvf", "outputEmptyTvf"}) {}

public:
    std::string getResourceName() const override {
        return RESOURCE_ID;
    }

public:
    static const std::string RESOURCE_ID;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override {
        return new OutputEmptyTvfFunc();
    }
};
const std::string OutputEmptyTvfFuncCreator::RESOURCE_ID = "tvf_test.output_empty_tvf";
REGISTER_RESOURCE(OutputEmptyTvfFuncCreator);

TEST_F(TvfKernelTest, testTvfOutputEmpty) {
    TvfFuncCreatorRPtr creator(new OutputEmptyTvfFuncCreator());
    _attributeMap["invocation"]
        = ParseJson(R"json({"op" : "outputEmptyTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table)), true));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);
    ASSERT_EQ("tvf [outputEmptyTvf] output is empty.", tester.getErrorMessage());
}

class TestOnePassTvfFunc : public OnePassTvfFunc {
public:
    bool init(TvfFuncInitContext &context) override {
        return true;
    };
    bool doCompute(const TablePtr &input, TablePtr &output) override {
        output = input;
        return true;
    }
};

class TestOnePassTvfFuncCreator : public TvfFuncCreatorR {
public:
    TestOnePassTvfFuncCreator()
        : TvfFuncCreatorR("", {"testOnePassTvf", "testOnePassTvf"}) {}

public:
    std::string getResourceName() const override {
        return RESOURCE_ID;
    }

public:
    static const std::string RESOURCE_ID;

public:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override {
        return new TestOnePassTvfFunc();
    }
};
const std::string TestOnePassTvfFuncCreator::RESOURCE_ID = "tvf_test.one_pass_tvf";
REGISTER_RESOURCE(TestOnePassTvfFuncCreator);

TEST_F(TvfKernelTest, testOnePassTvf) {
    TvfFuncCreatorRPtr creator(new TestOnePassTvfFuncCreator());
    _attributeMap["invocation"]
        = ParseJson(R"json({"op" : "testOnePassTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", TableDataPtr(new TableData(_table))));
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_NONE, ec);
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
}

} // namespace sql
