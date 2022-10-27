#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/tvf/TvfKernel.h>
#include <navi/engine/KernelTesterBuilder.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

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
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<char>(_allocator, docs, "mc", {{'c', '1'}, {'2'}}));
        _table.reset(new Table(docs, _allocator));
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("TvfKernel");
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

TvfKernelTest::TvfKernelTest() {
}

TvfKernelTest::~TvfKernelTest() {
}

TEST_F(TvfKernelTest, testInvocationAttr) {
    {
        string invStr =R"json({"op" : "printTableTvf", "params" : [], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {};
        vector<string> expectTable = {};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr =R"json({"op" : "printTableTvf", "params" : ["@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {};
        vector<string> expectTable = {"0"};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr =R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {"1"};
        vector<string> expectTable = {"0"};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr =R"json({"op" : "printTableTvf", "params" : ["1", "table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {"1", "table#0"};
        vector<string> expectTable = {};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr =R"json({"op" : "printTableTvf", "params" : [1, "@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_FALSE(attr.parseStrParams(params, tables));
    }
}

TEST_F(TvfKernelTest, testSimpleProcess) {
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
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
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
}

TEST_F(TvfKernelTest, testInvocationWithCastSimpleProcess) {
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "printTableTvf", "params" : ["1", {"params":"@table#0", "op": "CAST"}], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
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
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "mc", {{'c', '1'}, {'2'}}));
}


TEST_F(TvfKernelTest, testTvf_init_failed) {
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "printTableTvf", "params" : ["abc", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_FALSE(tester.compute()); // kernel compute success
    ASSERT_EQ("init tvf function [printTableTvf] failed.", tester.getErrorMessage());
}

TEST_F(TvfKernelTest, testtvf_not_exist) {
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "not_exist", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_FALSE(tester.compute());
    ASSERT_EQ("create tvf funcition [not_exist] failed.", tester.getErrorMessage());
}

TEST_F(TvfKernelTest, testCheckOutputFailed) {
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id_new", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);

    ASSERT_EQ("output field [id_new] not found.", tester.getErrorMessage());
}

class ComputeErrorTvfFunc : public TvfFunc
{
public:
    bool init(TvfFuncInitContext &context) override { return true;};
    bool compute(const TablePtr &input, bool eof, TablePtr &output) { return false; }
};

class ComputeErrorTvfFuncCreator : public TvfFuncCreator
{
public:
    ComputeErrorTvfFuncCreator()
        : TvfFuncCreator("")
    {
    }
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return new ComputeErrorTvfFunc();
    }
};


TEST_F(TvfKernelTest, testTvfComputeFailed) {
    TvfFuncCreatorPtr creator(new ComputeErrorTvfFuncCreator());
    creator->addTvfFunction({"computeErrorTvf", "computeErrorTvf"});
    _sqlResource->tvfFuncManager->_tvfNameToCreator["computeErrorTvf"] = creator.get();
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "computeErrorTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);
    ASSERT_EQ("tvf [computeErrorTvf] compute failed.", tester.getErrorMessage());
}

class OutputEmptyTvfFunc : public TvfFunc
{
public:
    bool init(TvfFuncInitContext &context) override { return true;};
    bool compute(const TablePtr &input, bool eof, TablePtr &output) { return true; }
};

class OutputEmptyTvfFuncCreator : public TvfFuncCreator
{
public:
    OutputEmptyTvfFuncCreator()
        : TvfFuncCreator("")
    {
    }
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return new OutputEmptyTvfFunc();
    }
};


TEST_F(TvfKernelTest, testTvfOutputEmpty) {
    TvfFuncCreatorPtr creator(new OutputEmptyTvfFuncCreator());
    creator->addTvfFunction({"outputEmptyTvf", "outputEmptyTvf"});
    _sqlResource->tvfFuncManager->_tvfNameToCreator["outputEmptyTvf"] = creator.get();
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "outputEmptyTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    auto ec = tester.getErrorCode();
    ASSERT_EQ(EC_ABORT, ec);
    ASSERT_EQ("tvf [outputEmptyTvf] output is empty.", tester.getErrorMessage());
}


class TestOnePassTvfFunc : public OnePassTvfFunc
{
public:
    bool init(TvfFuncInitContext &context) override { return true;};
    bool doCompute(const TablePtr &input, TablePtr &output) {
        output = input;
        return true;
    }
};

class TestOnePassTvfFuncCreator : public TvfFuncCreator
{
public:
    TestOnePassTvfFuncCreator()
        : TvfFuncCreator("")
    {}
public:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return new TestOnePassTvfFunc();
    }
};

TEST_F(TvfKernelTest, testOnePassTvf) {
    TvfFuncCreatorPtr creator(new TestOnePassTvfFuncCreator());
    creator->addTvfFunction({"testOnePassTvf", "testOnePassTvf"});
    _sqlResource->tvfFuncManager->_tvfNameToCreator["testOnePassTvf"] = creator.get() ;
    _attributeMap["invocation"] =
        ParseJson(R"json({"op" : "testOnePassTvf", "params" : ["@table#0"], "type":"TVF"})json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$id", "$mc"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["int32", "multi_int32"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
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



END_HA3_NAMESPACE(sql);
