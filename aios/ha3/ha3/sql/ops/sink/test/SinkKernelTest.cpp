#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/sink/SinkKernel.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;

BEGIN_HA3_NAMESPACE(sql);

class SinkKernelTest : public OpTestBase {
public:
    SinkKernelTest();
    ~SinkKernelTest();
public:
    void setUp() override {
    }
    void tearDown() override {
        _table.reset();
    }
private:
    void prepareTable() {
        vector<MatchDoc> docs = getMatchDocs(_allocator, 4);
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
        builder.kernel("SinkKernel");
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
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
};

SinkKernelTest::SinkKernelTest() {
}

SinkKernelTest::~SinkKernelTest() {
}

TEST_F(SinkKernelTest, testSimpleProcess) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(outputTable, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testBatchInput) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.compute()); // kernel compute success
    }
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.setInputEof("input0"));
        ASSERT_TRUE(tester.compute()); // kernel compute success
    }
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "a", {5, 6, 7, 8, 5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"b1", "b2", "b3", "b4", "b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(
                    outputTable, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}, {'c', '1'}, {'2'}, {'3'}, {'4'}}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testBatchInputEof) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.compute()); // kernel compute success
    }
    {
        prepareTable();
        ASSERT_TRUE(tester.setInput("input0", _table));
        ASSERT_TRUE(tester.compute()); // kernel compute success
        ASSERT_TRUE(tester.setInputEof("input0"));
        ASSERT_TRUE(tester.compute()); // kernel compute success
    }
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "a", {5, 6, 7, 8, 5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "b", {"b1", "b2", "b3", "b4", "b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(
                    outputTable, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}, {'c', '1'}, {'2'}, {'3'}, {'4'}}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testNoInput) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.compute());
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 1, 2, 3, 4}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(SinkKernelTest, testKernelDoComputeFailed) {
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    prepareTable();
    ASSERT_TRUE(tester.setInput("input0", _table));
    ASSERT_TRUE(tester.compute());
    navi::DataPtr data(new navi::Data());
    ASSERT_TRUE(tester.setInput("input0", data));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("invalid input table", tester.getErrorMessage());
    ASSERT_FALSE(tester.compute());
}

TEST_F(SinkKernelTest, testDoCompute) {
    navi::NaviLoggerProvider provider;
    SinkKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(_table));
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(_table));
    TablePtr outputTable = kernel._table;
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "id", {1, 2, 3, 4, 1, 2, 3, 4}));
    ASSERT_EQ("", provider.getErrorMessage());
}

TEST_F(SinkKernelTest, testDoComputeInvalidInput) {
    navi::NaviLoggerProvider provider;
    SinkKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(_table));
    ASSERT_FALSE(kernel.doCompute(nullptr));
    ASSERT_EQ("invalid input table", provider.getErrorMessage());
}

TEST_F(SinkKernelTest, testDoComputeSchemaMismatch) {
    navi::NaviLoggerProvider provider;
    SinkKernel kernel;
    prepareTable();
    ASSERT_TRUE(kernel.doCompute(_table));
    prepareTable();
    _table->declareColumn<int32_t>("new column");
    ASSERT_FALSE(kernel.doCompute(_table));
    ASSERT_EQ("merge input table failed", provider.getErrorMessage());
}

END_HA3_NAMESPACE(sql);
