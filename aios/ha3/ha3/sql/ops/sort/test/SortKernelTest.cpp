#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/sort/SortKernel.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class SortKernelTest : public OpTestBase {
public:
    SortKernelTest();
    ~SortKernelTest();
public:
    void setUp() override {
    }
    void tearDown() override {
        _attributeMap.clear();
    }
private:
    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    void checkOutput(const vector<uint32_t> &expect, DataPtr data) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        ASSERT_EQ(expect.size(), outputTable->getRowCount());
        ColumnPtr column = outputTable->getColumn("id");
        ASSERT_TRUE(column != NULL);
        ColumnData<uint32_t>* id = column->getColumnData<uint32_t>();
        ASSERT_TRUE(id != NULL);
        for (size_t i = 0; i < expect.size(); i++) {
            ASSERT_EQ(expect[i], id->get(i));
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("SortKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }
public:
    MatchDocAllocatorPtr _allocator;
};

SortKernelTest::SortKernelTest() {
}

SortKernelTest::~SortKernelTest() {
}

TEST_F(SortKernelTest, testSimpleProcess) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["ASC", "ASC"])json");
    _attributeMap["limit"] = Any(DEFAULT_BATCH_SIZE);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 3, 4, 2}, odata);
}

TEST_F(SortKernelTest, testBatchInput) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["ASC", "ASC"])json");
    _attributeMap["limit"] = Any(3);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 3, 4}, odata);
}

TEST_F(SortKernelTest, testBatchInputNoData) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["ASC", "ASC"])json");
    _attributeMap["limit"] = Any(3);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.compute());
    ASSERT_TRUE(tester.compute());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
    }
    ASSERT_TRUE(tester.setInputEof("input0"));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 3, 4}, odata);
}

TEST_F(SortKernelTest, testDescend) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC", "DESC"])json");
    _attributeMap["limit"] = Any(3);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({2, 4, 3}, odata);
}

TEST_F(SortKernelTest, testSortLimit) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC", "DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({2, 4, 3, 1}, odata);
}

TEST_F(SortKernelTest, testMultiValue) {
    _attributeMap["order_fields"] = ParseJson(R"json(["a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int>(
                        _allocator, docs, "a", {{1, 3, 0}, {2, 2, 0}, {1, 4, 0}, {2, 1, 0}}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({2, 4, 3, 1}, odata);
}

TEST_F(SortKernelTest, testMultiChar) {
    _attributeMap["order_fields"] = ParseJson(R"json(["a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());

    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "a", {"222", "22", "", "23"}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({4, 1, 2, 3}, odata);
}

TEST_F(SortKernelTest, testInit) {
    _attributeMap["order_fields"] = ParseJson(R"json(["a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
}

TEST_F(SortKernelTest, testInitNoPool) {
    _attributeMap["order_fields"] = ParseJson(R"json(["a"])json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    _resourceMap.erase(_sqlQueryResource->getResourceName());
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ(EC_ABORT, testerPtr->getErrorCode());
}

TEST_F(SortKernelTest, testInvalidJson) {
    _attributeMap["order_fields"] = ParseJson(R"json("a")json");
    _attributeMap["directions"] = ParseJson(R"json(["DESC"])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ(EC_INVALID_ATTRIBUTE, testerPtr->getErrorCode());
}

TEST_F(SortKernelTest, testInvalidJson2) {
    _attributeMap["order_fields"] = ParseJson(R"json(["a"])json");
    _attributeMap["directions"] = ParseJson(R"json([["DESC"]])json");
    _attributeMap["limit"] = Any(10);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder testerBuilder;
    KernelTesterPtr testerPtr = buildTester(testerBuilder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ(EC_INVALID_ATTRIBUTE, testerPtr->getErrorCode());
}

TEST_F(SortKernelTest, testDoComputeInvalidInput) {
    navi::NaviLoggerProvider provider;
    SortKernel kernel;
    ASSERT_FALSE(kernel.doCompute(nullptr));
    ASSERT_EQ("invalid input table", provider.getErrorMessage());
}

TEST_F(SortKernelTest, testInitComparatorFailed) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b", "c"])json");
    _attributeMap["directions"] = ParseJson(R"json(["ASC", "ASC"])json");
    _attributeMap["limit"] = Any(DEFAULT_BATCH_SIZE);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        auto table = createTable(_allocator, docs);
        ASSERT_TRUE(tester.setInput("input0", table));
        ASSERT_TRUE(tester.setInputEof("input0"));
    }
    ASSERT_TRUE(tester.compute()); // kernel compute success
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("invalid column name [c]", tester.getErrorMessage());
}

TEST_F(SortKernelTest, testInitArraySizeMisMatch) {
    _attributeMap["order_fields"] = ParseJson(R"json(["b"])json");
    _attributeMap["directions"] = ParseJson(R"json(["ASC", "ASC"])json");
    _attributeMap["limit"] = Any(0);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ(EC_INVALID_ATTRIBUTE, tester.getErrorCode());
    ASSERT_EQ("sort key & order size mismatch, key size [1], order size [2]",
              tester.getErrorMessage());
}

TEST_F(SortKernelTest, testOnlyLimitWrong) {
    _attributeMap["order_fields"] = ParseJson(R"json([])json");
    _attributeMap["directions"] = ParseJson(R"json([])json");
    _attributeMap["limit"] = Any(3);
    _attributeMap["offset"] = Any(0);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ("sort key empty", tester.getErrorMessage());
}

END_HA3_NAMESPACE(sql);
