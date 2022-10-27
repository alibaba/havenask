#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/limit/LimitKernel.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class LimitKernelTest : public OpTestBase {
public:
    LimitKernelTest();
    ~LimitKernelTest();
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
        builder.kernel("LimitKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }
public:
    MatchDocAllocatorPtr _allocator;
};

LimitKernelTest::LimitKernelTest() {
}

LimitKernelTest::~LimitKernelTest() {
}

TEST_F(LimitKernelTest, testLimit) {
    _attributeMap["limit"] = Any(3);
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
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2, 3}, odata);
}

TEST_F(LimitKernelTest, testLimitOversize) {
    _attributeMap["limit"] = Any(5);
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
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(tester.getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != NULL);
    checkOutput({1, 2, 3, 4}, odata);
}

TEST_F(LimitKernelTest, testBatchLimit) {
    _attributeMap["limit"] = Any(3);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
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
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
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
    checkOutput({1, 2, 3}, odata);
}

TEST_F(LimitKernelTest, testBatchLimitOversize) {
    _attributeMap["limit"] = Any(10);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
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
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2}));
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
    checkOutput({1, 2, 3, 4}, odata);
}

END_HA3_NAMESPACE(sql);
