#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/values/ValuesKernel.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class ValuesKernelTest : public OpTestBase {
public:
    ValuesKernelTest();
    ~ValuesKernelTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
private:
    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("ValuesKernel");
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
};

ValuesKernelTest::ValuesKernelTest() {
}

ValuesKernelTest::~ValuesKernelTest() {
}

TEST_F(ValuesKernelTest, testSimpleProcess) {
    _attributeMap["output_fields"] = ParseJson(
            R"json(["$f0", "$f1", "$f2", "$f3", "$f4", "$f5", "$f6", "$f7", "$f8", "$f9"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["BOOLEAN", "TINYINT", "SMALLINT",
                                              "INTEGER", "BIGINT", "DECIMAL", "FLOAT",
                                              "REAL", "DOUBLE", "VARCHAR"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    ASSERT_TRUE(tester.compute()); // kernel compute success
    TablePtr outputTable;
    getOutputTable(tester, outputTable);
    ASSERT_EQ(10, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<bool>(outputTable, "f0", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "f1", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "f2", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "f3", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "f4", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(outputTable, "f5", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(outputTable, "f6", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(outputTable, "f7", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(outputTable, "f8", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "f9", {}));
    ASSERT_EQ(EC_NONE, tester.getErrorCode());
    ASSERT_EQ("", tester.getErrorMessage());
}

TEST_F(ValuesKernelTest, testDupOutputField) {
    _attributeMap["output_fields"] = ParseJson(
            R"json(["f0", "f0"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["TINYINT", "SMALLINT"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    ASSERT_TRUE(tester.compute()); // kernel compute success
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("declare column [f0] failed, type [SMALLINT]", tester.getErrorMessage());
}

TEST_F(ValuesKernelTest, testWrongType) {
    _attributeMap["output_fields"] = ParseJson(
            R"json(["f0"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["xxx"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.hasError()); // kernel init success
    ASSERT_TRUE(tester.compute()); // kernel compute success
    ASSERT_EQ(EC_ABORT, tester.getErrorCode());
    ASSERT_EQ("output field [f0] with type [xxx] not supported", tester.getErrorMessage());
}

TEST_F(ValuesKernelTest, testSizeMismatch) {
    _attributeMap["output_fields"] = ParseJson(
            R"json(["f0", "f1"])json");
    _attributeMap["output_fields_type"] = ParseJson(R"json(["xxx"])json");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError()); // kernel init success
    ASSERT_EQ("output fields size[2] not equal output types size[1]", tester.getErrorMessage());
}

END_HA3_NAMESPACE(sql);
