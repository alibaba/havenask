#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <basic_ops/ops/test/OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);


class RequestInitOpTest : public OpTestBase {
public:
    void SetUp() override {
        _needTracer = false;
        _needTerminator = false;
        OpTestBase::SetUp();
    }
public:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myRequestInitOp", "RequestInitOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(bool hasTimer, bool hasTracer) {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);

        auto ha3RequestVariant = pOutputTensor->scalar<Variant>()().get<Ha3RequestVariant>();
        ASSERT_TRUE(ha3RequestVariant != nullptr);
        auto request = ha3RequestVariant->getRequest();
        ASSERT_TRUE(request != nullptr);

        QueryResourcePtr queryResource = getQueryResource();
        auto timeoutTerminator = queryResource->getTimeoutTerminator();
        ASSERT_EQ(hasTimer, timeoutTerminator != nullptr);
        auto tracer = queryResource->getTracer();
        ASSERT_EQ(hasTracer, tracer != nullptr);
    }

private:
    Ha3RequestVariant prepareInitRequestVariant(int32_t timeout, const string &traceLevel ) {
        RequestPtr requestPtr(new Request(&_pool));
        ConfigClause *configClause = new ConfigClause();
        configClause->setRpcTimeOut(timeout);
        configClause->setTrace(traceLevel);
        requestPtr->setConfigClause(configClause);
        Ha3RequestVariant variant(requestPtr, &_pool);
        VariantTensorData data;
        variant.Encode(&data);
        Ha3RequestVariant newVariant;
        newVariant.Decode(data);
        return newVariant;
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, RequestInitOpTest);

TEST_F(RequestInitOpTest, testDefaultRequest) {
    makeOp();
    Ha3RequestVariant variant = prepareInitRequestVariant(0, "");
    ASSERT_TRUE(variant.getRequest() == NULL);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true, false);
}

TEST_F(RequestInitOpTest, testRequestWithTracer) {
    makeOp();
    Ha3RequestVariant variant = prepareInitRequestVariant(0, "ERROR");
    ASSERT_TRUE(variant.getRequest() == NULL);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true, true);
}

TEST_F(RequestInitOpTest, testRequestWithTerminator) {
    makeOp();
    Ha3RequestVariant variant = prepareInitRequestVariant(100, "");
    ASSERT_TRUE(variant.getRequest() == NULL);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true, false);
}

TEST_F(RequestInitOpTest, testRequestWithTimeout) {
    makeOp();
    Ha3RequestVariant variant = prepareInitRequestVariant(100, "");
    usleep(200 * 1000);
    ASSERT_TRUE(variant.getRequest() == NULL);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    Status status = RunOpKernel();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ("timeout, current in query_init_op.", status.error_message());
}

END_HA3_NAMESPACE();
