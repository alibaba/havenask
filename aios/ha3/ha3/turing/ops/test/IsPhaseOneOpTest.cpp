#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
//#include <suez/turing/ops/test/OpTestBase.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class IsPhaseOneOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "IsPhaseOneOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request(uint32_t phaseNumber) {
        common::RequestPtr requestPtr(new common::Request(&_pool));
        common::ConfigClause *configClause = new common::ConfigClause();
        configClause->setPhaseNumber(phaseNumber);
        requestPtr->setConfigClause(configClause);
        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    void checkOutput(bool expectVal) {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);
        auto actualVal = pOutputTensor->scalar<bool>()();
        EXPECT_EQ(expectVal, actualVal);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, IsPhaseOneOpTest);


TEST_F(IsPhaseOneOpTest, testSimple) {
    makeOp();
    Variant variant = fakeHa3Request(SEARCH_PHASE_ONE);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true);
}

END_HA3_NAMESPACE();

