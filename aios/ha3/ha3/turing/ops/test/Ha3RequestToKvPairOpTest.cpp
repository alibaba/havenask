#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <basic_variant/variant/KvPairVariant.h>


using namespace std;
using namespace testing;
using namespace tensorflow;
using namespace suez::turing;
using namespace ::testing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class Ha3RequestToKvPairOpTest : public Ha3OpTestBase {
private:
    void prepareOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3RequestToKvPairOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void prepareSimpleInput() {
        _request.reset(new common::Request(&_pool));
        _request->setConfigClause(new ConfigClause());

        Ha3RequestVariant requestVariant(_request, &_pool);
        AddInputFromArray<Variant>(TensorShape({}), {requestVariant});
    }

private:
    common::RequestPtr _request;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(ops, Ha3RequestToKvPairOpTest);

TEST_F(Ha3RequestToKvPairOpTest, testNormalConvert) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    ConfigClause *configClause = _request->getConfigClause();
    ASSERT_NE(nullptr, configClause);
    KeyValueMapPtr inKvPair = configClause->getKeyValueMapPtr();
    (*inKvPair)["hello"] = "world";

    TF_ASSERT_OK(RunOpKernel());

    auto *kvPairVariant = GetOutputVariant<KvPairVariant>(0);
    ASSERT_NE(nullptr, kvPairVariant);
    KeyValueMapPtr outKvPair = kvPairVariant->getKvPair();
    ASSERT_NE(nullptr, outKvPair);

    ASSERT_EQ(*inKvPair, *outKvPair);
}


END_HA3_NAMESPACE();
