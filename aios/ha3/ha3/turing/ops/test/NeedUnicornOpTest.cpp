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

class NeedUnicornOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "NeedUnicornOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request(const vector<string> &keys,
            const vector<string> &values) {
        common::RequestPtr requestPtr(new common::Request(&_pool));
        common::ConfigClause *configClause = new common::ConfigClause();
        for (size_t i = 0; i < keys.size(); ++i) {
            configClause->addKVPair(keys[i], values[i]);
        }
        requestPtr->setConfigClause(configClause);
        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    Ha3RequestVariant emptyHa3Request() {
        common::RequestPtr requestPtr(new common::Request(&_pool));
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

HA3_LOG_SETUP(turing, NeedUnicornOpTest);


TEST_F(NeedUnicornOpTest, testNeedUnicornTrue) {
    makeOp();
    vector<string> keys{"need_unicorn", "aaa"};
    vector<string> values{"true", "bbb"};
    Variant variant = fakeHa3Request(keys, values);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true);
}

TEST_F(NeedUnicornOpTest, testNeedUnicornYes) {
    makeOp();
    vector<string> keys{"need_unicorn", "aaa"};
    vector<string> values{"yes", "bbb"};
    Variant variant = fakeHa3Request(keys, values);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(true);
}

TEST_F(NeedUnicornOpTest, testNeedUnicornOther) {
    makeOp();
    vector<string> keys{"need_unicorn", "aaa"};
    vector<string> values{"ccc", "bbb"};
    Variant variant = fakeHa3Request(keys, values);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false);
}

TEST_F(NeedUnicornOpTest, testNoNeedUnicorn) {
    makeOp();
    vector<string> keys{"ccc", "aaa"};
    vector<string> values{"ddd", "bbb"};
    Variant variant = fakeHa3Request(keys, values);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false);
}

TEST_F(NeedUnicornOpTest, testEmptyKVPairs) {
    makeOp();
    Variant variant = emptyHa3Request();
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(false);
}

END_HA3_NAMESPACE();
