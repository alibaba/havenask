#include <sap/util/environ_util.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/TuringUrlTransform.h>
#include <tensorflow/core/framework/tensor.h>
#include <ha3/common/ha3_op_common.h>

using namespace std;
using namespace sap;
using namespace testing;
using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class TuringUrlTransformTest : public TESTBASE {
protected:
    void expectTransform(
            const string& url,
            const string& bizName,
            const string& src,
            const map<string, string>& kvs)
    {
        unique_ptr<GraphRequest> result(_urlTransform.transform(url));
        ASSERT_TRUE(result.get() != nullptr);

        EXPECT_EQ(bizName, result->bizname());
        EXPECT_EQ(src, result->src());

        const auto& info = result->graphinfo();
        ASSERT_EQ(1, info.targets_size());
        EXPECT_EQ("run_done", info.targets(0));
        ASSERT_EQ(1, info.fetches_size());
        EXPECT_EQ("format_result", info.fetches(0));

        ASSERT_EQ(1, info.inputs_size());
        const auto& namedTensor = info.inputs(0);
        EXPECT_EQ("query_kv_pair", namedTensor.name());
        Tensor tensor;
        ASSERT_TRUE(tensor.FromProto(namedTensor.tensor()));
        auto pVaraint = tensor.scalar<Variant>()().get<KvPairVariant>();
        ASSERT_TRUE(pVaraint != nullptr);
        EXPECT_EQ(kvs, pVaraint->get());
    }
protected:
    TuringUrlTransform _urlTransform;
};


TEST_F(TuringUrlTransformTest, testEnvSetting) {
    EnvironUtil::setEnv("TURING_BIZ_NAME_FIELD", "BizEnv");
    EnvironUtil::setEnv("TURING_SRC_FIELD", "SrcEnv");
    ASSERT_TRUE(_urlTransform.init());
    expectTransform(
            "BizEnv=biz&SrcEnv=src&k1=v1&k2=v2&k3=v3",
            "biz",
            "src",
            {
                {"k1", "v1"},
                {"k2", "v2"},
                {"k3", "v3"},
                {"BizEnv", "biz"},
                {"SrcEnv", "src"},
            });
    EnvironUtil::setEnv("TURING_BIZ_NAME_FIELD", "");
    EnvironUtil::setEnv("TURING_SRC_FIELD", "");
}

TEST_F(TuringUrlTransformTest, testRegularUrl) {
    ASSERT_TRUE(_urlTransform.init());
    expectTransform(
            "s=biz&src=src&k1=v1&k2=v2&k3=v3",
            "biz",
            "src",
            {
                {"k1", "v1"},
                {"k2", "v2"},
                {"k3", "v3"},
                {"s", "biz"},
                {"src", "src"},
            });
}

TEST_F(TuringUrlTransformTest, testUrlWithoutBizname1) {
    ASSERT_TRUE(_urlTransform.init());
    auto result = _urlTransform.transform("k1=v1&k2=v2&k3=v3");
    ASSERT_FALSE(result);
}

TEST_F(TuringUrlTransformTest, testUrlWithoutBizname2) {
    ASSERT_TRUE(_urlTransform.init());
    auto result = _urlTransform.transform("s=&k1=v1&k2=v2&k3=v3");
    ASSERT_FALSE(result);
}

TEST_F(TuringUrlTransformTest, testUrlWithEmptyValue) {
    ASSERT_TRUE(_urlTransform.init());
    expectTransform(
            "s=biz&src=&k1=v1&k2=&k3=",
            "biz",
            "",
            {
                {"s", "biz"},
                {"src", ""},
                {"k1", "v1"},
                {"k2", ""},
                {"k3", ""},
            });
}

TEST_F(TuringUrlTransformTest, testUrlWithInvalidPair) {
    ASSERT_TRUE(_urlTransform.init());
    expectTransform(
            "s=biz&src&k1&=v2&k3=v3",
            "biz",
            "",
            {
                {"s", "biz"},
                {"k3", "v3"},
            });
}

TEST_F(TuringUrlTransformTest, testUrlWithSameKey) {
    ASSERT_TRUE(_urlTransform.init());
    expectTransform(
            "s=biz&src=src&k1=111&k1=222",
            "biz",
            "src",
            {
                {"s", "biz"},
                {"src", "src"},
                {"k1", "111"},
            });
}

END_HA3_NAMESPACE(turing);
