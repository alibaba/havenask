#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class Ha3SearcherCacheSwitchOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeCacheHit(bool hit) {
        SearcherQueryResourcePtr searcherQueryResourcePtr = 
            dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        ASSERT_TRUE(searcherQueryResourcePtr != NULL);
        search::SearcherCacheInfo *cacheInfo = new search::SearcherCacheInfo;
        cacheInfo->isHit = hit;
        searcherQueryResourcePtr->searcherCacheInfo.reset(cacheInfo);
    }
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SearcherCacheSwitchOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request() {
        common::RequestPtr requestPtr(new common::Request(&_pool));
        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    void checkOutput(uint32_t expectPort) {
        auto pOutputTensor = GetOutput(expectPort);
        ASSERT_TRUE(pOutputTensor != nullptr);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SearcherCacheSwitchOpTest);


TEST_F(Ha3SearcherCacheSwitchOpTest, testDisableCache) {
    makeOp();
    Variant variant = fakeHa3Request();
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);
}

TEST_F(Ha3SearcherCacheSwitchOpTest, testMissCache) {
    makeOp();
    makeCacheHit(false);
    Variant variant = fakeHa3Request();
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(1);
}

TEST_F(Ha3SearcherCacheSwitchOpTest, testHitCache) {
    makeOp();
    makeCacheHit(true);
    Variant variant = fakeHa3Request();
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(2);
}

END_HA3_NAMESPACE();

