#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/service/ServiceDegrade.h>
#include <ha3/config/ServiceDegradationConfig.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(turing);

#define RANK_SIZE 100
#define RERANK_SIZE 1000

class AddDegradeInfoOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        Ha3OpTestBase::SetUp();
    }
protected:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "AddDegradeInfoOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request(uint8_t phaseOneBasicInfoMask = 0) {
        RequestPtr requestPtr(new Request(&_pool));
        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    void checkOutput(float degradeLevel) {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);

        auto requestVariant = pOutputTensor->scalar<Variant>()().get<Ha3RequestVariant>();
        RequestPtr request = requestVariant->getRequest();
        float level;
        uint32_t rankSize;
        uint32_t rerankSize;
        request->getDegradeLevel(level, rankSize, rerankSize);
        ASSERT_FLOAT_EQ(degradeLevel, level);
        if (degradeLevel > 0) {
            ASSERT_EQ(RANK_SIZE, rankSize);
            ASSERT_EQ(RERANK_SIZE, rerankSize);
        } else {
            ASSERT_EQ(0, rankSize);
            ASSERT_EQ(0, rankSize);
        }
    }

    void initSearcherSessionResource() {
        auto searcherSessionResource = dynamic_pointer_cast<SearcherSessionResource>(
                _sessionResource);
        searcherSessionResource->searcherResource.reset(new HA3_NS(service)::SearcherResource);
        ServiceDegradationConfig config;
        config.request.rankSize = RANK_SIZE;
        config.request.rerankSize = RERANK_SIZE;
        HA3_NS(service)::ServiceDegradePtr serviceDegradePtr(new HA3_NS(service)::ServiceDegrade(config));
        searcherSessionResource->searcherResource->setServiceDegrade(serviceDegradePtr);
    }

    void initSearcherQueryResource(float degradeLevel) {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        searcherQueryResource->setDegradeLevel(degradeLevel);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, AddDegradeInfoOpTest);


TEST_F(AddDegradeInfoOpTest, testWithZeroDegradeLevel) {
    makeOp();
    initSearcherSessionResource();
    initSearcherQueryResource(0);

    Variant variant1 = fakeHa3Request();
    AddInputFromArray<Variant>(TensorShape({}), {variant1});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);
}

TEST_F(AddDegradeInfoOpTest, testWithDegradation) {
    makeOp();
    initSearcherSessionResource();
    initSearcherQueryResource(0.5);

    Variant variant1 = fakeHa3Request();
    AddInputFromArray<Variant>(TensorShape({}), {variant1});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0.5);
}


#undef RANK_SIZE
#undef RERANK_SIZE

END_HA3_NAMESPACE();

