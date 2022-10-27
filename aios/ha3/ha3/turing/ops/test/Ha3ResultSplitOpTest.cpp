#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultSplitOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3ResultSplitOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request(uint32_t start = 0, uint32_t hit = 10) {
        RequestPtr requestPtr(new Request(&_pool));
        ConfigClause *configClause = new ConfigClause();
        configClause->setStartOffset(start);
        configClause->setHitCount(hit);

        requestPtr->setConfigClause(configClause);

        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    Ha3ResultVariant fakeHa3ResultWithMatchDocs() {
        ResultPtr resultPtr(new Result());
        MatchDocs *matchDocs = new MatchDocs();

        auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
        Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
        matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

        matchdoc::MatchDoc matchDoc1 = matchDocAllocator->allocate(100);
        matchdoc::MatchDoc matchDoc2 = matchDocAllocator->allocate(101);
        matchDocs->addMatchDoc(matchDoc1);
        matchDocs->addMatchDoc(matchDoc2);

        matchDocs->setTotalMatchDocs(2u);
        matchDocs->setActualMatchDocs(2u);

        resultPtr->setMatchDocs(matchDocs);

        resultPtr->setSrcCount(4);

        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ResultSplitOpTest);

TEST_F(Ha3ResultSplitOpTest, testSimpleOp) {
    makeOp();

    uint32_t start = 10, hit = 20;
    uint32_t topK = start + hit;

    Variant variant1 = fakeHa3Request(start, hit);
    Variant variant2 = fakeHa3ResultWithMatchDocs();

    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(2u, matchDocVec.size());
    ASSERT_EQ(100, matchDocVec[0].getDocId());
    ASSERT_EQ(101, matchDocVec[1].getDocId());

    auto pExtraMatchDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraMatchDocsOutputTensor != nullptr);
    auto extraMatchDocs = pExtraMatchDocsOutputTensor ->scalar<uint32_t>()();
    ASSERT_EQ(topK, extraMatchDocs);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto totalMatchDocs = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(2u, totalMatchDocs);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto actualMatchDocs = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    ASSERT_EQ(2u, actualMatchDocs);

    auto pResultOutputTensor = GetOutput(4);
    ASSERT_TRUE(pResultOutputTensor != nullptr);
    auto ha3ResultVariant = pResultOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(0u, matchDocs->size());
    
    auto pSrcCountOutputTensor = GetOutput(5);
    ASSERT_TRUE(pSrcCountOutputTensor != nullptr);
    auto srcCountOutput = pSrcCountOutputTensor->scalar<uint16_t>()();
    ASSERT_EQ(4u, srcCountOutput);
}

END_HA3_NAMESPACE();

