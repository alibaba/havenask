#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);


class Ha3ReleaseRedundantOpTest : public Ha3OpTestBase {

public:
    void SetUp() override {
        OpTestBase::SetUp();
    }

protected:
    void makeRequiredTopK(uint32_t requiredTopK) {
        SearcherQueryResourcePtr searcherQueryResourcePtr = 
            dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        ASSERT_TRUE(searcherQueryResourcePtr != NULL);
        search::SearchRuntimeResourcePtr searchRuntimeResource(new search::SearchRuntimeResource);
        searchRuntimeResource->docCountLimits.requiredTopK = requiredTopK;
        searcherQueryResourcePtr->runtimeResource = searchRuntimeResource;
    }

    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3ReleaseRedundantOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3ResultVariant fakeHa3ResultWithMatchDocs(size_t docNum, int32_t startDocId, std::vector<int32_t>& docVec) {
        ResultPtr resultPtr(new Result());
        MatchDocs *matchDocs = new MatchDocs();

        auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
        Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
        matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

        for(size_t i = 0; i < docNum; i++) {
            int32_t docId = startDocId + i;
            matchDocs->addMatchDoc(matchDocAllocator->allocate(docId));
            docVec.emplace_back(docId);
        }
        
        resultPtr->setMatchDocs(matchDocs);

        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }

    Ha3ResultVariant fakeHa3ResultWithoutMatchDocs() {
        ResultPtr resultPtr(new Result());
        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }


    Ha3ResultVariant fakeHa3ResultWithoutAllocator() {
        ResultPtr resultPtr(new Result());
        MatchDocs *matchDocs = new MatchDocs();        
        resultPtr->setMatchDocs(matchDocs);
        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }

    void checkOutput(uint32_t expectPort) {
        auto pOutputTensor = GetOutput(expectPort);
        ASSERT_TRUE(pOutputTensor != nullptr);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ReleaseRedundantOpTest);


TEST_F(Ha3ReleaseRedundantOpTest, testDocNumLessThanTopK) {
    makeOp();

    uint32_t requiredTopK = 20;
    size_t docNum = 10;
    int32_t startDocId = 2;
 
    makeRequiredTopK(requiredTopK);

    std::vector<int32_t> docVec;
    Variant resultVariant = fakeHa3ResultWithMatchDocs(docNum, startDocId, docVec);

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(docNum, matchDocs->size());

    for(size_t i = 0; i < docNum; i++) {
        ASSERT_EQ(docVec[i], matchDocs->getDocId(i));
    }
}

TEST_F(Ha3ReleaseRedundantOpTest, testDocNumMoreThanTopK) {
    makeOp();

    uint32_t requiredTopK = 20;
    size_t docNum = 40;
    int32_t startDocId = 2;
 
    makeRequiredTopK(requiredTopK);

    std::vector<int32_t> docVec;
    Variant resultVariant = fakeHa3ResultWithMatchDocs(docNum, startDocId, docVec);

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(requiredTopK, matchDocs->size());

    for(size_t i = 0; i < requiredTopK; i++) {
        ASSERT_EQ(docVec[i], matchDocs->getDocId(i));
    }

}

TEST_F(Ha3ReleaseRedundantOpTest, testNullMatchDocs) {
    makeOp();

    uint32_t requiredTopK = 20;
 
    makeRequiredTopK(requiredTopK);

    std::vector<int32_t> docVec;
    Variant resultVariant = fakeHa3ResultWithoutMatchDocs();

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs == nullptr);
}

TEST_F(Ha3ReleaseRedundantOpTest, testNullAllocator) {
    makeOp();

    uint32_t requiredTopK = 20;
 
    makeRequiredTopK(requiredTopK);

    std::vector<int32_t> docVec;
    Variant resultVariant = fakeHa3ResultWithoutAllocator();

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_TRUE(matchDocs->getMatchDocAllocator() == nullptr);
}

END_HA3_NAMESPACE();

