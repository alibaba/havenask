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
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultReconstructOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3ResultReconstructOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_UINT32))
                .Input(FakeInput(DT_UINT32))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void initSearcherQueryResource(bool setCollector = true) {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        if (!setCollector) {
            searcherQueryResource->sessionMetricsCollector = nullptr;
        }
        searcherQueryResource->setPool(&_pool);
    }

    Ha3ResultVariant fakeHa3ResultVariant(Ha3MatchDocAllocatorPtr matchDocAllocatorPtr, bool setMatchDocs = true) {
        ResultPtr resultPtr(new Result());
        if (setMatchDocs) {
            MatchDocs *matchDocs = new MatchDocs();
            matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);
            resultPtr->setMatchDocs(matchDocs);
        }
        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }

    MatchDocsVariant fakeMatchDocsVariant(Ha3MatchDocAllocatorPtr matchDocAllocator) {
        matchdoc::MatchDoc matchDoc1 = matchDocAllocator->allocate(100);
        matchdoc::MatchDoc matchDoc2 = matchDocAllocator->allocate(101);
        std::vector<matchdoc::MatchDoc> docs;
        docs.push_back(matchDoc1);
        docs.push_back(matchDoc2);
        MatchDocsVariant variant(matchDocAllocator, &_pool);
        variant.stealMatchDocs(docs);
        return variant;
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ResultReconstructOpTest);

TEST_F(Ha3ResultReconstructOpTest, testSimpleOp) {
    makeOp();

    initSearcherQueryResource(true);

    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);
    auto collector = searcherQueryResource->sessionMetricsCollector;
    ASSERT_TRUE(collector != nullptr);
    ASSERT_EQ(0u, collector->getReturnCount());

    auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
    Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);

    Ha3ResultVariant resultVariant = fakeHa3ResultVariant(matchDocAllocatorPtr);
    MatchDocsVariant matchDocsVariant = fakeMatchDocsVariant(matchDocAllocatorPtr);
    uint32_t totalMatchDocs = 10;
    uint32_t actualMatchDocs = 8;

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&matchDocsVariant](int x) -> Variant {
                return matchDocsVariant;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&totalMatchDocs](int x) -> uint32_t {
                return totalMatchDocs;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&actualMatchDocs](int x) -> uint32_t {
                return actualMatchDocs;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pResultOutputTensor = GetOutput(0);
    ASSERT_TRUE(pResultOutputTensor != nullptr);
    auto ha3ResultVariant = pResultOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    ASSERT_EQ(100, matchDocs->getDocId(0));
    ASSERT_EQ(101, matchDocs->getDocId(1));
    ASSERT_EQ(totalMatchDocs, matchDocs->totalMatchDocs());
    ASSERT_EQ(actualMatchDocs, matchDocs->actualMatchDocs());

    searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);
    collector = searcherQueryResource->sessionMetricsCollector;
}

TEST_F(Ha3ResultReconstructOpTest, testSimpleOpForNullMatchDocs) {
    makeOp();
    auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
    Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);

    Ha3ResultVariant resultVariant = fakeHa3ResultVariant(matchDocAllocatorPtr, false);
    MatchDocsVariant matchDocsVariant = fakeMatchDocsVariant(matchDocAllocatorPtr);
    uint32_t totalMatchDocs = 10;
    uint32_t actualMatchDocs = 8;

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&matchDocsVariant](int x) -> Variant {
                return matchDocsVariant;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&totalMatchDocs](int x) -> uint32_t {
                return totalMatchDocs;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&actualMatchDocs](int x) -> uint32_t {
                return actualMatchDocs;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pResultOutputTensor = GetOutput(0);
    ASSERT_TRUE(pResultOutputTensor != nullptr);
    auto ha3ResultVariant = pResultOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs == nullptr);

}

TEST_F(Ha3ResultReconstructOpTest, testSimpleOpForNullMetricsCollector) {
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);

    makeOp();
    initSearcherQueryResource(false);
    auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
    Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);

    Ha3ResultVariant resultVariant = fakeHa3ResultVariant(matchDocAllocatorPtr, true);
    MatchDocsVariant matchDocsVariant = fakeMatchDocsVariant(matchDocAllocatorPtr);
    uint32_t totalMatchDocs = 10;
    uint32_t actualMatchDocs = 8;

    AddInput<Variant>(
            TensorShape({}),
            [&resultVariant](int x) -> Variant {
                return resultVariant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&matchDocsVariant](int x) -> Variant {
                return matchDocsVariant;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&totalMatchDocs](int x) -> uint32_t {
                return totalMatchDocs;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&actualMatchDocs](int x) -> uint32_t {
                return actualMatchDocs;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pResultOutputTensor = GetOutput(0);
    ASSERT_TRUE(pResultOutputTensor != nullptr);
    auto ha3ResultVariant = pResultOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    ASSERT_EQ(100, matchDocs->getDocId(0));
    ASSERT_EQ(101, matchDocs->getDocId(1));
    ASSERT_EQ(totalMatchDocs, matchDocs->totalMatchDocs());
    ASSERT_EQ(actualMatchDocs, matchDocs->actualMatchDocs());

    searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);
    auto collector = searcherQueryResource->sessionMetricsCollector;
    ASSERT_TRUE(collector == nullptr);

}

END_HA3_NAMESPACE();
