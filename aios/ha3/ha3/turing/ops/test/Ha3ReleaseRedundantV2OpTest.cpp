#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/Ha3ReleaseRedundantV2Op.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(turing);


class Ha3ReleaseRedundantV2OpTest : public Ha3OpTestBase {
private:
    common::RequestPtr _request;
    SearcherQueryResourcePtr _searcherQueryResource;
    Ha3ReleaseRedundantV2Op *_ha3ReleaseRedundantV2Op;
public:
    void SetUp() override {
        OpTestBase::SetUp();
        _request.reset(new common::Request());
        _searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        ASSERT_TRUE(_searcherQueryResource != NULL);
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

    Ha3RequestVariant fakeHa3Request(SearcherCacheClause* cacheClause) {
        _request.get()->setSearcherCacheClause(cacheClause);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    void fakeSearcherQueryResource(bool useCache, bool isHit) {
        if (!useCache) {
            _searcherQueryResource->searcherCacheInfo.reset();
            return;
        }
        _searcherQueryResource->searcherCacheInfo.reset(new SearcherCacheInfo());
        _searcherQueryResource->searcherCacheInfo->isHit = isHit;
    }

    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3ReleaseRedundantV2Op")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _ha3ReleaseRedundantV2Op = dynamic_cast<Ha3ReleaseRedundantV2Op *>(kernel_.get());
    }

    void checkOutput(uint32_t expectPort) {
        auto pOutputTensor = GetOutput(expectPort);
        ASSERT_TRUE(pOutputTensor != nullptr);
    }

    MatchDocsVariant fakeMatchDocs(size_t docNum, int32_t startDocId, std::vector<int32_t>& docIdVec) {
        auto matchDocAllocatorPtr = MatchDocAllocatorPtr(new Ha3MatchDocAllocator(&_pool));
        MatchDocsVariant variant(matchDocAllocatorPtr, &_pool);
        std::vector<matchdoc::MatchDoc> matchDocs;
        for (size_t i = 0; i < docNum; ++i) {
            int32_t docId = startDocId + i;
            matchDocs.push_back(matchDocAllocatorPtr->allocate(docId));
            docIdVec.emplace_back(docId);
        }
        variant.stealMatchDocs(matchDocs);
        return variant;
    }

    MatchDocsVariant fakeMatchDocsWithoutMatchDocs() {
        auto matchDocAllocatorPtr = MatchDocAllocatorPtr(new Ha3MatchDocAllocator(&_pool));
        MatchDocsVariant variant(matchDocAllocatorPtr, &_pool);
        return variant;
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ReleaseRedundantV2OpTest);


TEST_F(Ha3ReleaseRedundantV2OpTest, testDocNumLessThanTopK) {
    makeOp();

    uint32_t requiredTopK = 20;
    size_t docNum = 10;
    int32_t startDocId = 2;

    makeRequiredTopK(requiredTopK);

    Variant requestVariant = fakeHa3Request(NULL);
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    std::vector<int32_t> docVec;
    Variant inputMatchDocsVariant = fakeMatchDocs(docNum, startDocId, docVec);
    AddInput<Variant>(
            TensorShape({}),
            [&inputMatchDocsVariant](int x) -> Variant {
                return inputMatchDocsVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto matchDocsVariant = pOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);

    std::vector<MatchDoc> matchDocsVec;
    matchDocsVariant->stealMatchDocs(matchDocsVec);
    ASSERT_EQ(docNum, matchDocsVec.size());

    for(size_t i = 0; i < docNum; i++) {
        ASSERT_EQ(docVec[i], matchDocsVec[i].getDocId());
    }
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testDocNumMoreThanTopK) {
    makeOp();

    uint32_t requiredTopK = 20;
    size_t docNum = 40;
    int32_t startDocId = 2;

    makeRequiredTopK(requiredTopK);

    Variant requestVariant = fakeHa3Request(NULL);
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    std::vector<int32_t> docVec;
    Variant inputMatchDocsVariant = fakeMatchDocs(docNum, startDocId, docVec);
    AddInput<Variant>(
            TensorShape({}),
            [&inputMatchDocsVariant](int x) -> Variant {
                return inputMatchDocsVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto matchDocsVariant = pOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    std::vector<MatchDoc> matchDocsVec;
    matchDocsVariant->stealMatchDocs(matchDocsVec);
    ASSERT_EQ(requiredTopK, matchDocsVec.size());

    for(size_t i = 0; i < requiredTopK; i++) {
        ASSERT_EQ(docVec[i], matchDocsVec[i].getDocId());
    }
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testNoMatchDocs) {
    makeOp();

    uint32_t requiredTopK = 20;

    makeRequiredTopK(requiredTopK);

    Variant requestVariant = fakeHa3Request(NULL);
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    std::vector<int32_t> docVec;
    Variant inputMatchDocsVariant = fakeMatchDocsWithoutMatchDocs();

    AddInput<Variant>(
            TensorShape({}),
            [&inputMatchDocsVariant](int x) -> Variant {
                return inputMatchDocsVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto matchDocsVariant = pOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    std::vector<MatchDoc> matchDocsVec;
    matchDocsVariant->stealMatchDocs(matchDocsVec);
    ASSERT_EQ(0, matchDocsVec.size());
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testNotUseCache) {
    makeOp();

    uint32_t requiredTopK = 20;
    bool useCache = false;
    bool isHit = false;

    _request.get()->setSearcherCacheClause(new SearcherCacheClause());
    fakeSearcherQueryResource(useCache, isHit);
    uint32_t resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(requiredTopK, resultCnt);
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testUseCacheAndHit) {
    makeOp();

    uint32_t requiredTopK = 20;
    bool useCache = true;
    bool isHit = true;

    SearcherCacheClause *cacheClause = new SearcherCacheClause();
    _request.get()->setSearcherCacheClause(cacheClause);
    fakeSearcherQueryResource(useCache, isHit);
    uint32_t resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());

    ASSERT_EQ(requiredTopK, resultCnt);
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testUseCacheAndNotHit) {
    makeOp();

    bool useCache = true;
    bool isHit = false;

    SearcherCacheClause *cacheClause = new SearcherCacheClause();
    const vector<uint32_t> cacheDocNumVec = { 20, 200 };
    cacheClause->setCacheDocNumVec(cacheDocNumVec);
    _request.get()->setSearcherCacheClause(cacheClause);

    fakeSearcherQueryResource(useCache, isHit);

    uint32_t requiredTopK = 20;
    uint32_t resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(requiredTopK, resultCnt);

    requiredTopK = 10;
    resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(cacheDocNumVec[0], resultCnt);

    requiredTopK = 100;
    resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(cacheDocNumVec[1], resultCnt);

    requiredTopK = 300;
    resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(requiredTopK, resultCnt);
}

TEST_F(Ha3ReleaseRedundantV2OpTest, testNoCacheClause) {
    makeOp();

    uint32_t requiredTopK = 20;
    bool useCache = true;
    bool isHit = true;

    _request.get()->setSearcherCacheClause(NULL);
    fakeSearcherQueryResource(useCache, isHit);
    uint32_t resultCnt = _ha3ReleaseRedundantV2Op->getResultCount(_request.get(), requiredTopK, _searcherQueryResource.get());
    ASSERT_EQ(requiredTopK, resultCnt);
}

END_HA3_NAMESPACE();
