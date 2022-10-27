#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <suez/turing/common/CavaConfig.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/turing/ops/Ha3ResultMergeOp.h>
#include <ha3/turing/ops/test/MockCavaJitWrapper.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/common/test/ResultConstructor.h>


using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(turing);


class Ha3ResultMergeOpTest : public Ha3OpTestBase {

private:
    Ha3ResultMergeOp *_mergeOp = nullptr;

    common::ResultConstructor _resultConstructor;
    common::RequestPtr _request;
    std::vector<common::ResultPtr> _results;

public:
    void SetUp() override {
        OpTestBase::SetUp();
        _mergeOp = NULL;
        _request.reset();
        _results.clear();
    }

    void TearDown() override {
        _results.clear();
        _request.reset();
        _mergeOp = NULL;
        OpTestBase::TearDown();
    }

    void prepareMergeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3ResultMergeOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Attr("self_define_type", "Switch")
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _mergeOp = dynamic_cast<Ha3ResultMergeOp *>(kernel_.get());
    }

    void prepareRequest(uint32_t startOffset, uint32_t hitCount,
                        uint32_t sortRefCount, bool isAscFlag = false) {
        _request.reset(new Request(&_pool));

        QueryClause *queryClause = new QueryClause(NULL);
        _request->setQueryClause(queryClause);

        ConfigClause *configClause = new ConfigClause();
        configClause->setStartOffset(startOffset);
        configClause->setHitCount(hitCount);
        _request->setConfigClause(configClause);

        SortClause *sortClause = new SortClause();
        for (size_t i = 0; i < sortRefCount; ++i) {
            SyntaxExpr *syntax = new AtomicSyntaxExpr(
                    "SORT_FIELD_PREFIX_" + autil::StringUtil::toString(i),
                    vt_int32, ::ATTRIBUTE_NAME);
            sortClause->addSortDescription(syntax, false, isAscFlag);
        }
        _request->setSortClause(sortClause);
    }

    void createEmptyResult(uint32_t count) {
        for (uint32_t i = 0; i < count; i++) {
            Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(&_pool));
            MatchDocs *matchDocs = new MatchDocs;
            matchDocs->setMatchDocAllocator(allocatorPtr);
            Result *result = new Result(matchDocs);
            ResultPtr resultPtr(result);
            _results.push_back(resultPtr);
        }
    }

    void addAggregateDescription(RequestPtr request, const string &groupExprStr) {
        AggregateClause *aggClause = request->getAggregateClause();
        if (NULL == aggClause) {
            aggClause = new AggregateClause();
            request->setAggregateClause(aggClause);
            aggClause = request->getAggregateClause();
        }

        AggregateDescription *des = new AggregateDescription(groupExprStr);
        SyntaxExpr *syntax = new AtomicSyntaxExpr(
                groupExprStr, vt_int32, ::ATTRIBUTE_NAME);
        des->setGroupKeyExpr(syntax);
        aggClause->addAggDescription(des);
    }

    ResultPtr createSerializedResult(const string &value) {
        MatchDocs* matchDocs = new MatchDocs();
        ResultConstructor resultConstructor;
        resultConstructor.fillMatchDocs(matchDocs, 1, 0, 10, 10, &_pool, value);
        Result originResult(matchDocs);

        autil::DataBuffer dataBuffer;
        originResult.serialize(dataBuffer);
        Result *serializedResult = new Result();
        ResultPtr ptr(serializedResult);
        serializedResult->deserialize(dataBuffer, &_pool);

        ptr->getMultiErrorResult()->addError(ERROR_UNKNOWN);
        return ptr;
    }

    void mergeTwoTrace() {
        _request->getConfigClause()->setTrace("INFO");
        ResultPtr resultPtr1(new Result(new Hits));
        resultPtr1->setTracer(common::TracerPtr(new common::Tracer));
        std::string str("test1");
        resultPtr1->getTracer()->trace(str);
        ResultPtr resultPtr2(new Result(new Hits));
        resultPtr2->setTracer(common::TracerPtr(new common::Tracer));
        resultPtr2->getTracer()->trace("test2");
        _results.push_back(resultPtr1);
        _results.push_back(resultPtr2);
        ASSERT_EQ((size_t)2, _results.size());

        _resultConstructor.fillHit(_results[0]->getHits(), 1, 2, "userid, price", "3, uid1, price1 # 5, uid2, price2");
        _resultConstructor.fillHit(_results[1]->getHits(), 2, 2, "userid, price", "1, uid3, price3 # 7, uid4, price4");

        ResultPtr mergedResultPtr(new Result);
        _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

        ASSERT_TRUE(mergedResultPtr->getTracer() != NULL);
        string expectedString = string("test1\ntest2\n");
        ASSERT_EQ(expectedString, mergedResultPtr->getTracer()->getTraceInfo());
    }
    
    void mergeOneTrace() {
        _request->getConfigClause()->setTrace("INFO");
        ResultPtr resultPtr1(new Result(new Hits));
        resultPtr1->setTracer(common::TracerPtr(new common::Tracer));
        std::string str("test1");
        resultPtr1->getTracer()->trace(str);
        _results.push_back(resultPtr1);
        ASSERT_EQ((size_t)1, _results.size());
        _resultConstructor.fillHit(_results[0]->getHits(), 1, 2, "userid, price", "3, uid1, price1 # 5, uid2, price2");

        ResultPtr mergedResultPtr(new Result);
        _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

        ASSERT_TRUE(mergedResultPtr->getTracer() != NULL);
        string expectedString = string("test1\n");
        ASSERT_EQ(expectedString, mergedResultPtr->getTracer()->getTraceInfo());
}

    void mergeNullTrace() {
        ResultPtr resultPtr1(new Result());
        ResultPtr resultPtr2(new Result());
        _results.push_back(resultPtr1);
        _results.push_back(resultPtr2);
        ASSERT_EQ((size_t)2, _results.size());

        ResultPtr mergedResultPtr(new Result);
        _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

        ASSERT_TRUE(mergedResultPtr->getTracer() == NULL);
    }

    void mergeSecondNullTrace() {
        _request->getConfigClause()->setTrace("INFO");
        ResultPtr resultPtr1(new Result(new Hits));
        resultPtr1->setTracer(common::TracerPtr(new common::Tracer));
        resultPtr1->getTracer()->trace("test1");
        ResultPtr resultPtr2(new Result(new Hits));
        _results.push_back(resultPtr1);
        _results.push_back(resultPtr2);
        ASSERT_EQ((size_t)2, _results.size());

        _resultConstructor.fillHit(_results[0]->getHits(), 1, 2, "userid, price", "3, uid1, price1 # 5, uid2, price2");
        _resultConstructor.fillHit(_results[1]->getHits(), 2, 2, "userid, price", "1, uid3, price3 # 7, uid4, price4");

        ResultPtr mergedResultPtr(new Result);
        _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

        ASSERT_TRUE(mergedResultPtr->getTracer() != NULL);
        string expectedString = string("test1\n");
        ASSERT_EQ(expectedString, mergedResultPtr->getTracer()->getTraceInfo());
    }

    void mergeFirstNullTrace() {
        _request->getConfigClause()->setTrace("INFO");
        ResultPtr resultPtr1(new Result(new Hits));
        ResultPtr resultPtr2(new Result(new Hits));
        resultPtr2->setTracer(common::TracerPtr(new common::Tracer));
        resultPtr2->getTracer()->trace("test2");

        _results.push_back(resultPtr1);
        _results.push_back(resultPtr2);
        ASSERT_EQ((size_t)2, _results.size());

        _resultConstructor.fillHit(_results[0]->getHits(), 1, 2, "userid, price", "3, uid1, price1 # 5, uid2, price2");
        _resultConstructor.fillHit(_results[1]->getHits(), 2, 2, "userid, price", "1, uid3, price3 # 7, uid4, price4");

        ResultPtr mergedResultPtr(new Result);
        _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

        ASSERT_TRUE(mergedResultPtr->getTracer() != NULL);
        string expectedString = string("test2\n");
        ASSERT_EQ(expectedString, mergedResultPtr->getTracer()->getTraceInfo());
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ResultMergeOpTest);

TEST_F(Ha3ResultMergeOpTest, testSimpleProcess) {
    prepareMergeOp();

    prepareRequest(0, 10, 1, true);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);

    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());
    ASSERT_TRUE(mergedResultPtr.get());
    ASSERT_TRUE(!mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergeOneResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    createEmptyResult(1);
    ASSERT_EQ((size_t)1, _results.size());

    ResultPtr resultPtr = _results[0];
    resultPtr->getMatchDocs()->setTotalMatchDocs(8888);
    resultPtr->getMatchDocs()->setActualMatchDocs(7777);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)8888, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)7777, mergedMatchDocs->actualMatchDocs());
    ASSERT_FALSE(mergedMatchDocs->hasPrimaryKey());
}

TEST_F(Ha3ResultMergeOpTest, testMergeOneResultWithError) {
    prepareMergeOp();
    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    createEmptyResult(1);
    ASSERT_EQ((size_t)1, _results.size());

    ResultPtr resultPtr = _results[0];
    resultPtr->getMatchDocs()->setTotalMatchDocs(8888);
    resultPtr->getMatchDocs()->setActualMatchDocs(7777);
    string errorMsg = "no summary profile";
    ErrorResult error(ERROR_NO_SUMMARYPROFILE, errorMsg);
    resultPtr->addErrorResult(error);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)8888, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)7777, mergedMatchDocs->actualMatchDocs());

    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_NO_SUMMARYPROFILE, errors[0].getErrorCode());
    ASSERT_EQ(errorMsg, errors[0].getErrorMsg());
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoResultWithError) {
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 10, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    string errorMsg = "no summary profile";
    ErrorResult error(ERROR_NO_SUMMARYPROFILE, errorMsg);
    _results[0]->addErrorResult(error);

    string errorMsg2 = "rpc timeout";
    ErrorResult error2(ERROR_SEARCH_UNKNOWN, errorMsg2);
    _results[1]->addErrorResult(error2);
    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 88,8 # 14,22, 88,9 # 15,22,88,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());
    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->actualMatchDocs());

    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)2, errors.size());
    ASSERT_EQ(ERROR_NO_SUMMARYPROFILE, errors[0].getErrorCode());
    ASSERT_EQ(errorMsg, errors[0].getErrorMsg());
    ASSERT_EQ(errorMsg2, errors[1].getErrorMsg());
    ASSERT_EQ(ERROR_SEARCH_UNKNOWN, errors[1].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoResultWithSecondResultIsEmpty)
{
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 5, sortReferCount);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setDoDedup(true);
    createEmptyResult(1);

    Result *result = new Result();
    ResultPtr resultPtr(result);
    _results.push_back(resultPtr);

    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 1, 1, &_pool,
            "1,22, 77,1");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)1, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)1, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(1));
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoDistinctTwoResult) {
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 5, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 88,8 # 14,22, 88,9 # 15,22,88,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->actualMatchDocs());
    ASSERT_FALSE(mergedMatchDocs->hasPrimaryKey());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(10));
}

TEST_F(Ha3ResultMergeOpTest, testMergeWithNotSameVSA) {
    prepareMergeOp();
    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 100, 100,  &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 1, 0, 200, 200, &_pool,
            "11, 77,3 # 12, 77,4 # 13, 88,8 # 14, 88,9 # 15,88,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_VSA_NOT_SAME, errors[0].getErrorCode());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((uint32_t)5, mergedMatchDocs->size());
    ASSERT_FALSE(mergedMatchDocs->hasPrimaryKey());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(5));
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoDistinctTwoResultWithDifferentHashid) {
    prepareMergeOp();
    uint32_t sortReferCount = 1;
    prepareRequest(0, 5, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 100, 100, &_pool,
            "1,22, 77 # 2,22, 77 # 3,22, 88 # 4,22, 88 # 15,22,88");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 200, 200, &_pool,
            "11,21, 77 # 12,21, 77 # 13,21, 88 # 14,21, 88 # 15,21,88");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    auto hashIdRef = mergedMatchDocs->getHashIdRef();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->actualMatchDocs());
    ASSERT_FALSE(mergedMatchDocs->hasPrimaryKey());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_EQ((hashid_t)22, hashIdRef->get(mergedMatchDocs->getMatchDoc(1)));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(10));
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoDistinctTwoResultWithAscendSortFlag) {
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 5, sortReferCount, true);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7", true);
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 88,8 # 14,22, 88,9 # 15,22,88,10", true);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)300, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(10));
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoDistinctThreeResult) {
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 5, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(3);
    ASSERT_EQ((size_t)3, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 100, 100, &_pool,
            "1,22,88,1 # 2,22,77,2 # 3,22,66,3 # 4,22,66,3 # 5,22,55,2");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 200, 200, &_pool,
            "11,22,77,3 # 12,22,66,8 # 13,22,66,4 # 14,22,55,9 # 15,22,44,10");
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(), sortReferCount, 0, 300, 280, &_pool,
            "21,22,88,3 # 22,22,88,2 # 23,22,55,8 # 24,22,11,9 # 25,22,11,8");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)600, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)580, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_EQ((docid_t)21, mergedMatchDocs->getMatchDoc(10).getDocId());
    ASSERT_EQ((docid_t)22, mergedMatchDocs->getMatchDoc(11).getDocId());
    ASSERT_EQ((docid_t)23, mergedMatchDocs->getMatchDoc(12).getDocId());
    ASSERT_EQ((docid_t)24, mergedMatchDocs->getMatchDoc(13).getDocId());
    ASSERT_EQ((docid_t)25, mergedMatchDocs->getMatchDoc(14).getDocId());
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoDistinctThreeResultWithOneEmpty) {
    prepareMergeOp();
    uint32_t sortReferCount = 2;
    prepareRequest(0, 5, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(3);
    ASSERT_EQ((size_t)3, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), sortReferCount, 0, 0, 0, &_pool, "");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), sortReferCount, 0, 3, 3, &_pool,
            "11,22,77,3 # 13,22,66,4 # 15,22,44,10");
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(), sortReferCount, 0, 10, 10, &_pool,
            "21,22,88,3 # 22,22,88,2 # 23,22,55,8 # 24,22,11,9 # 25,22,11,8");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)13, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)8, mergedMatchDocs->size());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)21, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)22, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)23, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)24, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)25, mergedMatchDocs->getMatchDoc(7).getDocId());
}

TEST_F(Ha3ResultMergeOpTest, testCheckFullEmptyAggResult) {
    prepareMergeOp();
    size_t expectSize = 2;
    createEmptyResult(1);
    ASSERT_EQ((size_t)1, _results.size());
    ASSERT_TRUE(_mergeOp->checkAggResult(_results[0], expectSize));
}

TEST_F(Ha3ResultMergeOpTest, testCheckFullAggResult) {
    prepareMergeOp();
    size_t expectSize = 2;
    string groupExprStr1 = "fakeGroupExprStr1";
    string groupExprStr2 = "fakeGroupExprStr2";

    createEmptyResult(1);
    _resultConstructor.fillAggregateResult(_results[0], "sum", "", &_pool, groupExprStr1);
    _resultConstructor.fillAggregateResult(_results[0], "count", "", &_pool, groupExprStr2);

    ASSERT_TRUE(_mergeOp->checkAggResult(_results[0], expectSize));
}

TEST_F(Ha3ResultMergeOpTest, testCheckNotFullAggResult) {
    prepareMergeOp();
    size_t expectSize = 2;
    string groupExprStr1 = "fakeGroupExprStr1";

    createEmptyResult(1);
    _resultConstructor.fillAggregateResult(_results[0], "sum", "",  &_pool, groupExprStr1);

    ASSERT_FALSE(_mergeOp->checkAggResult(_results[0], expectSize));
}

TEST_F(Ha3ResultMergeOpTest, testMergeOneAggregateResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupStr = "groupExprStr";
    addAggregateDescription(_request, groupStr);

    createEmptyResult(1);
    ASSERT_EQ((size_t)1, _results.size());

    _resultConstructor.fillAggregateResult(_results[0], "sum,count,min,max",
            "key1, 4444, 111, 0, 3333 #key2, 1234, 222, 1, 789",  &_pool,
            groupStr);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)1, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr = mergedResultPtr->getAggregateResult(0);
    ASSERT_TRUE(aggResultPtr.get());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());
    ASSERT_EQ((int64_t)4444, aggResultPtr->getAggFunResult<int64_t>("key1", 0, 0));
    ASSERT_EQ((int64_t)111, aggResultPtr->getAggFunResult<int64_t>("key1", 1, 0));
    ASSERT_EQ((int64_t)0, aggResultPtr->getAggFunResult<int64_t>("key1", 2, 0));
    ASSERT_EQ((int64_t)3333, aggResultPtr->getAggFunResult<int64_t>("key1", 3, 0));
    ASSERT_EQ((int64_t)1234, aggResultPtr->getAggFunResult<int64_t>("key2", 0, 0));
}


TEST_F(Ha3ResultMergeOpTest, testMergeThreeAggregateResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    addAggregateDescription(_request, "groupExprStr");

    createEmptyResult(3);
    ASSERT_EQ((size_t)3, _results.size());

    _resultConstructor.fillAggregateResult(_results[0], "sum,count,min,max",
            "key1, 4444, 111, 0, 3333 #key2, 1234, 222, 1, 789", &_pool);
    _resultConstructor.fillAggregateResult(_results[1], "sum,count,min,max",
            "key1, 4443, 111, 10, 2222 #key3, 1234, 222, 1, 789", &_pool);
    _resultConstructor.fillAggregateResult(_results[2], "sum,count,min,max",
            "key3, 4444, 111, 0, 3333 #key4, 1234, 222, 1, 789", &_pool);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)1, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr = mergedResultPtr->getAggregateResult(0);
    ASSERT_TRUE(aggResultPtr);
    aggResultPtr->constructGroupValueMap();
    ASSERT_EQ((uint32_t)4, aggResultPtr->getAggExprValueCount());

    //key1
    ASSERT_EQ((int64_t)8887, aggResultPtr->getAggFunResult<int64_t>("key1", 0, 0));
    ASSERT_EQ((int64_t)222, aggResultPtr->getAggFunResult<int64_t>("key1", 1, 0));
    ASSERT_EQ((int64_t)0, aggResultPtr->getAggFunResult<int64_t>("key1", 2, 0));
    ASSERT_EQ((int64_t)3333, aggResultPtr->getAggFunResult<int64_t>("key1", 3, 0));

    //key2
    ASSERT_EQ((int64_t)1234, aggResultPtr->getAggFunResult<int64_t>("key2", 0, 0));
    ASSERT_EQ((int64_t)222, aggResultPtr->getAggFunResult<int64_t>("key2", 1, 0));
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("key2", 2, 0));
    ASSERT_EQ((int64_t)789, aggResultPtr->getAggFunResult<int64_t>("key2", 3, 0));

    //key3
    ASSERT_EQ((int64_t)5678, aggResultPtr->getAggFunResult<int64_t>("key3", 0, 0));
    ASSERT_EQ((int64_t)333, aggResultPtr->getAggFunResult<int64_t>("key3", 1, 0));
    ASSERT_EQ((int64_t)0, aggResultPtr->getAggFunResult<int64_t>("key3", 2, 0));
    ASSERT_EQ((int64_t)3333, aggResultPtr->getAggFunResult<int64_t>("key3", 3, 0));

    //key4
    ASSERT_EQ((int64_t)1234, aggResultPtr->getAggFunResult<int64_t>("key4", 0, 0));
    ASSERT_EQ((int64_t)222, aggResultPtr->getAggFunResult<int64_t>("key4", 1, 0));
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("key4", 2, 0));
    ASSERT_EQ((int64_t)789, aggResultPtr->getAggFunResult<int64_t>("key4", 3, 0));
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoResultWithAllFullEmptyAggResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)0, mergedResultPtr->getAggregateResultCount());
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoResultWithMultiAggregator) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupExprStr1 = "fakeGroupExprStr1";
    string groupExprStr2 = "fakeGroupExprStr2";
    string groupExprStr3 = "fakeGroupExprStr3";
    string groupExprStr4 = "fakeGroupExprStr4";
    addAggregateDescription(_request, groupExprStr1);
    addAggregateDescription(_request, groupExprStr2);
    addAggregateDescription(_request, groupExprStr3);
    addAggregateDescription(_request, groupExprStr4);

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    //prepare the first Result's AggregateResult (1 1 0 0)
    _resultConstructor.fillAggregateResult(_results[0], "sum",
            "key11, 11 #key12, 12", &_pool, groupExprStr1);
    _resultConstructor.fillAggregateResult(_results[0], "count",
            "key21, 21 #key22, 22", &_pool, groupExprStr2);
    _resultConstructor.fillAggregateResult(_results[0], "min",
            "", &_pool, groupExprStr3);
    _resultConstructor.fillAggregateResult(_results[0], "max",
            "", &_pool, groupExprStr4);

    //prepare the second Result's AggregateResult (1 0 1 0)
    _resultConstructor.fillAggregateResult(_results[1], "sum",
            "key31, 31 #key32, 32", &_pool, groupExprStr1);
    _resultConstructor.fillAggregateResult(_results[1], "count",
            "", &_pool, groupExprStr2);
    _resultConstructor.fillAggregateResult(_results[1], "min",
            "key41, 41 #key42, 42", &_pool, groupExprStr3);
    _resultConstructor.fillAggregateResult(_results[1], "max",
            "", &_pool, groupExprStr4);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)4, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr1 = mergedResultPtr->getAggregateResult(0);
    AggregateResultPtr aggResultPtr2 = mergedResultPtr->getAggregateResult(1);
    AggregateResultPtr aggResultPtr3 = mergedResultPtr->getAggregateResult(2);
    AggregateResultPtr aggResultPtr4 = mergedResultPtr->getAggregateResult(3);

    ASSERT_EQ(groupExprStr1, aggResultPtr1->getGroupExprStr());
    ASSERT_EQ(groupExprStr2, aggResultPtr2->getGroupExprStr());
    ASSERT_EQ(groupExprStr3, aggResultPtr3->getGroupExprStr());
    ASSERT_EQ(groupExprStr4, aggResultPtr4->getGroupExprStr());

    aggResultPtr1->constructGroupValueMap();
    aggResultPtr2->constructGroupValueMap();
    aggResultPtr3->constructGroupValueMap();
    aggResultPtr4->constructGroupValueMap();

    ASSERT_EQ((uint32_t)4, aggResultPtr1->getAggExprValueCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr2->getAggExprValueCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr3->getAggExprValueCount());
    ASSERT_EQ((uint32_t)0, aggResultPtr4->getAggExprValueCount());

    ASSERT_EQ((int64_t)11, aggResultPtr1->getAggFunResult<int64_t>("key11", 0, 0));
    ASSERT_EQ((int64_t)22, aggResultPtr2->getAggFunResult<int64_t>("key22", 0, 0));
    ASSERT_EQ((int64_t)41, aggResultPtr3->getAggFunResult<int64_t>("key41", 0, 0));
    ASSERT_EQ((int64_t)9999, aggResultPtr4->getAggFunResult<int64_t>("key_xxx", 0, 9999));//9999 is default value
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoResultWithDifferentAggregator) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupExprStr1 = "fakeGroupExprStr1";
    string groupExprStr2 = "fakeGroupExprStr2";
    string groupExprStr3 = "fakeGroupExprStr3";
    string groupExprStr4 = "fakeGroupExprStr4";
    addAggregateDescription(_request, groupExprStr1);
    addAggregateDescription(_request, groupExprStr2);
    addAggregateDescription(_request, groupExprStr3);
    addAggregateDescription(_request, groupExprStr4);

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    //prepare the first Result's AggregateResult (1 1 0 0)
    _resultConstructor.fillAggregateResult(_results[0], "sum",
            "key11, 11 #key12, 12", &_pool, groupExprStr1);
    _resultConstructor.fillAggregateResult(_results[0], "count",
            "key21, 21 #key22, 22", &_pool, groupExprStr2);
    _resultConstructor.fillAggregateResult(_results[0], "min",
            "", &_pool, groupExprStr3);
    _resultConstructor.fillAggregateResult(_results[0], "max",
            "", &_pool, groupExprStr4);

    string suffix = "aaa";
    //prepare the second Result's AggregateResult (1 0 1 0)
    _resultConstructor.fillAggregateResult(_results[1], "sum",
            "key31, 31 #key32, 32", &_pool, groupExprStr1 + suffix);
    _resultConstructor.fillAggregateResult(_results[1], "count",
            "", &_pool, groupExprStr2 + suffix);
    _resultConstructor.fillAggregateResult(_results[1], "min",
            "key41, 41 #key42, 42", &_pool, groupExprStr3 + suffix);
    _resultConstructor.fillAggregateResult(_results[1], "max",
            "", &_pool, groupExprStr4 + suffix);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)4, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr1 = mergedResultPtr->getAggregateResult(0);
    AggregateResultPtr aggResultPtr2 = mergedResultPtr->getAggregateResult(1);
    AggregateResultPtr aggResultPtr3 = mergedResultPtr->getAggregateResult(2);
    AggregateResultPtr aggResultPtr4 = mergedResultPtr->getAggregateResult(3);

    ASSERT_EQ(groupExprStr1, aggResultPtr1->getGroupExprStr());
    ASSERT_EQ(groupExprStr2, aggResultPtr2->getGroupExprStr());
    ASSERT_EQ(groupExprStr3, aggResultPtr3->getGroupExprStr());
    ASSERT_EQ(groupExprStr4, aggResultPtr4->getGroupExprStr());

    aggResultPtr1->constructGroupValueMap();
    aggResultPtr2->constructGroupValueMap();
    aggResultPtr3->constructGroupValueMap();
    aggResultPtr4->constructGroupValueMap();

    ASSERT_EQ((uint32_t)2, aggResultPtr1->getAggExprValueCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr2->getAggExprValueCount());
    ASSERT_EQ((uint32_t)0, aggResultPtr3->getAggExprValueCount());
    ASSERT_EQ((uint32_t)0, aggResultPtr4->getAggExprValueCount());

    ASSERT_EQ((int64_t)11, aggResultPtr1->getAggFunResult<int64_t>("key11", 0, 0));
    ASSERT_EQ((int64_t)22, aggResultPtr2->getAggFunResult<int64_t>("key22", 0, 0));
    ASSERT_EQ((int64_t)0, aggResultPtr3->getAggFunResult<int64_t>("key41", 0, 0));
    ASSERT_EQ((int64_t)9999, aggResultPtr4->getAggFunResult<int64_t>("key_xxx", 0, 9999));//9999 is default value
}

TEST_F(Ha3ResultMergeOpTest, testMergeFirstFullEmptyAggregateResults)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupExprStr = "fakeGroupExprStr";
    addAggregateDescription(_request, groupExprStr);

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillAggregateResult(_results[0],
            "sum", "key1, 4444 #key2, 1234", &_pool, groupExprStr);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)1, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr = mergedResultPtr->getAggregateResult(0);
    ASSERT_EQ(groupExprStr, aggResultPtr->getGroupExprStr());

    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());
    ASSERT_EQ((int64_t)4444, aggResultPtr->getAggFunResult<int64_t>("key1", 0, 0));
    ASSERT_EQ((int64_t)1234, aggResultPtr->getAggFunResult<int64_t>("key2", 0, 0));
}

TEST_F(Ha3ResultMergeOpTest, testMergeResultInSecondPhase)
{
    prepareMergeOp();

    prepareRequest(0, 4, 1);
    ASSERT_TRUE(_request.get());

    ConfigClause *configClause = _request->getConfigClause();
    configClause->setPhaseNumber(SEARCH_PHASE_TWO);

    ResultPtr resultPtr(new Result(new Hits));
    _results.push_back(resultPtr);

    ResultPtr resultPtr2(new Result(new Hits));
    _results.push_back(resultPtr2);

    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillHit(_results[0]->getHits(), 1, 2, "userid, price", "cluster1, 3, uid1, price1 # cluster1, 5, uid2, price2");
    _resultConstructor.fillHit(_results[1]->getHits(), 2, 2, "userid, price", "cluster2, 1, uid3, price3 # cluster2, 7, uid4, price4");

    _resultConstructor.fillPhaseTwoSearchInfo(_results[0], "1");
    _resultConstructor.fillPhaseTwoSearchInfo(_results[1], "11");
    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    Hits *hits = mergedResultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)4, hits->size());
    ASSERT_EQ((docid_t)3, hits->getDocId(0));
    ASSERT_EQ((docid_t)5, hits->getDocId(1));
    ASSERT_EQ((docid_t)1, hits->getDocId(2));
    ASSERT_EQ((docid_t)7, hits->getDocId(3));

    hashid_t hashid = 0;
    ASSERT_TRUE(hits->getHashId(0, hashid));
    ASSERT_EQ((hashid_t)1, hashid);
    ASSERT_TRUE(hits->getHashId(1, hashid));
    ASSERT_EQ((hashid_t)1, hashid);
    ASSERT_TRUE(hits->getHashId(2, hashid));
    ASSERT_EQ((hashid_t)2, hashid);
    ASSERT_TRUE(hits->getHashId(3, hashid));
    ASSERT_EQ((hashid_t)2, hashid);

    ASSERT_TRUE(!mergedResultPtr->hasError());
    ASSERT_EQ(mergedResultPtr->getPhaseTwoSearchInfo()->summaryLatency, 12);
}

TEST_F(Ha3ResultMergeOpTest, testEmptyMatchDocsInFirstResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    //fill the first empty Result
    ResultPtr resultPtr1(new Result(new MatchDocs));
    _results.push_back(resultPtr1);

    ResultPtr resultPtr2(new Result(ERROR_PARTIAL_SEARCHER));
    _results.push_back(resultPtr2);

    //fill another Result
    createEmptyResult(1);
    ASSERT_EQ((size_t)3, _results.size());
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(),
            1, 0, 200, 200, &_pool, "11,22, 10 # 12,22, 20");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)200, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)200, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_PARTIAL_SEARCHER, errors[0].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testEmptyMatchDocsInSecondResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    //fill first Result
    createEmptyResult(1);
    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(),
            1, 0, 200, 200,  &_pool, "11,22, 10 # 12,22, 20");

    //fill the second Result with empty 'MatchDocs'
    ResultPtr resultPtr(new Result(ERROR_PARTIAL_SEARCHER));
    _results.push_back(resultPtr);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)200, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());

    ASSERT_EQ(ERROR_PARTIAL_SEARCHER, errors[0].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testEmptyMatchDocsInAllResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    //fill all the empty Result
    ResultPtr resultPtr1(new Result(new MatchDocs));
    _results.push_back(resultPtr1);
    ResultPtr resultPtr2(new Result(new MatchDocs));
    _results.push_back(resultPtr2);

    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_TRUE(!mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testNoneMatchDocsObjectInAllResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    //fill all the empty Result
    ResultPtr resultPtr1(new Result(ERROR_PARTIAL_SEARCHER));
    _results.push_back(resultPtr1);
    ResultPtr resultPtr2(new Result(ERROR_PARTIAL_SEARCHER));
    _results.push_back(resultPtr2);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());
}

TEST_F(Ha3ResultMergeOpTest, testEmptyAggResultInFirstResult) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupExprStr = "fakeGroupExprStr";
    addAggregateDescription(_request, groupExprStr);

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillAggregateResult(_results[1], "sum,count,min,max",
            "key1, 4443, 111, 10, 2222 #key2, 1234, 222, 1, 789", &_pool, groupExprStr);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_EQ((uint32_t)1, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr = mergedResultPtr->getAggregateResult(0);
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    //key1
    ASSERT_EQ((int64_t)4443, aggResultPtr->getAggFunResult<int64_t>("key1", 0, 0));
    ASSERT_EQ((int64_t)111, aggResultPtr->getAggFunResult<int64_t>("key1", 1, 0));
    ASSERT_EQ((int64_t)10, aggResultPtr->getAggFunResult<int64_t>("key1", 2, 0));
    ASSERT_EQ((int64_t)2222, aggResultPtr->getAggFunResult<int64_t>("key1", 3, 0));

    //key2
    ASSERT_EQ((int64_t)1234, aggResultPtr->getAggFunResult<int64_t>("key2", 0, 0));
    ASSERT_EQ((int64_t)222, aggResultPtr->getAggFunResult<int64_t>("key2", 1, 0));
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("key2", 2, 0));
    ASSERT_EQ((int64_t)789, aggResultPtr->getAggFunResult<int64_t>("key2", 3, 0));
}

TEST_F(Ha3ResultMergeOpTest, testMergeEmptyHits) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());

    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);

    ResultPtr resultPtr1(new Result(new Hits()));
    resultPtr1->getMultiErrorResult()->addError(1001);
    ResultPtr resultPtr2(new Result(new Hits()));
    resultPtr2->getMultiErrorResult()->addError(1002);
    _results.push_back(resultPtr1);
    _results.push_back(resultPtr2);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    Hits *mergedHits = mergedResultPtr->getHits();
    ASSERT_TRUE(mergedHits);
    ASSERT_EQ((uint32_t)0, mergedHits->size());
    ASSERT_TRUE(mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergeNullHits) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);

    ResultPtr resultPtr1(new Result());
    resultPtr1->getMultiErrorResult()->addError(1001);
    ResultPtr resultPtr2(new Result());
    resultPtr2->getMultiErrorResult()->addError(1002);
    _results.push_back(resultPtr1);
    _results.push_back(resultPtr2);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    Hits *mergedHits = mergedResultPtr->getHits();
    ASSERT_TRUE(!mergedHits);
    ASSERT_TRUE(mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergeBeginWithNullHits) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);

    ResultPtr resultPtr1(new Result());
    resultPtr1->getMultiErrorResult()->addError(1001);
    ResultPtr resultPtr2(new Result(new Hits()));
    resultPtr2->getMultiErrorResult()->addError(1002);
    _results.push_back(resultPtr1);
    _results.push_back(resultPtr2);
    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    Hits *mergedHits = mergedResultPtr->getHits();
    ASSERT_TRUE(mergedHits);
    ASSERT_EQ(0u, mergedHits->size());
    ASSERT_TRUE(mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testSerializeAndMerge) {
    prepareMergeOp();

    prepareRequest(0, 3, 1);
    ASSERT_TRUE(_request.get());

    ResultPtr serializedResultPtr1 = createSerializedResult("2, 22, 222 # 3, 33, 333");
    ResultPtr serializedResultPtr2 = createSerializedResult("4, 44, 444 # 5, 55, 555");
    _results.push_back(serializedResultPtr1);
    _results.push_back(serializedResultPtr2);

    ASSERT_EQ((size_t)2, _results.size());

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)20, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)20, mergedMatchDocs->actualMatchDocs());
    ASSERT_EQ((uint32_t)4, mergedMatchDocs->size());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getDocId(0));
    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)2, errors.size());
    ASSERT_EQ(ERROR_UNKNOWN, errors[0].getErrorCode());
    ASSERT_EQ(ERROR_UNKNOWN, errors[1].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testMergeWithPrimaryKeyNoDedup) {
    prepareMergeOp();

    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(3);
    ASSERT_EQ((size_t)3, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7", false, "1,2,3,4,5");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 87,8 # 14,22, 87,9 # 15,22,88,10",
            false, "1,6,7,8,5");
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "21,33, 88,3 # 22,33, 77,4 # 23,33, 87,8 # 24,33, 87,9 # 25,33,88,10", false, "11,12,13,14,15");

    _request->getConfigClause()->setDoDedup(false);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)500, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)500, mergedMatchDocs->actualMatchDocs());
    ASSERT_TRUE(mergedMatchDocs->hasPrimaryKey());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)11, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)15, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_EQ((docid_t)21, mergedMatchDocs->getMatchDoc(10).getDocId());
    ASSERT_EQ((docid_t)22, mergedMatchDocs->getMatchDoc(11).getDocId());
    ASSERT_EQ((docid_t)23, mergedMatchDocs->getMatchDoc(12).getDocId());
    ASSERT_EQ((docid_t)24, mergedMatchDocs->getMatchDoc(13).getDocId());
    ASSERT_EQ((docid_t)25, mergedMatchDocs->getMatchDoc(14).getDocId());
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(15));
    ASSERT_TRUE(!mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergeWithPrimaryKey) {
    prepareMergeOp();

    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(3);
    ASSERT_EQ((size_t)3, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7", false, "1,2,3,4,5");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 87,8 # 14,22, 87,9 # 15,22,88,10",
            false, "1,6,7,8,5");
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "21,33, 99,3 # 22,33, 77,4 # 23,33, 87,8 # 24,33, 87,9 # 25,33,88,10", false, "10,11,12,13,14");

    _request->getConfigClause()->setDoDedup(true);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)500, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)500, mergedMatchDocs->actualMatchDocs());
    ASSERT_TRUE(mergedMatchDocs->hasPrimaryKey());
    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)13, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)14, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ((docid_t)21, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ((docid_t)22, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_EQ((docid_t)23, mergedMatchDocs->getMatchDoc(10).getDocId());
    ASSERT_EQ((docid_t)24, mergedMatchDocs->getMatchDoc(11).getDocId());
    ASSERT_EQ((docid_t)25, mergedMatchDocs->getMatchDoc(12).getDocId());

    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(13));
    ASSERT_TRUE(!mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergePhaseOneSearchInfos) {
    prepareMergeOp();

    createEmptyResult(2);
    _resultConstructor.fillPhaseOneSearchInfo(_results[0], "1,2,3,4,5,6,7,8,9,10");
    _resultConstructor.fillPhaseOneSearchInfo(_results[1], "11,12,13,14,15,16,17,18,19,20");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergePhaseOneSearchInfos(mergedResultPtr.get(), _results);
    PhaseOneSearchInfo expectedSearchInfo(12,14,16,18,20,22,24,26,28,30);
    ASSERT_TRUE(*(mergedResultPtr->getPhaseOneSearchInfo()) == expectedSearchInfo);
}

TEST_F(Ha3ResultMergeOpTest, testMergePhaseTwoSearchInfos) {
    prepareMergeOp();

    createEmptyResult(2);
    _resultConstructor.fillPhaseTwoSearchInfo(_results[0], "1");
    _resultConstructor.fillPhaseTwoSearchInfo(_results[1], "11");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergePhaseTwoSearchInfos(mergedResultPtr.get(), _results);
    PhaseTwoSearchInfo expectedSearchInfo;
    ASSERT_EQ(mergedResultPtr->getPhaseTwoSearchInfo()->summaryLatency, 12);
}

TEST_F(Ha3ResultMergeOpTest, testMergeAggregateResultWithOverflowAggExprValue) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    string groupExprStr = "fakeGroupExprStr";

    AggregateClause *aggClause = _request->getAggregateClause();
    if (NULL == aggClause) {
        aggClause = new AggregateClause();
        _request->setAggregateClause(aggClause);
        aggClause = _request->getAggregateClause();
    }

    AggregateDescription *des = new AggregateDescription(groupExprStr);
    SyntaxExpr *syntax = new AtomicSyntaxExpr(
            groupExprStr, vt_int32, ::ATTRIBUTE_NAME);
    des->setGroupKeyExpr(syntax);
    aggClause->addAggDescription(des);
    des->appendAggFunDescription(new AggFunDescription("count", NULL));
    des->appendAggFunDescription(new AggFunDescription("sum", NULL));

    _request->getAggregateClause()->getAggDescriptions()[0]->setMaxGroupCount(2);

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    //fill aggResult into Result
    _resultConstructor.fillAggregateResult(_results[0], "count, sum",
            "key1, 100, 1 #key2, 123, 2", &_pool, groupExprStr);
    _resultConstructor.fillAggregateResult(_results[1], "count, sum",
            "key1, 36, 1 #key4, 1234, 3", &_pool, groupExprStr);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    //the AggregateResult count must equal 1
    ASSERT_EQ((uint32_t)1, mergedResultPtr->getAggregateResultCount());
    AggregateResultPtr aggResultPtr = mergedResultPtr->getAggregateResult(0);
    ASSERT_TRUE(aggResultPtr.get());
    aggResultPtr->constructGroupValueMap();
    ASSERT_EQ(groupExprStr, aggResultPtr->getGroupExprStr());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    AggregateResult::AggExprValueMap& aggExprValMap =
        aggResultPtr->getAggExprValueMap();
    AggregateResult::AggExprValueMap::iterator it = aggExprValMap.find("key1");
    ASSERT_TRUE(it != aggExprValMap.end());

    it = aggExprValMap.find("key4");
    ASSERT_TRUE(it != aggExprValMap.end());

    it = aggExprValMap.find("key2");
    ASSERT_TRUE(it == aggExprValMap.end());
}

TEST_F(Ha3ResultMergeOpTest, testMergeNoErrors) {
    prepareMergeOp();
    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    MultiErrorResultPtr errorResult1(new MultiErrorResult());
    MultiErrorResultPtr errorResult2(new MultiErrorResult());
    ErrorResult errorResult3;
    ErrorResult errorResult4;
    ErrorResult errorResult5;
    errorResult1->addErrorResult(errorResult3);
    errorResult1->addErrorResult(errorResult4);
    errorResult1->addErrorResult(errorResult5);
    errorResult2->addErrorResult(errorResult4);

    createEmptyResult(2);
    _results[0]->addErrorResult(errorResult1);
    _results[1]->addErrorResult(errorResult2);

    ASSERT_EQ((size_t)2, _results.size());
    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 0, 0,  &_pool, "");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 3, 3, &_pool,
            "11,22,77,3 # 13,22,66,4 # 15,22,44,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_TRUE(!mergedResultPtr->hasError());
    string expectedString = "<Error>\n\t<ErrorCode>0</ErrorCode>\n\t<ErrorDescription></ErrorDescription>\n</Error>\n";
    ASSERT_EQ(expectedString, mergedResultPtr->getMultiErrorResult()->toXMLString());
}

TEST_F(Ha3ResultMergeOpTest, testMergeErrors) {
    prepareMergeOp();
    ErrorResult errorResult1(ERROR_UNKNOWN);
    ErrorResult errorResult2;

    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _results[0]->addErrorResult(errorResult1);
    _results[1]->addErrorResult(errorResult2);
    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 0, 0,  &_pool, "");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 3, 3, &_pool,
            "11,22,77,3 # 13,22,66,4 # 15,22,44,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_UNKNOWN, errors[0].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testMergeFuncWithCoveredRanges) {
    prepareMergeOp();

    vector<ResultPtr> results;
    prepareRequest(0, 5, 1);
    ASSERT_TRUE(_request.get());

    ResultPtr result1(new Result);
    ResultPtr result2(new Result);
    results.push_back(result1);
    results.push_back(result2);
    results[0]->addCoveredRange("cluster1", 6, 7);
    results[0]->addCoveredRange("cluster1", 0, 1);
    results[1]->addCoveredRange("cluster1", 2, 3);

    results[1]->addCoveredRange("cluster2", 0, 7);
    results[1]->addCoveredRange("cluster2", 4, 8);
    results[1]->addCoveredRange("cluster3", 0, 1);

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), results, _request.get());

    Result::ClusterPartitionRanges mergedRanges = mergedResultPtr->getCoveredRanges();
    ASSERT_EQ(size_t(3), mergedRanges.size());
    ASSERT_TRUE(mergedRanges.find("cluster1") != mergedRanges.end());
    ASSERT_TRUE(mergedRanges.find("cluster2") != mergedRanges.end());
    ASSERT_TRUE(mergedRanges.find("cluster3") != mergedRanges.end());

    ASSERT_EQ((size_t)2, mergedRanges["cluster1"].size());
    ASSERT_EQ((uint32_t)0, mergedRanges["cluster1"][0].first);
    ASSERT_EQ((uint32_t)3, mergedRanges["cluster1"][0].second);
    ASSERT_EQ((uint32_t)6, mergedRanges["cluster1"][1].first);
    ASSERT_EQ((uint32_t)7, mergedRanges["cluster1"][1].second);

    ASSERT_EQ((size_t)1, mergedRanges["cluster2"].size());
    ASSERT_EQ((uint32_t)0, mergedRanges["cluster2"][0].first);
    ASSERT_EQ((uint32_t)8, mergedRanges["cluster2"][0].second);

    ASSERT_EQ((size_t)1, mergedRanges["cluster3"].size());
    ASSERT_EQ((uint32_t)0, mergedRanges["cluster3"][0].first);
    ASSERT_EQ((uint32_t)1, mergedRanges["cluster3"][0].second);
}

TEST_F(Ha3ResultMergeOpTest, testMergeErrorsWithSelfError) {
    prepareMergeOp();
    ErrorResult errorResult1(ERROR_UNKNOWN);
    ErrorResult errorResult2;;

    prepareRequest(0, 5, 1);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _results[0]->addErrorResult(errorResult1);
    _results[1]->addErrorResult(errorResult2);

    ResultPtr mergedResultPtr(new Result(ERROR_OVER_RETURN_HITS_LIMIT));
    _mergeOp->mergeErrors(mergedResultPtr.get(), _results);

    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)2, errors.size());
    ASSERT_EQ(ERROR_OVER_RETURN_HITS_LIMIT, errors[0].getErrorCode());
    ASSERT_EQ(ERROR_UNKNOWN, errors[1].getErrorCode());
}


TEST_F(Ha3ResultMergeOpTest, testMergeMultiErrors) {
    prepareMergeOp();
    MultiErrorResultPtr errorResult1(new MultiErrorResult());
    MultiErrorResultPtr errorResult2(new MultiErrorResult());
    ErrorResult errorResult3(ERROR_AGG_RANGE_ORDER);
    ErrorResult errorResult4(ERROR_INDEX_NOT_EXIST);
    ErrorResult errorResult5(ERROR_NO_CONFIG_CLAUSE);
    errorResult1->addErrorResult(errorResult3);
    errorResult1->addErrorResult(errorResult4);
    errorResult1->addErrorResult(errorResult5);

    ASSERT_TRUE(errorResult1->hasError());

    ASSERT_TRUE(!errorResult2->hasError());

    prepareRequest(0, 5, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _results[0]->addErrorResult(errorResult1);
    _results[1]->addErrorResult(errorResult2);

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 0, 0,  &_pool, "");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 3, 3, &_pool,
            "11,22,77,3 # 13,22,66,4 # 15,22,44,10");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ASSERT_TRUE(mergedResultPtr->hasError());
    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)3, errors.size());
    ASSERT_EQ(ERROR_AGG_RANGE_ORDER, errors[0].getErrorCode());
    ASSERT_EQ(ERROR_INDEX_NOT_EXIST, errors[1].getErrorCode());
    ASSERT_EQ(ERROR_NO_CONFIG_CLAUSE, errors[2].getErrorCode());
}

TEST_F(Ha3ResultMergeOpTest, testMergeNullTracePhaseOne) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_ONE);
    mergeNullTrace();
}
TEST_F(Ha3ResultMergeOpTest, testMergeNullTracePhaseTwo) {
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);
    mergeNullTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeOneTracePhaseOne)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_ONE);
    mergeOneTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeOneTracePhaseTwo)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);
    mergeOneTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoTracePhaseOne)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_ONE);
    mergeTwoTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeTwoTracePhaseTwo)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);
    mergeTwoTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeSecondNullPhaseOne)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_ONE);
    mergeSecondNullTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeSecondNullPhaseTwo)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);
    mergeSecondNullTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeFirstNullPhaseOne)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_ONE);
    mergeFirstNullTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeFirstNullPhaseTwo)
{
    prepareMergeOp();

    prepareRequest(0, 10, 1);
    ASSERT_TRUE(_request.get());
    _request->getConfigClause()->setPhaseNumber(SEARCH_PHASE_TWO);
    mergeFirstNullTrace();
}

TEST_F(Ha3ResultMergeOpTest, testMergeWithPrimaryKeyAndDifferentVersion) {
    prepareMergeOp();
    prepareRequest(0, 50, 2);
    ASSERT_TRUE(_request.get());

    createEmptyResult(6);
    ASSERT_EQ((size_t)6, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "31,33, 99,3 # 32,33, 77,4 # 33,33, 87,8 # 34,33, 87,9 # 35,6,88,10", false, "10,11,12,13,14");
    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 2, 0, 100, 100, &_pool,
            "1,22, 77,1 # 2,22, 77,2 # 3,22, 88,5 # 4,22, 88,6 # 5,22,88,7", false, "1,2,3,4,5", 1);
    _resultConstructor.fillMatchDocs(_results[2]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "11,22, 77,3 # 12,22, 77,4 # 13,22, 87,8 # 14,22, 87,9 # 15,22,88,10",
            false, "1,6,7,8,5", 0);
    _resultConstructor.fillMatchDocs(_results[3]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "41,33, 99,3 # 42,33, 77,4 # 43,33, 87,8 # 44,33, 87,9 # 45,6,88,10",false, "20,21,22,23,24");
    _resultConstructor.fillMatchDocs(_results[4]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "21,33, 99,3 # 22,33, 77,4 # 23,33, 87,8 # 24,33, 87,9 # 25,6,88,10",false, "1,9,7,8,5", 1);
    _resultConstructor.fillMatchDocs(_results[5]->getMatchDocs(), 2, 0, 200, 200, &_pool,
            "51,33, 99,3 # 52,33, 77,4 # 53,33, 87,8 # 54,33, 87,9 # 55,6,88,10",false, "30,31,32,33,34");

    // test keep prior cluster matchdoc
    auto clusterIdRef = _results[2]->getMatchDocs()->getClusterIdRef();
    clusterIdRef->set(_results[2]->getMatchDocs()->getMatchDoc(4), 1);
    matchdoc::Reference<FullIndexVersion> *fullVersionRef =
        _results[2]->getMatchDocs()->getFullIndexVersionRef();
    fullVersionRef->getReference(_results[2]->getMatchDocs()->getMatchDoc(4)) = 10;

    _request->getConfigClause()->setDoDedup(true);
    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    ASSERT_TRUE(mergedMatchDocs);
    fullVersionRef = mergedMatchDocs->getFullIndexVersionRef();

    ASSERT_EQ((uint32_t)1100, mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)1100, mergedMatchDocs->actualMatchDocs());

    ASSERT_EQ((docid_t)1, mergedMatchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)2, mergedMatchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)3, mergedMatchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)4, mergedMatchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)5, mergedMatchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ(FullIndexVersion(1),
              fullVersionRef->getReference(mergedMatchDocs->getMatchDoc(4)));

    ASSERT_EQ((docid_t)12, mergedMatchDocs->getMatchDoc(5).getDocId());
    ASSERT_EQ((docid_t)23, mergedMatchDocs->getMatchDoc(6).getDocId());
    ASSERT_EQ((docid_t)24, mergedMatchDocs->getMatchDoc(7).getDocId());
    ASSERT_EQ(FullIndexVersion(0),
              fullVersionRef->getReference(mergedMatchDocs->getMatchDoc(5)));
    ASSERT_EQ(FullIndexVersion(1),
              fullVersionRef->getReference(mergedMatchDocs->getMatchDoc(7)));

    ASSERT_EQ((docid_t)22, mergedMatchDocs->getMatchDoc(8).getDocId());
    ASSERT_EQ(FullIndexVersion(1),
              fullVersionRef->getReference(mergedMatchDocs->getMatchDoc(8)));

    ASSERT_EQ((docid_t)31, mergedMatchDocs->getMatchDoc(9).getDocId());
    ASSERT_EQ((docid_t)32, mergedMatchDocs->getMatchDoc(10).getDocId());
    ASSERT_EQ((docid_t)33, mergedMatchDocs->getMatchDoc(11).getDocId());
    ASSERT_EQ((docid_t)34, mergedMatchDocs->getMatchDoc(12).getDocId());
    ASSERT_EQ((docid_t)35, mergedMatchDocs->getMatchDoc(13).getDocId());

    ASSERT_EQ((docid_t)41, mergedMatchDocs->getMatchDoc(14).getDocId());
    ASSERT_EQ((docid_t)42, mergedMatchDocs->getMatchDoc(15).getDocId());
    ASSERT_EQ((docid_t)43, mergedMatchDocs->getMatchDoc(16).getDocId());
    ASSERT_EQ((docid_t)44, mergedMatchDocs->getMatchDoc(17).getDocId());
    ASSERT_EQ((docid_t)45, mergedMatchDocs->getMatchDoc(18).getDocId());

    ASSERT_EQ((docid_t)51, mergedMatchDocs->getMatchDoc(19).getDocId());
    ASSERT_EQ((docid_t)52, mergedMatchDocs->getMatchDoc(20).getDocId());
    ASSERT_EQ((docid_t)53, mergedMatchDocs->getMatchDoc(21).getDocId());
    ASSERT_EQ((docid_t)54, mergedMatchDocs->getMatchDoc(22).getDocId());
    ASSERT_EQ((docid_t)55, mergedMatchDocs->getMatchDoc(23).getDocId());

    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == mergedMatchDocs->getMatchDoc(24));
    ASSERT_TRUE(!mergedResultPtr->hasError());
}

TEST_F(Ha3ResultMergeOpTest, testMergeEmptyCoveredRanges) {
    prepareMergeOp();

    ResultPtr mergedResultPtr(new Result);
    vector<ResultPtr> results;

    _mergeOp->mergeCoveredRanges(mergedResultPtr.get(), results);
    ASSERT_EQ((size_t)0, mergedResultPtr->getCoveredRanges().size());
    ResultPtr result1(new Result);
    ResultPtr result2(new Result);

    results.push_back(result1);
    results.push_back(result2);
    _mergeOp->mergeCoveredRanges(mergedResultPtr.get(), results);
    Result::ClusterPartitionRanges mergedRanges =
        mergedResultPtr->getCoveredRanges();
    ASSERT_EQ((size_t)0, mergedRanges.size());
}

TEST_F(Ha3ResultMergeOpTest, testMergeUnconnectedCoveredRanges) {
    prepareMergeOp();

    ResultPtr mergedResultPtr(new Result);
    vector<ResultPtr> results;

    ResultPtr result1(new Result);
    ResultPtr result2(new Result);

    results.push_back(result1);
    results.push_back(result2);

    results[0]->addCoveredRange("cluster1", 0, 1);
    results[0]->addCoveredRange("cluster1", 6, 7);
    results[1]->addCoveredRange("cluster1", 3, 4);

    _mergeOp->mergeCoveredRanges(mergedResultPtr.get(), results);
    Result::ClusterPartitionRanges mergedRanges = mergedResultPtr->getCoveredRanges();
    ASSERT_EQ((size_t)1, mergedRanges.size());
    ASSERT_EQ((uint32_t)0, mergedRanges["cluster1"][0].first);
    ASSERT_EQ((uint32_t)1, mergedRanges["cluster1"][0].second);
    ASSERT_EQ((uint32_t)3, mergedRanges["cluster1"][1].first);
    ASSERT_EQ((uint32_t)4, mergedRanges["cluster1"][1].second);
    ASSERT_EQ((uint32_t)6, mergedRanges["cluster1"][2].first);
    ASSERT_EQ((uint32_t)7, mergedRanges["cluster1"][2].second);
}

TEST_F(Ha3ResultMergeOpTest, testMergeCoveredRanges) {
    prepareMergeOp();

    ResultPtr mergedResultPtr(new Result);
    vector<ResultPtr> results;

    ResultPtr result1(new Result);
    ResultPtr result2(new Result);
    ResultPtr result3(new Result);
    ResultPtr result4(new Result);

    results.push_back(result1);
    results.push_back(result2);
    results.push_back(result3);
    results.push_back(result4);

    results[0]->addCoveredRange("cluster1", 1, 2);
    results[0]->addCoveredRange("cluster1", 2, 3);
    results[0]->addCoveredRange("cluster1", 2, 5);

    results[1]->addCoveredRange("cluster1", 70, 90);
    results[1]->addCoveredRange("cluster1", 40, 100);
    results[1]->addCoveredRange("cluster1", 60, 80);

    results[2]->addCoveredRange("cluster1", 110, 120);
    results[2]->addCoveredRange("cluster1", 121, 140);

    results[3]->addCoveredRange("cluster1", 150, 150);
    results[3]->addCoveredRange("cluster1", 150, 160);
    results[3]->addCoveredRange("cluster1", 150, 180);

    _mergeOp->mergeCoveredRanges(mergedResultPtr.get(), results);

    Result::ClusterPartitionRanges mergedRanges = mergedResultPtr->getCoveredRanges();
    ASSERT_EQ((size_t)1, mergedRanges.size());
    ASSERT_EQ((size_t)4, mergedRanges["cluster1"].size());
    ASSERT_EQ((uint32_t)1, mergedRanges["cluster1"][0].first);
    ASSERT_EQ((uint32_t)5, mergedRanges["cluster1"][0].second);
    ASSERT_EQ((uint32_t)40, mergedRanges["cluster1"][1].first);
    ASSERT_EQ((uint32_t)100, mergedRanges["cluster1"][1].second);
    ASSERT_EQ((uint32_t)110, mergedRanges["cluster1"][2].first);
    ASSERT_EQ((uint32_t)140, mergedRanges["cluster1"][2].second);
    ASSERT_EQ((uint32_t)150, mergedRanges["cluster1"][3].first);
    ASSERT_EQ((uint32_t)180, mergedRanges["cluster1"][3].second);
}

TEST_F(Ha3ResultMergeOpTest, testMergeSorterBeginRequestFailed) {
    prepareMergeOp();

    uint32_t sortReferCount = 2;
    prepareRequest(0, 10, sortReferCount);
    ASSERT_TRUE(_request.get());

    createEmptyResult(2);
    ASSERT_EQ((size_t)2, _results.size());

    _resultConstructor.fillMatchDocs(_results[0]->getMatchDocs(), 1, 0, 100, 100, &_pool,
            "7,1,1 # 8,1,1 # 9,1,1");

    _resultConstructor.fillMatchDocs(_results[1]->getMatchDocs(), 1, 0, 200, 200, &_pool,
            "10,1,2 # 11,1,1 # 12,1,1");

    ResultPtr mergedResultPtr(new Result);
    _mergeOp->mergeResults(mergedResultPtr.get(), _results, _request.get());

    ErrorResults errors = mergedResultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)0, errors.size());
}

END_HA3_NAMESPACE();
