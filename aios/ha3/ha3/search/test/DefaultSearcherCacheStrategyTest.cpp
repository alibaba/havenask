#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/DefaultSearcherCacheStrategy.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <suez/turing/expression/function/FunctionManager.h>
#include <ha3/common/Request.h>
#include <ha3/common/SearcherCacheClause.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <ha3/search/CacheResult.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/func_expression/FunctionProvider.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(func_expression);
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
class FakeExpireTimeFunction;
class FakeFunctionCreator;
HA3_TYPEDEF_PTR(FakeFunctionCreator);

class DefaultSearcherCacheStrategyTest : public TESTBASE {
public:
    DefaultSearcherCacheStrategyTest();
    ~DefaultSearcherCacheStrategyTest();
public:
    void setUp();
    void tearDown();

protected:
    std::string makeAttrValues(const std::vector<uint32_t> &values);
    IndexPartitionReaderWrapperPtr makeFakeIndexPartReader();
    common::MatchDocs* createMatchDocs(int32_t docNum);
protected:
    IndexPartitionReaderWrapperPtr _fakeIndexPartReader;
    AttributeExpressionCreator *_attributeExprCreator = nullptr;
    FakeExpireTimeFunction *_fakeExpireTimeFuncion = nullptr;
    FakeFunctionCreatorPtr _fakeFunctionCreatorPtr;
    func_expression::FunctionProvider *_functionProvider = nullptr;
    std::vector<uint32_t> _values;

    common::Ha3MatchDocAllocatorPtr _matchDocAllocatorPtr;
    std::vector<matchdoc::MatchDoc> _matchDocsVect;
    autil::mem_pool::Pool *_pool = nullptr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, DefaultSearcherCacheStrategyTest);


class FakeExpireTimeFunction : public FunctionInterfaceTyped<uint32_t> {
public:
    FakeExpireTimeFunction() { _hasBeginRequest = false; }
    ~FakeExpireTimeFunction() { }
public:
    bool beginRequest(suez::turing::FunctionProvider* dataProvider) {
        _hasBeginRequest = true;
        return true;
    }
    void endRequest() { }
    uint32_t evaluate(matchdoc::MatchDoc matchDoc) {
        assert(_hasBeginRequest);
        docid_t docId = matchDoc.getDocId();
        if (docId < 0 || docId > (docid_t)_values.size()) {
            return (uint32_t)-1;
        }
        return _values[docId];
    }
public:
    void setValues(const vector<uint32_t> &values) {
        _values = values;
    }
private:
    vector<uint32_t> _values;
    bool _hasBeginRequest;
};

class FakeFunctionCreator : public FunctionInterfaceCreator {
public:
    FakeFunctionCreator() {
        _fakeExpireTimeFunc = NULL;
    }
    ~FakeFunctionCreator() {
        delete _fakeExpireTimeFunc;
    }

    FunctionInterface *createFunction(
            const FunctionSubExprVec& funcSubExprVec)
    {
        return _fakeExpireTimeFunc;
    }
public:
    void setFakeExpireTimeFunction(FakeExpireTimeFunction *func) {
        _fakeExpireTimeFunc = func;
    }
private:
    FakeExpireTimeFunction *_fakeExpireTimeFunc;
};

DefaultSearcherCacheStrategyTest::DefaultSearcherCacheStrategyTest() {
    _attributeExprCreator = NULL;
    uint32_t values[] = {3, 1, 2};
    _values = vector<uint32_t>(values,
                               values + sizeof(values) / sizeof(uint32_t));
    _pool = new autil::mem_pool::Pool(1024);
}

DefaultSearcherCacheStrategyTest::~DefaultSearcherCacheStrategyTest() {
    _matchDocAllocatorPtr.reset();
    delete _pool;
}

void DefaultSearcherCacheStrategyTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _matchDocAllocatorPtr.reset(new common::Ha3MatchDocAllocator(_pool));
    _fakeIndexPartReader = makeFakeIndexPartReader();
    _fakeExpireTimeFuncion = new FakeExpireTimeFunction;
    _fakeFunctionCreatorPtr = FakeFunctionCreatorPtr(
            new FakeFunctionCreator());
    _fakeFunctionCreatorPtr->setFakeExpireTimeFunction(
            _fakeExpireTimeFuncion);
    _functionProvider = new func_expression::FunctionProvider(FunctionResource());
    _attributeExprCreator = POOL_NEW_CLASS(_pool, FakeAttributeExpressionCreator,
            _pool,
            _fakeIndexPartReader,
            _fakeFunctionCreatorPtr.get(),
            _functionProvider, NULL, NULL, NULL);
}

void DefaultSearcherCacheStrategyTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    if (_attributeExprCreator) {
        POOL_DELETE_CLASS(_attributeExprCreator);
    }
    DELETE_AND_SET_NULL(_functionProvider);
    for (size_t i = 0; i < _matchDocsVect.size(); i++) {
        _matchDocAllocatorPtr->deallocate(_matchDocsVect[i]);
    }
}

string DefaultSearcherCacheStrategyTest::makeAttrValues(
        const vector<uint32_t> &values)
{
    stringstream ss;
    for (size_t i = 0; i < values.size(); i++) {
        ss << values[i];
        if (i != values.size() - 1) {
            ss << ",";
        }
    }
    return ss.str();
}

IndexPartitionReaderWrapperPtr
DefaultSearcherCacheStrategyTest::makeFakeIndexPartReader() {
    FakeIndex fakeIndex;
    fakeIndex.versionId = 1;
    fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    fakeIndex.attributes = "attr_expire_time : uint32_t : " + makeAttrValues(_values) + ";"
                           "attr_1 : uint32_t : " + makeAttrValues(_values) + ";"
                           "attr_2 : uint32_t : " + makeAttrValues(_values) + ";";
    return FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
}

MatchDocs* DefaultSearcherCacheStrategyTest::createMatchDocs(int32_t docNum) {
    MatchDocs *matchDocs = new MatchDocs;
    for (int32_t i = 0; i < docNum; i++) {
        matchdoc::MatchDoc matchDoc = _matchDocAllocatorPtr->allocate(i);
        _matchDocsVect.push_back(matchDoc);
        matchDocs->addMatchDoc(matchDoc);
    }
    return matchDocs;
}

TEST_F(DefaultSearcherCacheStrategyTest, testGenKey) {
    HA3_LOG(DEBUG, "Begin Test!");

    DefaultSearcherCacheStrategy strategy(_pool);
    uint64_t key = 0L;
    Request request;
    ASSERT_TRUE(!strategy.genKey(&request, key));

    SearcherCacheClause* cacheClause = new SearcherCacheClause;
    request.setSearcherCacheClause(cacheClause);

    ASSERT_TRUE(!strategy.genKey(&request, key));

    cacheClause->setUse(true);
    ASSERT_TRUE(strategy.genKey(&request, key));
    ASSERT_EQ((uint64_t)0L, key);

    cacheClause->setKey(12345);
    ASSERT_TRUE(strategy.genKey(&request, key));
    ASSERT_EQ((uint64_t)12345, key);
}

TEST_F(DefaultSearcherCacheStrategyTest, testGetCacheDocNum){
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    Request request;
    SearcherCacheClause* cacheClause = new SearcherCacheClause;
    request.setSearcherCacheClause(cacheClause);
    vector<uint32_t> cacheDocNumVec;
    cacheDocNumVec.push_back(100);
    cacheDocNumVec.push_back(300);
    cacheDocNumVec.push_back(500);
    cacheClause->setCacheDocNumVec(cacheDocNumVec);

    ASSERT_EQ((uint32_t)100,
                         strategy.getCacheDocNum(&request, 5));
    ASSERT_EQ((uint32_t)100,
                         strategy.getCacheDocNum(&request, 100));
    ASSERT_EQ((uint32_t)300,
                         strategy.getCacheDocNum(&request, 150));
    ASSERT_EQ((uint32_t)300,
                         strategy.getCacheDocNum(&request, 300));
    ASSERT_EQ((uint32_t)500,
                         strategy.getCacheDocNum(&request, 500));
    ASSERT_EQ((uint32_t)500,
                         strategy.getCacheDocNum(&request, 5000));
}

TEST_F(DefaultSearcherCacheStrategyTest, testCalcAndFillExpireTimeFail) {
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();
    CacheResult cacheResult;

    uint32_t defaultExpireTime = 100;
    bool ret = strategy.calcAndfillExpireTime(&cacheResult, defaultExpireTime);
    ASSERT_TRUE(ret);

    Request request;
    MultiErrorResult errorResult;
    SearcherCacheClause *cacheClause = new SearcherCacheClause;
    SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
            "attr_expire_time", vt_uint32, ATTRIBUTE_NAME);
    cacheClause->setExpireExpr(expireSyntaxExpr);
    request.setSearcherCacheClause(cacheClause);
    ASSERT_TRUE(strategy.init(&request, &errorResult));

    Result *result1 = new Result;
    MatchDocs *matchDocs1 = new MatchDocs;
    matchDocs1->addMatchDoc(matchdoc::INVALID_MATCHDOC);
    result1->setMatchDocs(matchDocs1);
    cacheResult.setResult(result1);
    ret = strategy.calcAndfillExpireTime(&cacheResult, defaultExpireTime);
    ASSERT_TRUE(!ret);
}

TEST_F(DefaultSearcherCacheStrategyTest, testCalcAndFillExpireTime) {
    HA3_LOG(DEBUG, "Begin Test!");
    Request request;
    MultiErrorResult errorResult;
    SearcherCacheClause *cacheClause = new SearcherCacheClause;
    SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
            "attr_expire_time", vt_uint32, ATTRIBUTE_NAME);
    cacheClause->setExpireExpr(expireSyntaxExpr);
    request.setSearcherCacheClause(cacheClause);

    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();

    ASSERT_TRUE(strategy.init(&request, &errorResult));

    CacheResult cacheResult;
    Result *result = new Result;
    MatchDocs *matchDocs = createMatchDocs(_values.size());
    result->setMatchDocs(matchDocs);
    cacheResult.setResult(result);

    bool ret = strategy.calcAndfillExpireTime(&cacheResult, 100);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)1, cacheResult.getHeader()->expireTime);

    CacheResult cacheResult2;
    Result *result2 = new Result;
    MatchDocs *matchDocs2 = new MatchDocs;
    result2->setMatchDocs(matchDocs2);
    cacheResult2.setResult(result2);

    ret = strategy.calcAndfillExpireTime(&cacheResult2, 100);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)100, cacheResult2.getHeader()->expireTime);
}

TEST_F(DefaultSearcherCacheStrategyTest, testCalcExpireTime) {
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();
    strategy._matchDocAllocatorPtr = _matchDocAllocatorPtr;

    // test with invalid expr ref
    MatchDocs* matchDocs = NULL;
    AttributeExpressionTyped<bool> expireExprWithWrongRef;
    matchdoc::Reference<bool> *boolRef = new matchdoc::Reference<bool>();
    expireExprWithWrongRef.setReference(boolRef);
    uint32_t expireTime = 0;
    bool ret = strategy.calcExpireTime(&expireExprWithWrongRef, matchDocs,
            expireTime);
    delete boolRef;
    ASSERT_TRUE(!ret);

    // test normal
    SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr("attr_expire_time",
            vt_uint32, ATTRIBUTE_NAME);
    unique_ptr<SyntaxExpr> expireSyntaxExpr_ptr(expireSyntaxExpr);
    AttributeExpression *expireExpr =
        strategy._attributeExpressionCreator->createAttributeExpression(expireSyntaxExpr);
    expireExpr->allocate(_matchDocAllocatorPtr.get());

    matchDocs = createMatchDocs(_values.size());
    unique_ptr<MatchDocs> matchDocs_ptr(matchDocs);
    ret = strategy.calcExpireTime(expireExpr, matchDocs,
                                  expireTime);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)1, expireTime);

    // test with null matchdoc
    MatchDocs matchDocs2;
    matchDocs2.addMatchDoc(matchdoc::INVALID_MATCHDOC);
    ret = strategy.calcExpireTime(expireExpr, &matchDocs2,
                                  expireTime);
    ASSERT_TRUE(!ret);
}

TEST_F(DefaultSearcherCacheStrategyTest, testCreateExpireTimeExpr) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResult;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        ASSERT_TRUE(strategy.init(&request, &errorResult));
        ASSERT_TRUE(!strategy.getExpireTimeExpr());
    }

    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResult;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
                "not_exist_attr", vt_uint32, ATTRIBUTE_NAME);
        cacheClause->setExpireExpr(expireSyntaxExpr);
        ASSERT_TRUE(!strategy.init(&request, &errorResult));
        ASSERT_TRUE(!strategy.getExpireTimeExpr());
    }
    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResult;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
                "attr_expire_time", vt_uint32, ATTRIBUTE_NAME);
        cacheClause->setExpireExpr(expireSyntaxExpr);
        ASSERT_TRUE(strategy.init(&request, &errorResult));

        AttributeExpression* expireExpr = strategy.getExpireTimeExpr();
        ASSERT_TRUE(expireExpr);
        AtomicAttributeExpression<uint32_t>* expireTimeExpr =
            dynamic_cast<AtomicAttributeExpression<uint32_t>*>(expireExpr);
        ASSERT_TRUE(expireTimeExpr);
    }
}

TEST_F(DefaultSearcherCacheStrategyTest, testCreateFilter) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        Request request;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        MultiErrorResult errorResult;
        ASSERT_TRUE(strategy.init(&request, &errorResult));
        ASSERT_TRUE(!strategy.getFilter());
    }
    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        Request request;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        MultiErrorResult errorResult;
        FilterClause *filterClause = new FilterClause;
        cacheClause->setFilterClause(filterClause);
        SyntaxExpr *filterExpr = new AtomicSyntaxExpr(
                "not_exist_attr", vt_uint32, ATTRIBUTE_NAME);
        filterClause->setRootSyntaxExpr(filterExpr);
        ASSERT_TRUE(!strategy.init(&request, &errorResult));
        ASSERT_TRUE(!strategy.getFilter());
    }
    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);
        FilterClause *filterClause = new FilterClause;
        cacheClause->setFilterClause(filterClause);
        MultiErrorResult errorResult;
        SyntaxExpr *syntaxExpr1 = new AtomicSyntaxExpr(
                "attr_1", vt_uint32, ATTRIBUTE_NAME);
        SyntaxExpr *syntaxExpr2 = new AtomicSyntaxExpr(
                "attr_2", vt_uint32, ATTRIBUTE_NAME);
        SyntaxExpr *filterExpr = new EqualSyntaxExpr(syntaxExpr1, syntaxExpr2);
        filterClause->setRootSyntaxExpr(filterExpr);
        ASSERT_TRUE(strategy.init(&request, &errorResult));
        Filter *filter = strategy.getFilter();
        ASSERT_TRUE(filter);
    }
}

TEST_F(DefaultSearcherCacheStrategyTest, testFilterCacheResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();

    Request request;
    MultiErrorResult errorResult;
    SearcherCacheClause *cacheClause = new SearcherCacheClause;
    request.setSearcherCacheClause(cacheClause);
    MatchDocs *matchDocs1 = new MatchDocs;
    unique_ptr<MatchDocs> matchDocs_ptr1(matchDocs1);
    uint32_t delCount = strategy.filterCacheResult(matchDocs1);
    ASSERT_EQ((uint32_t)0, delCount);

    SyntaxExpr *syntaxExpr1 = new AtomicSyntaxExpr(
            "attr_1", vt_uint32, ATTRIBUTE_NAME);
    SyntaxExpr *syntaxExpr2 = new AtomicSyntaxExpr(
            "2", vt_uint32, INTEGER_VALUE);
    SyntaxExpr *filterExpr = new EqualSyntaxExpr(syntaxExpr1, syntaxExpr2);
    FilterClause *filterClause = new FilterClause;
    cacheClause->setFilterClause(filterClause);
    filterClause->setRootSyntaxExpr(filterExpr);

    ASSERT_TRUE(strategy.init(&request, &errorResult));

    MatchDocs *matchDocs2 = createMatchDocs(_values.size());
    unique_ptr<MatchDocs> matchDocs_ptr2(matchDocs2);

    delCount = strategy.filterCacheResult(matchDocs2);
    ASSERT_EQ((uint32_t)2, delCount);
    ASSERT_EQ((uint32_t)3, matchDocs2->size());
    ASSERT_TRUE(matchDocs2->getMatchDoc(0).isDeleted());
    ASSERT_TRUE(matchDocs2->getMatchDoc(1).isDeleted());
    ASSERT_TRUE(!matchDocs2->getMatchDoc(2).isDeleted());
}

TEST_F(DefaultSearcherCacheStrategyTest, testReleaseDeletedMatchDocs) {
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();

    MatchDocs *matchDocs = createMatchDocs(3);
    unique_ptr<MatchDocs> matchDocs_ptr(matchDocs);

    matchDocs->setMatchDocAllocator(_matchDocAllocatorPtr);
    _matchDocsVect.clear();
    matchDocs->getMatchDoc(1).setDeleted();
    strategy.releaseDeletedMatchDocs(matchDocs);
    ASSERT_EQ((uint32_t)2, matchDocs->size());
}

TEST_F(DefaultSearcherCacheStrategyTest, testFilterInvalidMatchDocs) {
    HA3_LOG(DEBUG, "Begin Test!");
    DefaultSearcherCacheStrategy strategy(_pool);
    strategy._attributeExpressionCreator = _attributeExprCreator;
    _attributeExprCreator = NULL;
    strategy._functionProvider = _functionProvider;
    strategy._functionCreator = _fakeFunctionCreatorPtr.get();

    MatchDocs *matchDocs = createMatchDocs(3);
    unique_ptr<MatchDocs> matchDocs_ptr(matchDocs);
    matchDocs->setMatchDocAllocator(_matchDocAllocatorPtr);
    _matchDocsVect.clear();

    matchDocs->getMatchDoc(1).setDeleted();
    uint32_t delCount = strategy.filterCacheResult(matchDocs);
    ASSERT_EQ((uint32_t)1, delCount);
}

TEST_F(DefaultSearcherCacheStrategyTest, testInitFail) {

    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResults;

        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
                "attr_expire_time", vt_float, ATTRIBUTE_NAME);
        cacheClause->setExpireExpr(expireSyntaxExpr);
        request.setSearcherCacheClause(cacheClause);

        VirtualAttributeClause *vaClause = new VirtualAttributeClause;
        SyntaxExpr *expr = new AtomicSyntaxExpr(
                "not_exist_attr", vt_uint32, ATTRIBUTE_NAME);
        vaClause->addVirtualAttribute("attr_expire_time", expr);
        request.setVirtualAttributeClause(vaClause);

        ASSERT_TRUE(!strategy.init(&request, &errorResults));
        ErrorResult result = errorResults.getErrorResults().front();
        ErrorCode errorCode = result.getErrorCode();
        ASSERT_EQ(ERROR_CACHE_STRATEGY_INIT, errorCode);
    }
    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResults;
        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        request.setSearcherCacheClause(cacheClause);

        FilterClause *filterClause = new FilterClause;
        cacheClause->setFilterClause(filterClause);
        SyntaxExpr *filterExpr = new AtomicSyntaxExpr(
                "not_exist_attr", vt_uint32, ATTRIBUTE_NAME);
        filterClause->setRootSyntaxExpr(filterExpr);

        ASSERT_TRUE(!strategy.init(&request, &errorResults));
        ErrorResult result = errorResults.getErrorResults().front();
        ErrorCode errorCode = result.getErrorCode();
        ASSERT_EQ(ERROR_CACHE_STRATEGY_INIT, errorCode);
    }
    tearDown();
    setUp();
    {
        DefaultSearcherCacheStrategy strategy(_pool);
        strategy._attributeExpressionCreator = _attributeExprCreator;
        _attributeExprCreator = NULL;
        strategy._functionProvider = _functionProvider;
        strategy._functionCreator = _fakeFunctionCreatorPtr.get();

        Request request;
        MultiErrorResult errorResults;

        SearcherCacheClause *cacheClause = new SearcherCacheClause;
        SyntaxExpr *expireSyntaxExpr = new AtomicSyntaxExpr(
                "attr_expire_time", vt_float, ATTRIBUTE_NAME);
        cacheClause->setExpireExpr(expireSyntaxExpr);
        request.setSearcherCacheClause(cacheClause);

        ASSERT_TRUE(!strategy.init(&request, &errorResults));
        ErrorResult result = errorResults.getErrorResults().front();
        ErrorCode errorCode = result.getErrorCode();
        ASSERT_EQ(ERROR_CACHE_STRATEGY_INIT, errorCode);
    }
}

END_HA3_NAMESPACE(search);
