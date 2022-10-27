#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/AtomicAttributeExpression.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/search/Filter.h>
#include <indexlib/index/normal/attribute/accessor/attribute_reader.h>
#include <string>
#include <ha3/search/Filter.h>
#include <matchdoc/MatchDocAllocator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3_sdk/testlib/index/FakeAttributeReader.h>
#include <ha3_sdk/testlib/index/FakeMultiValueAttributeReader.h>
#include <ha3/search/BinaryAttributeExpression.h>
#include <ha3/search/test/FakeJoinDocidReader.h>
#include <suez/turing/expression/framework/JoinDocIdConverterBase.h>

using namespace std;
using namespace std::tr1;
using namespace suez::turing;

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

enum ExprType {
    ET_NORMAL = 0,
    ET_SUB,
    ET_JOIN
};

class FilterTest : public TESTBASE {
public:
    FilterTest();
    ~FilterTest();
public:
    void setUp();
    void tearDown();
protected:
    void createFilter(AttributeExpressionTyped<bool> *attrExpr);

    template <template<class T> class BinaryOperator> 
    AttributeExpressionTyped<bool>* constructBinaryAttrExpr(
            ExprType exprType = ET_NORMAL);

    template <template<class T> class BinaryOperator>
    AttributeExpressionTyped<bool>* constructMultiValueBinaryAttrExpr();
protected:
    index::AttributeReaderPtr _attributeReaderPtr1;
    index::AttributeReaderPtr _attributeReaderPtr2;
    index::AttributeReaderPtr _attributeReaderPtr3;

    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocator *_mdAllocator;
    Filter *_filter;

    matchdoc::MatchDoc _matchDoc1;
    matchdoc::MatchDoc _matchDoc2;

    std::vector<AttributeExpression *> _exprs;

    JoinDocIdConverterBase *_joinDocIdConverter;
    FakeJoinDocidReader* _joinDocidReader;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, FilterTest);


FilterTest::FilterTest() { 
    _mdAllocator = NULL;
    _filter = NULL;

    _matchDoc1 = matchdoc::INVALID_MATCHDOC;
    _matchDoc2 = matchdoc::INVALID_MATCHDOC;

    _joinDocIdConverter = NULL;
}

FilterTest::~FilterTest() { 
}

void FilterTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    FakeAttributeReader<int32_t> *fakeReader1 = new FakeAttributeReader<int32_t>;
    fakeReader1->setAttributeValues("1, 2, 3, 4, 5, 6");
    _attributeReaderPtr1 =  AttributeReaderPtr(fakeReader1);

    HA3_LOG(DEBUG, "setUp1!");
    FakeAttributeReader<int32_t> *fakeReader2 = new FakeAttributeReader<int32_t>;
    fakeReader2->setAttributeValues("-10, -9, 8, 7, 5, 9");
    _attributeReaderPtr2 =  AttributeReaderPtr(fakeReader2);

    FakeMultiValueAttributeReader<int32_t> *fakeReader3 = 
        new FakeMultiValueAttributeReader<int32_t>;
    fakeReader3->setAttributeValues("2, 2, 3#1, 2, 3#3, 4, 5#1, 2, 3#1, 2, 3#3, 4, 5");
    _attributeReaderPtr3 =  AttributeReaderPtr(fakeReader3);
   
    HA3_LOG(DEBUG, "setUp2!");
    _mdAllocator = new common::Ha3MatchDocAllocator(&_pool, true);
    map<docid_t, docid_t> docIdMap;
    docIdMap[0] = 3;
    docIdMap[2] = INVALID_DOCID;
    _joinDocidReader = POOL_NEW_CLASS((&_pool), FakeJoinDocidReader);
    _joinDocidReader->setDocIdMap(docIdMap);
    _joinDocIdConverter = POOL_NEW_CLASS((&_pool), JoinDocIdConverterBase, 
                                         "join_attr", _joinDocidReader);
    _joinDocIdConverter->init(_mdAllocator, false);
    
    HA3_LOG(DEBUG, "end setup!");
}

void FilterTest::createFilter(AttributeExpressionTyped<bool> *attrExpr) {
    _matchDoc1 = _mdAllocator->allocate(0);
    _matchDoc2 = _mdAllocator->allocate(2);
    _filter = new Filter(attrExpr);
}

void FilterTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");

    for (size_t i = 0; i < _exprs.size(); ++i) {
        delete _exprs[i];
    }
    
    _exprs.clear();

    if (matchdoc::INVALID_MATCHDOC != _matchDoc1) {
        _mdAllocator->deallocate(_matchDoc1);
        _matchDoc1 = matchdoc::INVALID_MATCHDOC;
    }
    
    if (matchdoc::INVALID_MATCHDOC != _matchDoc2) {
        _mdAllocator->deallocate(_matchDoc2);
        _matchDoc2 = matchdoc::INVALID_MATCHDOC;
    }

    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_filter);
    POOL_DELETE_CLASS(_joinDocIdConverter);
    HA3_LOG(DEBUG, "end teardown!");
}

TEST_F(FilterTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    AttributeExpressionTyped<bool> *attrExpr = constructBinaryAttrExpr<std::less>();
    ASSERT_TRUE(attrExpr);

    createFilter(attrExpr);

    ASSERT_TRUE(_filter);
    ASSERT_TRUE(!_filter->pass(_matchDoc1));
    ASSERT_TRUE(_filter->pass(_matchDoc2));
}

TEST_F(FilterTest, testMultiValueEqualFilter) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<bool> *attrExpr = 
        constructMultiValueBinaryAttrExpr<multi_equal_to>();
    ASSERT_TRUE(attrExpr);

    createFilter(attrExpr);

    ASSERT_TRUE(_filter);

    ASSERT_TRUE(!_filter->pass(_matchDoc1));
    ASSERT_TRUE(_filter->pass(_matchDoc2));
}

TEST_F(FilterTest, testFilterWithJoinField) {
    AttributeExpressionTyped<bool> *attrExpr = 
        constructBinaryAttrExpr<less_equal>(ET_JOIN);
    ASSERT_TRUE(attrExpr);

    createFilter(attrExpr);

    ASSERT_TRUE(_filter);

    ASSERT_TRUE(_filter->pass(_matchDoc1)); // 4 <= 7
    ASSERT_TRUE(_filter->pass(_matchDoc2)); // 0 <= 0
}

template <template<class T> class BinaryOperator> 
AttributeExpressionTyped<bool>* 
FilterTest::constructBinaryAttrExpr(ExprType exprType) 
{
    AttributeIteratorBase* iterator1 = _attributeReaderPtr1->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* attrIterator1 = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(iterator1);

    AttributeIteratorBase* iterator2 = _attributeReaderPtr2->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* attrIterator2 = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(iterator2);

    SubDocIdAccessor subDocIdAccessor(_mdAllocator->getSubDocRef());
    AttributeExpression *attrExpr1 = NULL;
    AttributeExpression *attrExpr2 = NULL;

    switch (exprType) {
    case ET_NORMAL:
        attrExpr1 = new AtomicAttributeExpression<int32_t>("price1", attrIterator1);
        attrExpr2 = new AtomicAttributeExpression<int32_t>("price2", attrIterator2);
        break;
    case ET_SUB:
        attrExpr1 = new AtomicAttributeExpression<int32_t, SubDocIdAccessor>(
                "price1", attrIterator1, subDocIdAccessor);
        attrExpr2 = new AtomicAttributeExpression<int32_t, SubDocIdAccessor>(
                "price2", attrIterator2, subDocIdAccessor);
        break;
    case ET_JOIN: {
        JoinDocIdAccessor joinDocIdAccessor(_joinDocIdConverter);
        attrExpr1 = new AtomicAttributeExpression<int32_t, JoinDocIdAccessor>(
                "price1", attrIterator1, joinDocIdAccessor);
        attrExpr2 = new AtomicAttributeExpression<int32_t, JoinDocIdAccessor>(
                "price2", attrIterator2, joinDocIdAccessor);
    }
        break;
    default:
        assert(false);
        break;
    }

    AttributeExpressionTyped<bool>* ret = 
        new BinaryAttributeExpression<BinaryOperator, int32_t>(attrExpr1, attrExpr2);
    if (exprType == ET_SUB) {
        attrExpr1->setIsSubExpression(true);
        attrExpr2->setIsSubExpression(true);
        ret->setIsSubExpression(true);
    }

    _exprs.push_back(attrExpr1);
    _exprs.push_back(attrExpr2);
    _exprs.push_back(ret);

    return ret;
}

template <template<class T> class BinaryOperator> 
AttributeExpressionTyped<bool>* 
FilterTest::constructMultiValueBinaryAttrExpr() 
{
    AttributeIteratorBase* iterator1 = _attributeReaderPtr1->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* attrIterator1 = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(iterator1);

    AttributeIteratorBase* iterator2 = _attributeReaderPtr3->CreateIterator(&_pool);
    AttributeIteratorTyped<autil::MultiValueType<int32_t> >* attrIterator2 = 
        dynamic_cast<AttributeIteratorTyped<autil::MultiValueType<int32_t> >* >(iterator2);

    AttributeExpression *attrExpr1
        = new AtomicAttributeExpression<int32_t>("price1", attrIterator1);
    AttributeExpression *attrExpr2
        = new AtomicAttributeExpression<autil::MultiValueType<int32_t> >("price2", attrIterator2);

    AttributeExpressionTyped<bool>* ret = 
        new BinaryAttributeExpression<BinaryOperator, int32_t, 
                                      autil::MultiValueType<int32_t> >(attrExpr1, attrExpr2);

    _exprs.push_back(attrExpr1);
    _exprs.push_back(attrExpr2);
    _exprs.push_back(ret);

    return ret;
}

END_HA3_NAMESPACE(search);

