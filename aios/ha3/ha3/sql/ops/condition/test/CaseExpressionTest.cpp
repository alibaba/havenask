#include <unittest/unittest.h>
#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/AtomicAttributeExpression.h"
#include "suez/turing/expression/framework/BinaryAttributeExpression.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/testlib/fake_multi_value_attribute_reader.h"
#include "indexlib/testlib/fake_attribute_reader.h"
#include "ha3/sql/ops/condition/CaseExpression.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(testlib);

BEGIN_HA3_NAMESPACE(sql);
class CaseExpressionTest : public TESTBASE {
public:
    CaseExpressionTest();
    ~CaseExpressionTest();
public:
    void setUp();
    void tearDown();
protected:
    void allocateMatchDocs();
    void constructCaseWhenExprs(vector<AttributeExpression*> &caseParamExprs, bool isMultiThen);
    template <template<class T> class BinaryOperator>
    AttributeExpressionTyped<bool>* constructRelationBinaryAttrExpr();
protected:
    AttributeReaderPtr _relationLeftIntPtr;
    AttributeReaderPtr _relationRightIntPtr;

    AttributeReaderPtr _thenReaderIntPtr;
    AttributeReaderPtr _elseReaderIntPtr;

    AttributeReaderPtr _thenReaderMultiIntPtr;
    
    autil::mem_pool::Pool _pool;
    matchdoc::MatchDocAllocator *_mdAllocator;
    matchdoc::MatchDoc _matchDoc1;
    matchdoc::MatchDoc _matchDoc2;
    matchdoc::MatchDoc _matchDoc3;
    matchdoc::MatchDoc _matchDoc4;
    matchdoc::MatchDoc _matchDoc5;

    std::vector<AttributeExpression *> _exprs;
protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(expression, CaseExpressionTest);

CaseExpressionTest::CaseExpressionTest() { 
    _mdAllocator = NULL;
    _matchDoc1 = matchdoc::INVALID_MATCHDOC;
    _matchDoc2 = matchdoc::INVALID_MATCHDOC;
    _matchDoc3 = matchdoc::INVALID_MATCHDOC;
    _matchDoc4 = matchdoc::INVALID_MATCHDOC;
    _matchDoc5 = matchdoc::INVALID_MATCHDOC;
}

CaseExpressionTest::~CaseExpressionTest() { 
}

void CaseExpressionTest::setUp() { 
    AUTIL_LOG(DEBUG, "setUp!");

    FakeAttributeReader<int32_t> *relationLeftReaderInt = new FakeAttributeReader<int32_t>;
    relationLeftReaderInt->setAttributeValues("1, 2, 9, 4, 5, 6, 1");
    _relationLeftIntPtr = AttributeReaderPtr(relationLeftReaderInt);
    FakeAttributeReader<int32_t> *relationRightReaderInt = new FakeAttributeReader<int32_t>;
    relationRightReaderInt->setAttributeValues("1, -2, -9, 4, 5, 6, -1");
    _relationRightIntPtr = AttributeReaderPtr(relationRightReaderInt);
    
    FakeAttributeReader<int32_t> *thenReaderInt = new FakeAttributeReader<int32_t>;
    thenReaderInt->setAttributeValues("100, 200, 900, 400, 500, 600, 100");
    _thenReaderIntPtr = AttributeReaderPtr(thenReaderInt);
    FakeAttributeReader<int32_t> *elseReaderInt = new FakeAttributeReader<int32_t>;
    elseReaderInt->setAttributeValues("-1, -2, -3, -4, -5, -6, -7");
    _elseReaderIntPtr = AttributeReaderPtr(elseReaderInt);

    FakeMultiValueAttributeReader<int32_t> *thenReaderMultiInt = 
        new FakeMultiValueAttributeReader<int32_t>("");
    thenReaderMultiInt->setAttributeValues("1, 2, 3#1, 2, 3#3, 4, 5#1, 2, 3#1, 2, 3#3, 4, 5#3, 4, 5");
    _thenReaderMultiIntPtr = AttributeReaderPtr(thenReaderMultiInt);

    _mdAllocator = new matchdoc::MatchDocAllocator(&_pool);
    
}

void CaseExpressionTest::allocateMatchDocs() {
    assert(_mdAllocator);
    _matchDoc1 = _mdAllocator->allocate(1);
    _matchDoc2 = _mdAllocator->allocate(4);
    _matchDoc3 = _mdAllocator->allocate(6); // divide 0.
    _matchDoc4 = _mdAllocator->allocate(222);//invalid MatchDoc
    _matchDoc5 = _mdAllocator->allocate(3);
}

void CaseExpressionTest::tearDown() { 
    AUTIL_LOG(DEBUG, "tearDown!");

    for (size_t i = 0; i < _exprs.size(); ++i) {
        delete _exprs[i];
    }
    _exprs.clear();

    assert(_mdAllocator);
    _mdAllocator->deallocate(_matchDoc1);
    _matchDoc1 = matchdoc::INVALID_MATCHDOC;

    _mdAllocator->deallocate(_matchDoc2);
    _matchDoc2 = matchdoc::INVALID_MATCHDOC;

    _mdAllocator->deallocate(_matchDoc3);
    _matchDoc3 = matchdoc::INVALID_MATCHDOC;

    _mdAllocator->deallocate(_matchDoc4);
    _matchDoc4 = matchdoc::INVALID_MATCHDOC;

    _mdAllocator->deallocate(_matchDoc5);
    _matchDoc5 = matchdoc::INVALID_MATCHDOC;

    DELETE_AND_SET_NULL(_mdAllocator);
}

void CaseExpressionTest::constructCaseWhenExprs(vector<AttributeExpression*> &caseParamExprs, bool isMultiThen) 
{
    AttributeExpression *whenExpr
        = constructRelationBinaryAttrExpr<std::not_equal_to>();
    
    AttributeIteratorBase* thenIterator = NULL;
    AttributeExpression *thenExpr = NULL;
    if (isMultiThen) {
        thenIterator = _thenReaderMultiIntPtr->CreateIterator(&_pool);        
        AttributeIteratorTyped<autil::MultiValueType<int32_t> >* typedThenIterator = 
            dynamic_cast<AttributeIteratorTyped<autil::MultiValueType<int32_t> >* >(thenIterator);
        thenExpr = new AtomicAttributeExpression<autil::MultiValueType<int32_t> >("_then_", typedThenIterator);
    } else {
        thenIterator = _thenReaderIntPtr->CreateIterator(&_pool);
        AttributeIteratorTyped<int32_t>* typedThenIterator = 
            dynamic_cast<AttributeIteratorTyped<int32_t>* >(thenIterator);
        thenExpr = new AtomicAttributeExpression<int32_t>("_then_", typedThenIterator);
    }

    AttributeIteratorBase* elseIterator = _elseReaderIntPtr->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* typedElseIterator = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(elseIterator);
    AttributeExpression *elseExpr
        = new AtomicAttributeExpression<int32_t>("_else_", typedElseIterator);

    _exprs.push_back(whenExpr);
    _exprs.push_back(thenExpr);
    _exprs.push_back(elseExpr);

    caseParamExprs.push_back(whenExpr);
    caseParamExprs.push_back(thenExpr);
    caseParamExprs.push_back(elseExpr);    
}

template <template<class T> class BinaryOperator> 
AttributeExpressionTyped<bool>* 
CaseExpressionTest::constructRelationBinaryAttrExpr() 
{
    AttributeIteratorBase* leftIterator = _relationLeftIntPtr->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* leftAttrIterator = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(leftIterator);

    AttributeIteratorBase* rightIterator = _relationRightIntPtr->CreateIterator(&_pool);
    AttributeIteratorTyped<int32_t>* rightAttrIterator = 
        dynamic_cast<AttributeIteratorTyped<int32_t>* >(rightIterator);

    AttributeExpression *attrExprLeft
        = new AtomicAttributeExpression<int32_t>("price1", leftAttrIterator);
    AttributeExpression *attrExprRight
        = new AtomicAttributeExpression<int32_t>("price2", rightAttrIterator);

    AttributeExpressionTyped<bool>* ret = 
        new BinaryAttributeExpression<BinaryOperator, int32_t>(attrExprLeft, attrExprRight);

    _exprs.push_back(attrExprLeft);
    _exprs.push_back(attrExprRight);
    return ret;
}


TEST_F(CaseExpressionTest, testSimpleCase) { 
    AUTIL_LOG(DEBUG, "Begin Test!");

    // for single then expr
    vector<AttributeExpression*> caseParamExprs;
    constructCaseWhenExprs(caseParamExprs, false);

    AttributeExpression* expr = CaseExpressionCreator::createCaseExpression(vt_int32, caseParamExprs, &_pool);
    ASSERT_TRUE(expr != NULL);

    using OutputType = VariableTypeTraits<vt_int32, false>::AttrExprType;
    CaseExpression<OutputType>* caseExpr = dynamic_cast<CaseExpression<OutputType>*>(expr);
    ASSERT_TRUE(caseExpr != NULL);
    
    allocateMatchDocs();

    ASSERT_EQ(200, caseExpr->evaluateAndReturn(_matchDoc1));

    ASSERT_EQ(-5, caseExpr->evaluateAndReturn(_matchDoc2));

    ASSERT_EQ(100, caseExpr->evaluateAndReturn(_matchDoc3));

    ASSERT_EQ(0, caseExpr->evaluateAndReturn(_matchDoc4));

    POOL_DELETE_CLASS(expr);
    
    // for multi then expr
    caseParamExprs.clear();
    constructCaseWhenExprs(caseParamExprs, true);

    expr = CaseExpressionCreator::createCaseExpression(vt_int32, caseParamExprs, &_pool);
    ASSERT_TRUE(expr == NULL);
}

END_HA3_NAMESPACE(sql);
