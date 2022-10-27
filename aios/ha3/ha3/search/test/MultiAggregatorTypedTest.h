#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/AggregateBase.h"
#include "ha3/search/MultiAggregator.h"
#include <ha3/search/BatchMultiAggregator.h>

using namespace testing;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

template <typename T>
class MultiAggregatorTypedTest : public TESTBASE, public AggregateBase {
public:
    MultiAggregatorTypedTest() : _multiAgg(NULL) {}

    virtual MultiAggregator* create() {
        return new MultiAggregator(_pool);
    }
    void setUp() {
        baseSetUp();
        _multiAgg = create();
        initMultiAggregator();
    }
    
    void tearDown() {
        _agg.clear();
        DELETE_AND_SET_NULL(_multiAgg);
        baseTearDown();
    }
    
    void initMultiAggregator();
protected:
    std::vector<T*> _agg;
    MultiAggregator *_multiAgg;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(search, MultiAggregatorTypedTest, T);

template <typename T>
void MultiAggregatorTypedTest<T>::initMultiAggregator() {

    AttributeExpressionTyped<int64_t> *keyExpr1 = initKeyExpression<int64_t>("uid");
    T *agg1 = new T(keyExpr1, 100, _pool);

    AttributeExpressionTyped<int64_t> *expr1 = initFunExpression<int64_t>("uid1");
    MaxAggregateFunction<int64_t> *fun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int64_t>, "price", vt_int64, expr1);
    agg1->addAggregateFunction(fun1);
    _agg.emplace_back(agg1);

    AttributeExpressionTyped<int64_t> *keyExpr2 = initKeyExpression<int64_t>("type");
    T *agg2 = new T(keyExpr2, 100, _pool);

    AttributeExpressionTyped<double> *expr2 = initFunExpression<double>("uid2");
    MinAggregateFunction<double> *fun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<double>, "price", vt_double, expr2);
    agg2->addAggregateFunction(fun2);
    _agg.emplace_back(agg2);

    AttributeExpressionTyped<int64_t> *keyExpr3 = initKeyExpression<int64_t>("type");
    T *agg3 = new T(keyExpr3, 100, _pool);

    AttributeExpressionTyped<double> *expr3 = initFunExpression<double>("uid3");
    SumAggregateFunction<double> *fun3
        = POOL_NEW_CLASS(_pool, SumAggregateFunction<double>, "price", vt_double, expr3);
    agg3->addAggregateFunction(fun3);
    _agg.emplace_back(agg3);

        
    AttributeExpressionTyped<int64_t> *keyExpr4 = initKeyExpression<int64_t>("type");
    T *agg4 = new T(keyExpr4, 100, _pool);

    CountAggregateFunction *fun4 = POOL_NEW_CLASS(_pool, CountAggregateFunction);
    agg4->addAggregateFunction(fun4);
    _agg.emplace_back(agg4);

    for (auto it : _agg) {
        this->_multiAgg->addAggregator(it);
    }
    this->_multiAgg->setMatchDocAllocator(_mdAllocator);
}

END_HA3_NAMESPACE();
