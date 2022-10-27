#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/AggregateBase.h"
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/BatchAggregator.h>

using namespace std;
using namespace testing;
using namespace suez::turing;
using namespace autil;
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class AggregatorTest : public AggregateBase,
                       public TestWithParam< ::std::tr1::tuple<VariableType, VariableType, bool> >
{
public:
    AggregatorTest() : _keyType(vt_unknown), _funType(vt_unknown), _isBatch(false)
    {}
    ~AggregatorTest() {
    }
public:
    void setUp();
    void tearDown();

private:
    VariableType _keyType;
    VariableType _funType;
    bool _isBatch;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AggregatorTest);

void AggregatorTest::setUp() {
    baseSetUp();
    std::tr1::tie(_keyType, _funType, _isBatch) = GetParam();
}

void AggregatorTest::tearDown() {
    baseTearDown();
}

TEST_P(AggregatorTest, testSingleValueAgg) {

    HA3_LOG(DEBUG, "Begin Test!");

#define TEEST_WITH_FUNCTION_TYPED(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T_FUN; \
        _funAttributeExpr = initFunExpression<T_FUN>("attr");           \
        AggregateFunction *fun = NULL;                                  \
        fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<T_FUN>, "price", vt_double, (AttributeExpressionTyped<T_FUN> *)_funAttributeExpr); \
        agg->addAggregateFunction(fun);                                 \
        aggMatchDoc(DOC_NUM, agg);                                      \
        const matchdoc::Reference<T_FUN> *funResultRef = agg->template getFunResultReference<T_FUN>("max(price)"); \
        ASSERT_TRUE(funResultRef);                                      \
        ASSERT_EQ(2u, agg->getKeyCount());                              \
        matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((T_KEY)0); \
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);          \
        ASSERT_EQ((T_FUN)8, *(funResultRef->getPointer(resultSlab)));   \
        resultSlab = agg->getResultMatchDoc((T_KEY)1);                  \
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);          \
        ASSERT_EQ((T_FUN)9, *(funResultRef->getPointer(resultSlab)));   \
        resultSlab = agg->getResultMatchDoc(31);                        \
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);          \
        T_FUN value;                                                    \
        ASSERT_TRUE(agg->template getFunResultOfKey<T_FUN>("max(price)", 0, value)); \
        ASSERT_EQ((T_FUN)8, value);                                     \
        ASSERT_TRUE(agg->template getFunResultOfKey<T_FUN>("max(price)", 1, value)); \
        ASSERT_EQ((T_FUN)9, value);                                     \
        ASSERT_TRUE(!agg->template getFunResultOfKey<T_FUN>("max(price)", 31, value)); \
    }                                                                   \
    break

#define CREATE_SINGLE_AGG_TESTER_HELPER(vt_type, FUNCITON)              \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T_KEY; \
        _keyAttributeExpr = initKeyExpression<T_KEY>("key");            \
        NormalAggregator<T_KEY>* agg = NULL;                            \
        if (!_isBatch) {                                                \
            agg = POOL_NEW_CLASS(_pool, NormalAggregator<T_KEY>, (AttributeExpressionTyped<T_KEY> *)_keyAttributeExpr, 100, _pool); \
        } else {                                                        \
            agg = POOL_NEW_CLASS(_pool, BatchAggregator<T_KEY>, (AttributeExpressionTyped<T_KEY> *)_keyAttributeExpr, 100, _pool); \
        }                                                               \
        ASSERT_TRUE(agg);                                               \
        switch(_funType) {                                              \
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(TEEST_WITH_FUNCTION_TYPED); \
            POOL_DELETE_CLASS(agg);                                     \
        default:                                                        \
            auto typeStr = vt2TypeName(_funType);                       \
            HA3_LOG(WARN, "does not support this funciton type: %s", typeStr.c_str()); \
            break;                                                      \
        }                                                               \
    }                                                                   \
    break

#define CASE_EACH_AGGREGATOR(MACRO, FUNCITON)   \
    MACRO(vt_int8, FUNCITON);                   \
    MACRO(vt_int16, FUNCITON);                  \
    MACRO(vt_int32, FUNCITON);                  \
    MACRO(vt_int64, FUNCITON);                  \
    MACRO(vt_uint8, FUNCITON);                  \
    MACRO(vt_uint16, FUNCITON);

    switch(_keyType) {
        CASE_EACH_AGGREGATOR(CREATE_SINGLE_AGG_TESTER_HELPER, TEEST_WITH_FUNCTION_TYPED);
    default:
        auto typeStr = vt2TypeName(_keyType);
        HA3_LOG(WARN, "does not support this Aggregator type: %s",
                typeStr.c_str());
        break;
    }

#undef CREATE_SINGLE_AGG_TESTER_HELPER
#undef TEEST_WITH_FUNCTION_TYPED
}
// use macro INSTANTIATE_TEST_SUITE_P when gtest(version) > 1.8.0
/*
INSTANTIATE_TEST_SUITE_P(VariableType, AggregatorTest, Combine(
                Values(vt_int8, vt_int16, vt_int32, vt_int64, vt_float, vt_uint8, vt_uint16, vt_uint32, vt_uint64, vt_double),
                Values(vt_int8, vt_int16, vt_int32, vt_int64, vt_float, vt_uint8, vt_uint16, vt_uint32, vt_uint64, vt_double),
                testing::Bool())
                         );
*/
END_HA3_NAMESPACE();
