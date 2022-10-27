#include "CaseExpression.h"

BEGIN_HA3_NAMESPACE(sql);

using namespace std;
using namespace suez::turing;

AttributeExpression*
CaseExpressionCreator::createCaseExpression(suez::turing::VariableType resultType,
                                            const std::vector<AttributeExpression*> &exprs,
                                            autil::mem_pool::Pool* pool)
{
    // TODO error log
    if(1 != (exprs.size() % 2)) {
        return NULL;
    }
    
#define CREATE_CASE_EXPR(vt_type)                                       \
    case (vt_type):                                                     \
    {                                                                   \
        using OutputType =                                              \
            VariableTypeTraits<vt_type, false>::AttrExprType;           \
        vector<CaseTuple<OutputType> > caseTuples;                      \
        OutputExpr<OutputType>* elseExpr;                               \
        size_t i;                                                       \
        for (i = 0; i < exprs.size() - 1; i += 2) {                     \
            CaseTuple<OutputType> caseTuple;                            \
            caseTuple.whenExpr =                                        \
                dynamic_cast<AttributeExpressionTyped<bool>* >(exprs[i]); \
            if (caseTuple.whenExpr == NULL) {                           \
                return NULL;                                            \
            }                                                           \
            caseTuple.thenExpr =                                        \
            CreateOutputExpr<OutputType>(exprs[i+1], pool);             \
            if (caseTuple.thenExpr == NULL) {                           \
                return NULL;                                            \
            }                                                           \
            caseTuples.push_back(caseTuple);                            \
        }                                                               \
        elseExpr = CreateOutputExpr<OutputType>(exprs[i], pool);        \
        if (elseExpr == NULL) {                                         \
            return NULL;                                                \
        }                                                               \
        return POOL_NEW_CLASS(pool, CaseExpression<OutputType>,         \
                              caseTuples, elseExpr);                    \
}

    switch(resultType)
    {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(CREATE_CASE_EXPR);
    default:
        return NULL;
    }
    #undef CREATE_CASE_EXPR
}



END_HA3_NAMESPACE(sql);
