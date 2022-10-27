#ifndef ISEARCH_ATTRIBUTEEXPRESSIONCONVERTOR_H
#define ISEARCH_ATTRIBUTEEXPRESSIONCONVERTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>

BEGIN_HA3_NAMESPACE(rank);

class AttributeExpressionConvertor
{
public:
    AttributeExpressionConvertor();
    ~AttributeExpressionConvertor();
private:
    AttributeExpressionConvertor(const AttributeExpressionConvertor &);
    AttributeExpressionConvertor& operator=(const AttributeExpressionConvertor &);
public:
    template<class BaseType, template <typename T> class DerivedType>
    static BaseType* convert(suez::turing::AttributeExpression* attrExpr) {
        if (NULL == attrExpr) {
            HA3_LOG(DEBUG, "attr expr is NULL");
            return NULL;
        }

        matchdoc::ReferenceBase *ref = NULL;
        if (suez::turing::ET_COMBO != attrExpr->getExpressionType()) {
            ref = attrExpr->getReferenceBase();
        } else {
            suez::turing::ComboAttributeExpression *comboAttrExpr =
                dynamic_cast<suez::turing::ComboAttributeExpression*>(attrExpr);
            assert(comboAttrExpr);
            const suez::turing::ExpressionVector &exprVect = comboAttrExpr->getExpressions();
            if (exprVect.size() == 0) {
                HA3_LOG(DEBUG, "combo attr expr vect size is 0");
                return NULL;
            }
            ref = exprVect[0]->getReferenceBase();
        }

        if (ref == NULL) {
            HA3_LOG(WARN, "ref of attrexpr is NULL");
            return NULL;
        }

        auto vt = ref->getValueType().getBuiltinType();
        HA3_LOG(DEBUG, "variable type:%d", (int)vt);
#ifndef CREATE_DERIVED_OBJECT_CASE
#define CREATE_DERIVED_OBJECT_CASE(vt)                                  \
        case vt:                                                        \
        {                                                               \
            auto refTyped = dynamic_cast<matchdoc::Reference<           \
                suez::turing::VariableTypeTraits<vt, false>::AttrExprType> *>(ref); \
            if (refTyped == NULL) {                                     \
                HA3_LOG(WARN, "dynamic varref cast failed");            \
                return NULL;                                            \
            }                                                           \
            return new DerivedType<                                     \
                suez::turing::VariableTypeTraits<vt, false>::AttrExprType>(refTyped); \
        }

        switch(vt) {
            CREATE_DERIVED_OBJECT_CASE(vt_int8);
            CREATE_DERIVED_OBJECT_CASE(vt_int16);
            CREATE_DERIVED_OBJECT_CASE(vt_int32);
            CREATE_DERIVED_OBJECT_CASE(vt_int64);
            CREATE_DERIVED_OBJECT_CASE(vt_uint8);
            CREATE_DERIVED_OBJECT_CASE(vt_uint16);
            CREATE_DERIVED_OBJECT_CASE(vt_uint32);
            CREATE_DERIVED_OBJECT_CASE(vt_uint64);
            CREATE_DERIVED_OBJECT_CASE(vt_float);
            CREATE_DERIVED_OBJECT_CASE(vt_double);
        default:
            HA3_LOG(WARN, "invalid variable type");
            return NULL;
        }
#undef CREATE_DERIVED_OBJECT_CASE
#endif
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AttributeExpressionConvertor);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_ATTRIBUTEEXPRESSIONCONVERTOR_H
