#ifndef ISEARCH_MULTIVALUEFUNCTION_H
#define ISEARCH_MULTIVALUEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/FunctionInterface.h>
#include <autil/MultiValueType.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/common/DataProvider.h>

BEGIN_HA3_NAMESPACE(func_expression);

template <typename T>
class MultiValueFunction : public FunctionInterfaceTyped<autil::MultiValueType<T> >
{
public:
    MultiValueFunction(HA3_NS(search)::AttributeExpressionTyped<autil::MultiValueType<T> > *attrExpr)
    {
        _attrExpr = attrExpr;
    }
    ~MultiValueFunction() {}
private:
    MultiValueFunction(const MultiValueFunction &);
    MultiValueFunction& operator=(const MultiValueFunction &);
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) override {
        return true;
    }
    autil::MultiValueType<T> evaluate(matchdoc::MatchDoc matchDoc) override;
    void endRequest() override{}
private:
    HA3_NS(search)::AttributeExpressionTyped<autil::MultiValueType<T> > *_attrExpr;
private:
    HA3_LOG_DECLARE();
};

template <typename T>
inline autil::MultiValueType<T>
MultiValueFunction<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    (void)_attrExpr->evaluate(matchDoc);
    return _attrExpr->getValue(matchDoc);
}

END_HA3_NAMESPACE(functions);

#endif //ISEARCH_MULTIVALUEFUNCTION_H
