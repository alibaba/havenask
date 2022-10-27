#ifndef ISEARCH_FAKEATTRIBUTEEXPRESSION_H
#define ISEARCH_FAKEATTRIBUTEEXPRESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);

template<typename T>
class FakeAttributeExpression : public suez::turing::AttributeExpressionTyped<T>
{
public:
    static uint32_t sEvaluateCount;
public:
    FakeAttributeExpression(const std::string &name,
                            const std::vector<T> *values)
        :_attributeName(name)
        , _values(values)
        , _currentRef(NULL)
    {
        this->setOriginalString(_attributeName);
    }

    FakeAttributeExpression(const std::string &name,
                            const std::vector<T> &valueVec)
        : _attributeName(name)
        , _valueVec(valueVec)
        , _values(&_valueVec)
        , _currentRef(NULL)
    {
        this->setOriginalString(_attributeName);
    }

    virtual ~FakeAttributeExpression() {}
    virtual bool operator == (const suez::turing::AttributeExpression *checkExpr) const {
        return false;
    }
    virtual bool evaluate(matchdoc::MatchDoc matchDoc) {
        evaluateAndReturn(matchDoc);
        return true;
    }
    virtual T evaluateAndReturn(matchdoc::MatchDoc matchDoc);
    void setSubCurrentRef(matchdoc::Reference<matchdoc::MatchDoc> *currentRef) {
        _currentRef = currentRef;
    }
private:
    std::string _attributeName;
    std::vector<T> _valueVec;
    const std::vector<T> *_values;
    matchdoc::Reference<matchdoc::MatchDoc> *_currentRef;
};

template<typename T>
uint32_t FakeAttributeExpression<T>::sEvaluateCount = 0;

template<typename T>
inline T FakeAttributeExpression<T>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    FakeAttributeExpression<T>::sEvaluateCount++;
    size_t idx = (size_t)(matchDoc.getDocId());
    if (_currentRef) {
        idx = (_currentRef->getReference(matchDoc)).getDocId();
    }
    T t = T();
    if (idx >= _values->size()) {
        this->setValue(t);
        if (this->getReference()) {
            this->getReference()->set(matchDoc, t);
        }
        return t;
    }
    T value = (*_values)[idx];
    this->setValue(value);
    if (this->getReference()) {
        this->getReference()->set(matchDoc, value);
    }
    return value;
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEATTRIBUTEEXPRESSION_H
