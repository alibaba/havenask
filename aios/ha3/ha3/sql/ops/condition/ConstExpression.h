#pragma once

#include <ha3/sql/common/common.h>
#include <suez/turing/expression/common.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(sql);
template<typename T>
class ConstExpression : public suez::turing::AttributeExpressionTyped<T>
{
public:
    ConstExpression(const T &constValue) {
        this->setValue(constValue);
        setEvaluated();
    }
    virtual ~ConstExpression() {}
public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        this->storeValue(matchDoc, this->_value);
        return true;
    }
    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        this->storeValue(matchDoc, this->_value);
        return this->_value;
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count) override {
        for (uint32_t i = 0; i < count; i++) {
            this->storeValue(matchDocs[i], this->_value);
        }
        return true;
    }
    void setEvaluated() override {
        this->_isEvaluated = true;
    }
    suez::turing::ExpressionType getExpressionType() const override {
        return suez::turing::ET_CONST;
    }
    bool operator==(const suez::turing::AttributeExpression *checkExpr) const override;
};

template<typename T>
inline bool ConstExpression<T>::operator == (
        const suez::turing::AttributeExpression *checkExpr) const
{
    assert(checkExpr);
    const ConstExpression<T>* expr =
        dynamic_cast<const ConstExpression<T> *>(checkExpr);
    if (expr && this->_value == expr->_value
        && this->getType() == expr->getType())
    {
        return true;
    }
    return false;
}

END_HA3_NAMESPACE(sql);
