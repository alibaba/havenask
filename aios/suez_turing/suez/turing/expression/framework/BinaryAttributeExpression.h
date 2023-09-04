/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <assert.h>
#include <cmath>
#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <typeinfo>

#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace suez {
namespace turing {

template <typename T>
struct divides : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) const {
        if (rhs == 0) {
            return 0;
        }
        return lhs / rhs;
    }
};

template <>
inline autil::MultiChar divides<autil::MultiChar>::operator()(autil::MultiChar lhs, autil::MultiChar rhs) const {
    assert(false);
    return autil::MultiChar();
}

template <typename T>
struct power : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) const { return std::pow(lhs, rhs); }
};

template <>
inline autil::MultiChar power<autil::MultiChar>::operator()(autil::MultiChar lhs, autil::MultiChar rhs) const {
    assert(false);
    return autil::MultiChar();
}
template <>
inline bool power<bool>::operator()(bool lhs, bool rhs) const {
    return lhs || rhs;
}
template <typename T>
struct log : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) const {
        if (lhs <= 0 || rhs <= 0 || std::log(lhs) == 0) {
            return 0;
        }
        return std::log(rhs) / std::log(lhs);
    }
};

template <>
inline autil::MultiChar log<autil::MultiChar>::operator()(autil::MultiChar lhs, autil::MultiChar rhs) const {
    assert(false);
    return autil::MultiChar();
}

template <typename T>
struct bit_and : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) { return lhs & rhs; }
};

template <typename T>
struct bit_xor : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) { return lhs ^ rhs; }
};

template <typename T>
struct bit_or : std::binary_function<T, T, T> {
    T operator()(T lhs, T rhs) { return lhs | rhs; }
};

#define SPECIAL_BIT_FLOAT_DOULE(opType)                                                                                \
    template <>                                                                                                        \
    struct opType<float> : std::binary_function<float, float, float> {                                                 \
        float operator()(float, float) {                                                                               \
            assert(false);                                                                                             \
            return 0;                                                                                                  \
        }                                                                                                              \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct opType<double> : std::binary_function<double, double, double> {                                             \
        double operator()(double, double) {                                                                            \
            assert(false);                                                                                             \
            return 0;                                                                                                  \
        }                                                                                                              \
    }

SPECIAL_BIT_FLOAT_DOULE(bit_and);
SPECIAL_BIT_FLOAT_DOULE(bit_xor);
SPECIAL_BIT_FLOAT_DOULE(bit_or);

#undef SPECIAL_BIT_FLOAT_DOULE

/////////////////// multi_equal_to and multi_not_equal_to
template <typename T>
struct multi_equal_to : std::binary_function<T, autil::MultiValueType<T>, bool> {
    bool operator()(const T &x, const autil::MultiValueType<T> &y) const {
        for (uint32_t i = 0; i < y.size(); ++i) {
            if (x == y[i]) {
                return true;
            }
        }
        return false;
    }
};

template <typename T>
struct multi_not_equal_to : std::binary_function<T, autil::MultiValueType<T>, bool> {
    bool operator()(const T &x, const autil::MultiValueType<T> &y) const {
        multi_equal_to<T> multiEqual;
        return !multiEqual(x, y);
    }
};

//////////////////////BinaryAttributeExpression
template <template <class T> class BinaryOperatorType,
          typename LeftArgType,
          typename RightArgType = LeftArgType,
          typename ResultType = typename BinaryOperatorType<LeftArgType>::result_type>
class BinaryAttributeExpression : public AttributeExpressionTyped<ResultType> {
public:
    typedef AttributeExpressionTyped<LeftArgType> LeftAttrExpr;
    typedef AttributeExpressionTyped<RightArgType> RightAttrExpr;

public:
    BinaryAttributeExpression(AttributeExpression *leftExpr, AttributeExpression *rightExpr) {
        _leftExpr = dynamic_cast<LeftAttrExpr *>(leftExpr);
        _rightExpr = dynamic_cast<RightAttrExpr *>(rightExpr);
        this->andIsDeterministic(leftExpr->isDeterministic());
        this->andIsDeterministic(rightExpr->isDeterministic());
        assert(_leftExpr);
        assert(_rightExpr);
    }
    virtual ~BinaryAttributeExpression() {
        _leftExpr = NULL;
        _rightExpr = NULL;
    }

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        ResultType value =
            BinaryAttributeExpression<BinaryOperatorType, LeftArgType, RightArgType, ResultType>::evaluateAndReturn(
                matchDoc);
        this->setValue(value);
        return true;
    }

    ResultType evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        assert(_leftExpr);
        assert(_rightExpr);
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        ResultType resultValue;
        if (!this->tryFetchValue(matchDoc, resultValue)) {
            LeftArgType leftValue = _leftExpr->evaluateAndReturn(matchDoc);
            RightArgType rightValue = _rightExpr->evaluateAndReturn(matchDoc);
            resultValue = _binaryOperator(leftValue, rightValue);
            this->storeValue(matchDoc, resultValue);
        }
        return resultValue;
    }

    ExpressionType getExpressionType() const override { return ET_BINARY; }

    void setEvaluated() override {
        assert(_leftExpr);
        assert(_rightExpr);
        AttributeExpressionTyped<ResultType>::setEvaluated();
        _leftExpr->setEvaluated();
        _rightExpr->setEvaluated();
    }

    bool operator==(const AttributeExpression *checkExpr) const override {
        assert(checkExpr);
        if (typeid(*checkExpr) != typeid(*this)) {
            return false;
        }
        const BinaryAttributeExpression<BinaryOperatorType, LeftArgType, RightArgType> *expr =
            dynamic_cast<const BinaryAttributeExpression<BinaryOperatorType, LeftArgType, RightArgType> *>(checkExpr);

        return (expr && (*_leftExpr) == expr->_leftExpr && (*_rightExpr) == expr->_rightExpr);
    }

private:
    LeftAttrExpr *_leftExpr;
    RightAttrExpr *_rightExpr;
    BinaryOperatorType<LeftArgType> _binaryOperator;
};

//////////////////////////////////////////////////////////////////////
// optimize for logical_and, logical_or.
template <>
inline bool BinaryAttributeExpression<std::logical_and, bool>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(_leftExpr);
    assert(_rightExpr);
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);

    bool resultValue;
    if (this->tryFetchValue(matchDoc, resultValue)) {
        return resultValue;
    }

    if (!_leftExpr->evaluateAndReturn(matchDoc)) {
        this->storeValue(matchDoc, false);
        return false;
    }

    resultValue = _rightExpr->evaluateAndReturn(matchDoc);
    this->storeValue(matchDoc, resultValue);
    return resultValue;
};

template <>
inline bool BinaryAttributeExpression<std::logical_or, bool>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(_leftExpr);
    assert(_rightExpr);
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);

    bool resultValue;
    if (this->tryFetchValue(matchDoc, resultValue)) {
        return resultValue;
    }

    if (_leftExpr->evaluateAndReturn(matchDoc)) {
        this->storeValue(matchDoc, true);
        return true;
    }

    resultValue = _rightExpr->evaluateAndReturn(matchDoc);
    this->storeValue(matchDoc, resultValue);
    return resultValue;
}

template <>
inline void BinaryAttributeExpression<std::logical_or, bool>::setEvaluated() {
    AttributeExpressionTyped<bool>::setEvaluated();
    _leftExpr->setEvaluated();
}

template <>
inline void BinaryAttributeExpression<std::logical_and, bool>::setEvaluated() {
    AttributeExpressionTyped<bool>::setEvaluated();
    _leftExpr->setEvaluated();
}

} // namespace turing
} // namespace suez
