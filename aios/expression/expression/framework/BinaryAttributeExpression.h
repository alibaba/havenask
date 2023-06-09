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
#ifndef ISEARCH_EXPRESSION_BINARYATTRIBUTEEXPRESSION_H
#define ISEARCH_EXPRESSION_BINARYATTRIBUTEEXPRESSION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/TypeInfo.h"
#include "expression/framework/TypeTraits.h"
#include "autil/MultiValueType.h"

namespace expression {

template<typename T>
struct divides : std::binary_function <T, T, T> {
    T operator() (T lhs, T rhs) const {
        if (rhs == 0) {
            return 0;
        }
        return lhs / rhs;
    }
};

template<>
inline autil::MultiChar divides<autil::MultiChar>::operator()(
        autil::MultiChar lhs,
        autil::MultiChar rhs) const
{
    assert(false);
    return autil::MultiChar();
}

template<typename T>
struct bit_and : std::binary_function<T, T, T> {
    T operator() (T lhs, T rhs) {
        return lhs & rhs;
    }
};

template<typename T>
struct bit_xor : std::binary_function<T, T, T> {
    T operator() (T lhs, T rhs) {
        return lhs ^ rhs;
    }
};

template<typename T>
struct bit_or : std::binary_function<T, T, T> {
    T operator() (T lhs, T rhs) {
        return lhs | rhs;
    }
};

#define SPECIAL_BIT_FLOAT_DOULE(opType)                                 \
    template<> struct opType<float> : std::binary_function<float, float, float> { \
        float operator()(float, float) {                                \
            assert(false);                                              \
            return 0;                                                   \
        }                                                               \
    };                                                                  \
    template<> struct opType<double> : std::binary_function<double, double, double> { \
        double operator()(double, double) {                             \
            assert(false);                                              \
            return 0;                                                   \
        }                                                               \
    }

SPECIAL_BIT_FLOAT_DOULE(bit_and);
SPECIAL_BIT_FLOAT_DOULE(bit_xor);
SPECIAL_BIT_FLOAT_DOULE(bit_or);

#undef SPECIAL_BIT_FLOAT_DOULE

/////////////////// multi_equal_to and multi_not_equal_to
template<typename T>
struct multi_equal_to : std::binary_function <T, autil::MultiValueType<T>, bool> {
    bool operator() (const T& x, const autil::MultiValueType<T>& y) const {
        for (uint32_t i = 0; i < y.size(); ++i) {
            if (x == y[i]) {
                return true;
            }
        }
        return false;
    }
};

template<typename T>
struct multi_not_equal_to : std::binary_function <T, autil::MultiValueType<T>, bool> {
    bool operator() (const T &x, const autil::MultiValueType<T> &y) const {
        multi_equal_to<T>  multiEqual;
        return !multiEqual(x, y);
    }
};

template<template<class T> class BinaryOperatorType,
         typename LeftArgType, typename RightArgType = LeftArgType,
         typename ResultType = typename BinaryOperatorType<LeftArgType>::result_type>
class BinaryAttributeExpression : public AttributeExpressionTyped<ResultType>
{
public:
    typedef AttributeExpressionTyped<LeftArgType> LeftAttrExpr;
    typedef AttributeExpressionTyped<RightArgType> RightAttrExpr;
public:
    BinaryAttributeExpression(
            const std::string &exprStr,
            AttributeExpression *leftExpr,
            AttributeExpression *rightExpr)
        : AttributeExpressionTyped<ResultType>(
                ET_BINARY,
                AttrValueType2ExprValueType<ResultType>::evt,
                AttrValueType2ExprValueType<ResultType>::isMulti,
                exprStr)
    {
        _leftExpr = dynamic_cast<LeftAttrExpr *>(leftExpr);
        _rightExpr = dynamic_cast<RightAttrExpr *>(rightExpr);
        assert(_leftExpr);
        assert(_rightExpr);
    }
    virtual ~BinaryAttributeExpression() {
        _leftExpr = NULL;
        _rightExpr = NULL;
    }
public:
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc) {
        innerEvaluate(matchDoc);
    }

    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
        for (uint32_t i = 0; i < docCount; i++) {
            innerEvaluate(matchDocs[i]);
        }
    }

    /* override */ matchdoc::ReferenceBase* getReferenceBase() const {
        return this->_ref;
    }

private:
    inline void innerEvaluate(const matchdoc::MatchDoc &matchDoc) __attribute__((always_inline)) {
        ResultType result = _binaryOperator(_leftExpr->getValue(matchDoc),
                _rightExpr->getValue(matchDoc));
        this->storeValue(matchDoc, result);
    }

private:
    LeftAttrExpr *_leftExpr;
    RightAttrExpr *_rightExpr;
    BinaryOperatorType<LeftArgType> _binaryOperator;
};

// optimize logical_and, logical_or
// TODO: uncomment later
// template<>
// inline void BinaryAttributeExpression<std::logical_and, bool>::evaluate(const matchdoc::MatchDoc &matchDoc) {
//     _leftExpr->evaluate(matchDoc);
//     bool left = _leftExpr->getValue();
//     if (!left) {
//         this->storeValue(matchDoc, false);
//         return;
//     }
//     _rightExpr->evaluate(matchDoc);
//     this->storeValue(matchDoc, _rightExpr->getValue());
// }

// template<>
// inline void BinaryAttributeExpression<std::logical_or, bool>::evaluate(const matchdoc::MatchDoc &matchDoc) {
//     _leftExpr->evaluate(matchDoc);
//     if (_leftExpr->getValue()) {
//         this->storeValue(matchDoc, true);
//         return;
//     }
//     _rightExpr->evaluate(matchDoc);
//     this->storeValue(matchDoc, _rightExpr->getValue());
// }

}

#endif //ISEARCH_EXPRESSION_BINARYATTRIBUTEEXPRESSION_H
