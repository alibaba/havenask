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
#ifndef ISEARCH_EXPRESSION_INFUNCEXPRESSION_H_
#define ISEARCH_EXPRESSION_INFUNCEXPRESSION_H_

#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionCreator.h"
#include "matchdoc/MatchDoc.h"

namespace expression {

template<typename T>
class InFuncInterface : public FunctionInterfaceTyped<bool>
{
public:
    typedef typename BasicTypeTraits<T>::BasicType ValueType;
public:
    InFuncInterface(AttributeExpressionTyped<T> *attrExpr,
                    std::set<ValueType> &set,
                    bool existFlag);
    ~InFuncInterface();
public:
    /* override */ bool evaluate(const matchdoc::MatchDoc &matchDoc);
    /* override */ void batchEvaluate(
            matchdoc::MatchDoc *matchDocs, uint32_t docCount);
private:
    AttributeExpressionTyped<T> *_attrExpr;
    std::set<ValueType> _set;
    bool _existFlag;
private:
    AUTIL_LOG_DECLARE();
};
////////////////////////////////////////////////////////////

class InFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(bool, "in", 2,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
};
class NotInFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(bool, "notin", 2,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
};

class TypedInFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const AttributeExpressionVec& funcSubExprVec, bool existFlag);
private:
    template<typename T>
    static FunctionInterface *createTypedFunction(const AttributeExpressionVec& funcSubExprVec, bool existFlag);
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
InFuncInterface<T>::InFuncInterface(AttributeExpressionTyped<T> *attrExpr,
                                    std::set<ValueType> &set, bool existFlag)
    : _attrExpr(attrExpr)
    , _existFlag(existFlag)
{
    _set.swap(set);
}

template<typename T>
InFuncInterface<T>::~InFuncInterface() {
}

template<typename T>
bool InFuncInterface<T>::evaluate(const matchdoc::MatchDoc &matchDoc) {
    const T &value = _attrExpr->getValue(matchDoc);
    bool isExist = (_set.find(value) != _set.end());
    return isExist == _existFlag;
}

template<typename T>
void InFuncInterface<T>::batchEvaluate(matchdoc::MatchDoc *matchDocs, 
                                       uint32_t docCount) 
{
    assert(_ref);
    for (uint32_t i = 0; i < docCount; i++) {
        const matchdoc::MatchDoc& matchDoc = matchDocs[i];
        const T &value = _attrExpr->getValue(matchDoc);
        bool isExist = (_set.find(value) != _set.end());
        _ref->set(matchDoc, isExist == _existFlag);
    }
}

#define DECLARE_MULTI_TYPE_SPECIALIZATION(MultiType) \
    template <>                                                         \
    bool InFuncInterface<MultiType>::evaluate(const matchdoc::MatchDoc &matchDoc); \
    template<>                                                          \
    void InFuncInterface<MultiType>::batchEvaluate(matchdoc::MatchDoc *matchDocs, \
            uint32_t docCount) 

DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt8);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt16);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt32);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt64);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt8);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt16);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt32);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt64);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiFloat);
DECLARE_MULTI_TYPE_SPECIALIZATION(autil::MultiDouble);

}

#endif //ISEARCH_EXPRESSION_INFUNCEXPRESSION_H
