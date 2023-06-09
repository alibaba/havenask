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
#include "expression/function/InFuncInterface.h"
#include "expression/function/ArgumentAttributeExpression.h"
#include "autil/StringUtil.h"
#include "autil/StringTokenizer.h"

using namespace std;
using namespace autil;

namespace expression {

AUTIL_LOG_SETUP(expression, TypedInFuncCreator);

#define DEFINE_MULTI_TYPE_SPECIALIZATION(MultiType) \
    template <>                                                         \
    bool InFuncInterface<MultiType>::evaluate(const matchdoc::MatchDoc &matchDoc) \
    {                                                                   \
        const MultiType &value = _attrExpr->getValue(matchDoc);         \
        bool isExist = false;                                           \
        uint32_t valueCount = value.size();                             \
        for (uint32_t i = 0; i < valueCount; ++i)                       \
        {                                                               \
            if (_set.find(value[i]) != _set.end())                      \
            {                                                           \
                isExist = true;                                         \
                break;                                                  \
            }                                                           \
        }                                                               \
        return isExist == _existFlag;                                   \
    }                                                                   \
    template<>                                                          \
    void InFuncInterface<MultiType>::batchEvaluate(matchdoc::MatchDoc *matchDocs, \
            uint32_t docCount)                                          \
    {                                                                   \
        assert(_ref);                                                   \
        for (uint32_t i = 0; i < docCount; i++) {                       \
            const matchdoc::MatchDoc& matchDoc = matchDocs[i];          \
            const MultiType &value = _attrExpr->getValue(matchDoc);     \
            bool isExist = false;                                       \
            uint32_t valueCount = value.size();                         \
            for (uint32_t i = 0; i < valueCount; ++i)                   \
            {                                                           \
                if (_set.find(value[i]) != _set.end())                  \
                {                                                       \
                    isExist = true;                                     \
                    break;                                              \
                }                                                       \
            }                                                           \
            _ref->set(matchDoc, isExist == _existFlag);                 \
        }                                                               \
    }

DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt8);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt16);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt32);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiInt64);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt8);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt16);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt32);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiUInt64);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiFloat);
DEFINE_MULTI_TYPE_SPECIALIZATION(autil::MultiDouble);



template<typename T>
FunctionInterface *TypedInFuncCreator::createTypedFunction(
        const AttributeExpressionVec& funcSubExprVec, bool existFlag)
{
    AttributeExpressionTyped<T> *attrExpr = 
        dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.",
                funcSubExprVec[0]->getExprString().c_str());
        return NULL;
    }
    ArgumentAttributeExpression *arg =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[1]);
    if (!arg) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.",
                funcSubExprVec[1]->getExprString().c_str());
        return NULL;
    }
    string setStr;
    if (!arg->getConstValue<string>(setStr)) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.",
                funcSubExprVec[1]->getExprString().c_str());
        return NULL;
    }

    StringTokenizer st(setStr, "|",
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    set<typename BasicTypeTraits<T>::BasicType> tempSet;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        typename BasicTypeTraits<T>::BasicType value;
        if (!StringUtil::fromString(st[i], value)) {
            AUTIL_LOG(WARN, "invalid parameter for in function: %s.", st[i].c_str());
            return NULL;
        }
        tempSet.insert(value);
    }
    return new InFuncInterface<T>(attrExpr, tempSet, existFlag);
}

FunctionInterface *TypedInFuncCreator::createTypedFunction(
        const AttributeExpressionVec& funcSubExprVec, bool existFlag)
{
    if (funcSubExprVec.size() != 2) {
        AUTIL_LOG(WARN, "function can't accept more than one arg.");
        return NULL;
    }
#define CREATE_TYPED_FUNCTION(vt_type, isMulti)                                 \
    case vt_type:                                                       \
    {                                                                   \
        if (isMulti)                                                    \
        {                                                               \
            typedef ExprValueType2AttrValueType<vt_type, true>::AttrValueType T; \
            return createTypedFunction<T>(funcSubExprVec, existFlag);   \
        } else                                                          \
        {                                                               \
            typedef ExprValueType2AttrValueType<vt_type, false>::AttrValueType T; \
            return createTypedFunction<T>(funcSubExprVec, existFlag);   \
        }                                                               \
    }

    ExprValueType evt = funcSubExprVec[0]->getExprValueType();
    bool isMulti = funcSubExprVec[0]->isMulti();
    switch(evt) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER_WITH_ARGS(CREATE_TYPED_FUNCTION, isMulti);
    default:
        AUTIL_LOG(WARN, "function can't accept %d type arg.",
                  int(evt));
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION
    return NULL;
}

bool InFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *InFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    return TypedInFuncCreator::createTypedFunction(funcSubExprVec, true);
}

bool NotInFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *NotInFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    return TypedInFuncCreator::createTypedFunction(funcSubExprVec, false);
}

}
