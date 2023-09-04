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

#include <set>

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"

namespace suez {
namespace turing {
class FunctionProvider;
template <typename T>
class AttributeExpressionTyped;

template <typename T>
class SingleHaInFuncInterface : public FunctionInterfaceTyped<bool> {
public:
    typedef typename VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, false>::AttrItemType SingleValueType;

public:
    SingleHaInFuncInterface(const FunctionSubExprVec &exprs, bool existFlag)
        : _exprs(exprs), _attrExpr(NULL), _set(NULL), _existFlag(existFlag) {}

    ~SingleHaInFuncInterface() { DELETE_AND_SET_NULL(_set); }

public:
    bool beginRequest(FunctionProvider *provider) override;
    bool evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    FunctionSubExprVec _exprs;
    AttributeExpressionTyped<T> *_attrExpr;
    std::set<SingleValueType> *_set;
    bool _existFlag;
};

template <typename T>
class MultiHaInFuncInterface : public FunctionInterfaceTyped<bool> {
public:
    typedef typename VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, false>::AttrItemType SingleValueType;

public:
    MultiHaInFuncInterface(const FunctionSubExprVec &exprs, bool existFlag)
        : _exprs(exprs), _attrExpr(NULL), _set(NULL), _existFlag(existFlag) {}

    ~MultiHaInFuncInterface() { DELETE_AND_SET_NULL(_set); }

public:
    bool beginRequest(FunctionProvider *provider) override;
    bool evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    FunctionSubExprVec _exprs;
    AttributeExpressionTyped<T> *_attrExpr;
    std::set<SingleValueType> *_set;
    bool _existFlag;
};
////////////////////////////////////////////////////////////

DECLARE_TEMPLATE_FUNCTION_CREATOR(HaInFuncInterface, bool, "ha_in", -1);
DECLARE_TEMPLATE_FUNCTION_CREATOR(NotHaInFuncInterface, bool, "not_ha_in", -1);

class TypedHaInFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag);
    template <typename T1, typename T2>
    static bool processFuncArg(const FunctionSubExprVec &funcSubExprVec,
                               AttributeExpressionTyped<T1> *&attrExpr,
                               std::set<T2> *&valueSet);

private:
    AUTIL_LOG_DECLARE();
};

class HaInFuncInterfaceCreatorImpl : public HaInFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class NotHaInFuncInterfaceCreatorImpl : public NotHaInFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

} // namespace turing
} // namespace suez
