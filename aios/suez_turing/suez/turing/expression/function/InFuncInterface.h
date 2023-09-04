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
#include <set>
#include <string>

#include "autil/CommonMacros.h"
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
class InFuncInterface : public FunctionInterfaceTyped<bool> {
public:
    InFuncInterface(AttributeExpressionTyped<T> *attrExpr, std::set<T> *set, bool existFlag);
    ~InFuncInterface();

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    bool evaluate(matchdoc::MatchDoc matchDoc) override;

public:
    // for test
    const std::set<T> &getSet() const { return *_set; }
    bool getInFlag() const { return _existFlag; }

private:
    AttributeExpressionTyped<T> *_attrExpr;
    std::set<T> *_set;
    bool _existFlag;

private:
    AUTIL_LOG_DECLARE();
};
////////////////////////////////////////////////////////////

DECLARE_TEMPLATE_FUNCTION_CREATOR(InFuncInterface, bool, "in", 2);
DECLARE_TEMPLATE_FUNCTION_CREATOR(NotInFuncInterface, bool, "notin", 2);

class TypedInFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag);

private:
    template <typename T>
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag);

private:
    AUTIL_LOG_DECLARE();
};

class InFuncInterfaceCreatorImpl : public InFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class NotInFuncInterfaceCreatorImpl : public NotInFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

AUTIL_LOG_SETUP_TEMPLATE(search, InFuncInterface, T);
///////////////////////////////////////////////////////////////

template <typename T>
InFuncInterface<T>::InFuncInterface(AttributeExpressionTyped<T> *attrExpr, std::set<T> *set, bool existFlag)
    : _attrExpr(attrExpr), _set(set), _existFlag(existFlag) {}

template <typename T>
InFuncInterface<T>::~InFuncInterface() {
    DELETE_AND_SET_NULL(_set);
}

template <typename T>
bool InFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    bool isExist = _set->find(_attrExpr->evaluateAndReturn(matchDoc)) != _set->end();
    return isExist == _existFlag;
}

} // namespace turing
} // namespace suez
