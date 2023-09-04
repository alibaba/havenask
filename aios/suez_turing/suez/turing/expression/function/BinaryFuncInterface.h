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
class AttributeExpression;
template <typename T>
class AttributeExpressionTyped;

template <typename T>
class PowFuncInterface : public FunctionInterfaceTyped<float> {
public:
    PowFuncInterface(AttributeExpressionTyped<T> *attrExpr1, AttributeExpressionTyped<T> *attrExpr2, T lhs, T rhs);
    ~PowFuncInterface();

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    float evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    AttributeExpressionTyped<T> *_attrExpr1;
    AttributeExpressionTyped<T> *_attrExpr2;
    T _lhs;
    T _rhs;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
class LogFuncInterface : public FunctionInterfaceTyped<float> {
public:
    LogFuncInterface(AttributeExpressionTyped<T> *attrExpr1, AttributeExpressionTyped<T> *attrExpr2, T lhs, T rhs);
    ~LogFuncInterface();

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    float evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    AttributeExpressionTyped<T> *_attrExpr1;
    AttributeExpressionTyped<T> *_attrExpr2;
    T _lhs;
    T _rhs;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////

DECLARE_TEMPLATE_FUNCTION_CREATOR(PowFuncInterface, float, "pow", 2);
DECLARE_TEMPLATE_FUNCTION_CREATOR(LogFuncInterface, float, "log", 2);

class TypedBinaryFuncCreator {
public:
    template <template <typename TT> class InterfaceType>
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec);

private:
    template <typename T, template <typename TT> class InterfaceType>
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec);

private:
    AUTIL_LOG_DECLARE();
};

class PowFuncInterfaceCreatorImpl : public PowFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class LogFuncInterfaceCreatorImpl : public LogFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

AUTIL_LOG_SETUP_TEMPLATE(search, PowFuncInterface, T);
AUTIL_LOG_SETUP_TEMPLATE(search, LogFuncInterface, T);

} // namespace turing
} // namespace suez
