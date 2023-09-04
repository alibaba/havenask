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

#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

namespace suez {
namespace turing {
template <typename T>
class AttributeExpressionTyped;

template <typename T>
autil::MultiChar to_string(const T &x, autil::mem_pool::Pool *_pool) {
    std::string data = autil::StringUtil::toString(x);
    return autil::MultiChar(autil::MultiValueCreator::createMultiValueBuffer(data.c_str(), data.size(), _pool));
}

template <>
autil::MultiChar to_string<autil::MultiChar>(const autil::MultiChar &x, autil::mem_pool::Pool *_pool);

template <typename T>
class ToStringFuncInterface : public FunctionInterfaceTyped<autil::MultiChar> {
public:
    ToStringFuncInterface(AttributeExpressionTyped<T> *attrExpr) : _attrExpr(attrExpr), _pool(nullptr) {}
    ~ToStringFuncInterface() {}

public:
    bool beginRequest(FunctionProvider *provider) override {
        _pool = provider->getPool();
        return true;
    }
    autil::MultiChar evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    AttributeExpressionTyped<T> *_attrExpr;
    autil::mem_pool::Pool *_pool;

private:
    AUTIL_LOG_DECLARE();
};

DECLARE_TEMPLATE_FUNCTION_CREATOR(ToStringFuncInterface, autil::MultiChar, "to_string", 1);

class TypedFuncCreator {
public:
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec);

private:
    template <typename T>
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec);

    AUTIL_LOG_DECLARE();
};

class ToStringFuncInterfaceCreatorImpl : public ToStringFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
autil::MultiChar ToStringFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    T value = _attrExpr->evaluateAndReturn(matchDoc);
    return to_string(value, _pool);
}

////////////////////////////////////////////////////////////////////////////

class StringReplaceFuncInterface : public FunctionInterfaceTyped<std::string> {
public:
    StringReplaceFuncInterface(AttributeExpressionTyped<autil::MultiChar> *attrExpr,
                               const std::string &replaceStr,
                               const char newChar);
    ~StringReplaceFuncInterface();

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    std::string evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    AttributeExpressionTyped<autil::MultiChar> *_attrExpr;
    std::string _replaceStr;
    char _newChar;

private:
    AUTIL_LOG_DECLARE();
};
////////////////////////////////////////////////////////////

DECLARE_FUNCTION_CREATOR(StringReplaceFuncInterface, "string_replace", 3);

class StringReplaceFuncInterfaceCreatorImpl : public StringReplaceFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
