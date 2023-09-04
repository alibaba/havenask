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

#include <vector>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace suez {
namespace turing {
class AttributeExpression;
class FunctionProvider;

typedef std::vector<AttributeExpression *> FunctionSubExprVec;

class FunctionInterface {
public:
    FunctionInterface() {}
    virtual ~FunctionInterface() {}

public:
    virtual bool beginRequest(FunctionProvider *provider) = 0;
    virtual void endRequest() = 0;
    virtual void destroy() = 0;
    /*
     * set all definitely evaluated expression to evaluated
     */
    virtual void setExpressionEvaluated() = 0;

private:
    FunctionInterface(const FunctionInterface &);
    FunctionInterface &operator=(const FunctionInterface &);
};

template <typename T>
class FunctionInterfaceTyped : public FunctionInterface {
public:
    FunctionInterfaceTyped() {}
    virtual ~FunctionInterfaceTyped() {}

public:
    virtual T evaluate(matchdoc::MatchDoc matchDoc) = 0;

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    void endRequest() override {}
    void destroy() override { delete this; }
    void setExpressionEvaluated() override {
        // do nothing by default.
    }

public:
    static VariableType getType() { return TypeHaInfoTraits<T>::VARIABLE_TYPE; }

    static bool isMultiValue() { return TypeHaInfoTraits<T>::IS_MULTI; }

private:
    FunctionInterfaceTyped(const FunctionInterfaceTyped &);
    FunctionInterfaceTyped &operator=(const FunctionInterfaceTyped &);
};

SUEZ_TYPEDEF_PTR(FunctionInterface);

} // namespace turing
} // namespace suez
