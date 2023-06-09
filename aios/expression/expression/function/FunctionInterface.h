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
#ifndef ISEARCH_FUNCTIONINTERFACE_H
#define ISEARCH_FUNCTIONINTERFACE_H

#include "expression/common.h"
#include "matchdoc/MatchDoc.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/TypeTraits.h"
#include "expression/function/FunctionProvider.h"

namespace expression {

class FunctionInterface
{
public:
    FunctionInterface() 
        : _isSubScope(false)
    {}

    virtual ~FunctionInterface() {}
public:
    virtual bool beginRequest(FunctionProvider *provider) { return true; }
    virtual void endRequest() {}

    virtual void destroy() { delete this; }

    void setSubScope(bool isSubScope) { _isSubScope = isSubScope; }
    bool isSubScope() const { return _isSubScope; }

private:
    FunctionInterface(const FunctionInterface &);
    FunctionInterface& operator=(const FunctionInterface &);

protected:
    bool _isSubScope;
};

template<typename T>
class FunctionInterfaceTyped : public FunctionInterface
{
public:
    FunctionInterfaceTyped()
        : _ref(NULL)
    {}

    virtual ~FunctionInterfaceTyped() {}
public:
    virtual T evaluate(const matchdoc::MatchDoc &matchDoc) = 0;
    virtual void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
        assert(false);
        // for (uint32_t i = 0; i < docCount; i++) {
        //     (void)evaluate(matchDocs[i]);
        // }
    }

public:
    static ExprValueType getType() {
        return AttrValueType2ExprValueType<T>::evt;
    }

    static bool isMultiValue() {
        return AttrValueType2ExprValueType<T>::isMulti;
    }

    void setReference(matchdoc::Reference<T>* ref) {
        _ref = ref;
    }

private:
    FunctionInterfaceTyped(const FunctionInterfaceTyped &);
    FunctionInterfaceTyped& operator=(const FunctionInterfaceTyped &);

protected:
    matchdoc::Reference<T>* _ref;
};

}

#endif //ISEARCH_FUNCTIONINTERFACE_H
