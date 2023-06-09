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
#ifndef ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONTYPED_H
#define ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONTYPED_H

#include "autil/Log.h"
#include "expression/framework/AttributeExpression.h"
#include "matchdoc/Reference.h"

namespace expression {

template <typename T>
class AttributeExpressionTyped : public AttributeExpression
{
public:
    AttributeExpressionTyped(ExpressionType et,
                             ExprValueType evt = EVT_UNKNOWN,
                             bool isMulti = false,
                             const std::string &exprStr = "")
        : AttributeExpression(et, evt, isMulti, exprStr)
        , _ref(NULL)
        , _value(T())
    {}
    virtual ~AttributeExpressionTyped() {}
private:
    AttributeExpressionTyped(const AttributeExpressionTyped &);
    AttributeExpressionTyped& operator=(const AttributeExpressionTyped &);
public:
    bool allocate(matchdoc::MatchDocAllocator *allocator,
                                 const std::string &groupName = matchdoc::DEFAULT_GROUP,
                                 uint8_t serializeLevel = SL_PROXY) override
    {
        if (this->isSubExpression()) {
            this->_ref = allocator->declareSub<T>(this->getExprString(),
                    groupName, serializeLevel);
        } else {
            this->_ref = allocator->declare<T>(this->getExprString(),
                    groupName, serializeLevel);
        }
        return this->_ref != NULL;
    }
    void reset() override {
        _value = T();
        _ref = NULL;
    }
    matchdoc::ReferenceBase* getReferenceBase() const override {
        return _ref;
    }
public:
    T getValue(matchdoc::MatchDoc matchDoc) const __attribute__((always_inline)) {
        if(unlikely(_ref == NULL)) {
            return _value;
        }
        return _ref->get(matchDoc);
    }
    
    inline void storeValue(const matchdoc::MatchDoc &matchDoc, const T &value) {
        assert(_ref);
        _ref->set(matchDoc, value);
    }

    // for test
    void setReference(matchdoc::Reference<T> *ref)
    { _ref = ref; }

protected:
    matchdoc::Reference<T> *_ref;
    T _value; // only for ConstAttributeExpression
private:
    AUTIL_LOG_DECLARE();
};

template <>
class AttributeExpressionTyped<void> : public AttributeExpression
{
public:
    AttributeExpressionTyped(ExpressionType et,
                             ExprValueType evt = EVT_UNKNOWN,
                             bool isMulti = false,
                             const std::string &exprStr = "")
        : AttributeExpression(et, evt, isMulti, exprStr)
    {}
    virtual ~AttributeExpressionTyped() {}
private:
    AttributeExpressionTyped(const AttributeExpressionTyped &);
    AttributeExpressionTyped& operator=(const AttributeExpressionTyped &);
public:
    bool allocate(matchdoc::MatchDocAllocator *allocator,
                                 const std::string &groupName = matchdoc::DEFAULT_GROUP,
                                 uint8_t serializeLevel = SL_PROXY) override
    {
        return true;
    }
    void reset()  override{};
    matchdoc::ReferenceBase* getReferenceBase() const  override{
        return NULL;
    }
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONTYPED_H
