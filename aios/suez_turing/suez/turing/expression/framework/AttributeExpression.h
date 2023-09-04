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
#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <tr1/type_traits>
#include <vector>

#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h" // //IWYU pragma: keep
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"

namespace matchdoc {
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {

enum ExpressionType {
    ET_UNKNOWN = 0,
    ET_ATOMIC,
    ET_COMBO,
    ET_ARGUMENT,
    ET_RANK,
    ET_BINARY,
    ET_FUNCTION,
    ET_CONST,
    ET_TUPLE,
    ET_OPTIMIZE
};

class AttributeExpression {
public:
    AttributeExpression(VariableType vt = vt_unknown, bool isMulti = false)
        : _isMultiValue(isMulti)
        , _isEvaluated(false)
        , _isSubExpression(false)
        , _isDeterministic(true)
        , _vt(vt)
        , _refId(INVALID_REFERENCE_ID) {}
    virtual ~AttributeExpression() {}

public:
    virtual bool evaluate(matchdoc::MatchDoc matchDoc) = 0;
    virtual bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) = 0;
    virtual bool allocate(matchdoc::MatchDocAllocator *allocator) = 0;
    virtual matchdoc::ReferenceBase *getReferenceBase() const = 0;
    virtual ExpressionType getExpressionType() const { return ET_UNKNOWN; }
    virtual bool operator==(const AttributeExpression *checkExpr) const {
        assert(false);
        return false;
    }
    virtual void setEvaluated() = 0;
    virtual uint64_t evaluateHash(matchdoc::MatchDoc matchDoc) = 0;
    template <typename T>
    bool getConstValue(T &value) const {
        if (ET_ARGUMENT != getExpressionType()) {
            return false;
        }

        return autil::StringUtil::numberFromString<T>(getOriginalString(), value);
    }

public:
    bool isEvaluated() const { return _isEvaluated; }
    VariableType getType() const { return _vt; }
    bool isMultiValue() const { return _isMultiValue; }
    const std::string &getOriginalString() const { return _originalString; }
    void setOriginalString(const std::string &originalString) { _originalString = originalString; }
    bool canLazyEvaluate() const { return _refId != INVALID_REFERENCE_ID; }
    void setIsSubExpression(bool f) { _isSubExpression = f; }
    bool isSubExpression() const { return _isSubExpression; }

    void andIsDeterministic(bool f) { _isDeterministic &= f; }
    bool isDeterministic() const { return _isDeterministic; }

public:
    // for test
    void setExpressionId(uint32_t id) {
        assert(!_isSubExpression);
        _refId = id;
    }

protected:
    bool _isMultiValue;
    bool _isEvaluated;
    bool _isSubExpression;
    bool _isDeterministic;
    VariableType _vt;
    uint32_t _refId;
    std::string _originalString;
};

template <>
bool AttributeExpression::getConstValue<std::string>(std::string &value) const;

typedef std::shared_ptr<AttributeExpression> AttributeExpressionPtr;
typedef std::vector<AttributeExpression *> ExpressionVector;
typedef std::set<AttributeExpression *> ExpressionSet;

template <typename T>
class AttributeExpressionTyped : public AttributeExpression {
public:
    AttributeExpressionTyped()
        : AttributeExpression(TypeHaInfoTraits<T>::VARIABLE_TYPE, TypeHaInfoTraits<T>::IS_MULTI)
        , _ref(NULL)
        , _value(T()) {}
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        if (_ref) {
            _value = _ref->get(matchDoc);
        }
        return true;
    }
    virtual T evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
        if (_ref) {
            _value = _ref->get(matchDoc);
        }
        return _value;
    }

    uint64_t evaluateHash(matchdoc::MatchDoc matchDoc) override {
        assert(false);
        return 0;
    }

    bool allocate(matchdoc::MatchDocAllocator *allocator) override {
        if (_ref != NULL) {
            return true;
        }
        if (!_isSubExpression) {
            _ref = allocator->declareWithConstructFlagDefaultGroup<T>(
                _originalString, matchdoc::ConstructTypeTraits<T>::NeedConstruct());
        } else {
            _ref = allocator->declareSubWithConstructFlagDefaultGroup<T>(
                _originalString, matchdoc::ConstructTypeTraits<T>::NeedConstruct());
        }
        return _ref != NULL;
    }
    matchdoc::ReferenceBase *getReferenceBase() const override {
        assert(_ref);
        return _ref;
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override {
        return defaultBatchEvaluate(matchDocs, matchDocCount);
    }
    void setEvaluated() override { _isEvaluated = (_ref != NULL); }
    virtual T getValue(matchdoc::MatchDoc matchDoc) const {
        if (_ref) {
            return _ref->get(matchDoc);
        } else {
            return _value;
        }
    }
    T getValue() const { return _value; }
    virtual matchdoc::Reference<T> *getReference() const { return _ref; }
    void setReference(matchdoc::Reference<T> *ref) { _ref = ref; }

protected:
    bool defaultBatchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
        auto reference = getReference();
        if (!reference) {
            return false;
        }
        if (isEvaluated()) {
            return true;
        }
        for (size_t i = 0; i < matchDocCount; i++) {
            evaluateAndReturn(matchDocs[i]);
        }
        return true;
    }

    inline void setValue(const T &value) { _value = value; }
    inline void storeValue(matchdoc::MatchDoc matchDoc, const T &value) __attribute__((always_inline)) {
        if (_ref) {
            _ref->set(matchDoc, value);
        }
    }
    inline bool tryFetchValue(matchdoc::MatchDoc matchDoc, T &value) __attribute__((always_inline)) {
        if (this->isEvaluated()) {
            assert(_ref != NULL);
            value = _ref->get(matchDoc);
            return true;
        }
        return false;
    }

protected:
    matchdoc::Reference<T> *_ref;
    T _value;
};

extern std::ostream &operator<<(std::ostream &os, const AttributeExpression &expr);
extern std::ostream &operator<<(std::ostream &os, const AttributeExpression *expr);

} // namespace turing
} // namespace suez
