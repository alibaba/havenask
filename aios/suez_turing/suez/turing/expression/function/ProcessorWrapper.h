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
#include <limits>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace suez {
namespace turing {
template <typename T>
class AttributeExpressionTyped;

struct value_helper {
    template <typename P>
    static inline P getValue(matchdoc::MatchDoc matchDoc, AttributeExpressionTyped<P> *expr) {
        return expr->evaluateAndReturn(matchDoc);
    }
    template <typename P>
    static inline const P &getValue(matchdoc::MatchDoc matchDoc, matchdoc::Reference<P> *ref) {
        return ref->getReference(matchDoc);
    }
};

template <typename T, typename ResultType, typename Accumulator, typename ValueRef>
class SubAccProcessor {
public:
    SubAccProcessor(ValueRef *valueRef, ResultType initValue, const std::string &param = std::string())
        : _valueRef(valueRef), _initValue(initValue), _value(initValue), _acc(param) {}

public:
    void setPool(autil::mem_pool::Pool *pool) {}
    void begin() {
        _value = _initValue;
        _acc.begin();
    }
    void operator()(matchdoc::MatchDoc matchDoc) { _value = _acc(_value, value_helper::getValue(matchDoc, _valueRef)); }
    ResultType getResult() const { return _value; }

private:
    ValueRef *_valueRef;
    ResultType _initValue;
    ResultType _value;
    std::string _param;
    Accumulator _acc;
};

class CounterProcessor {
public:
    typedef uint32_t result_type;

public:
    CounterProcessor(void * /*useless*/, uint32_t initValue) : _subSlabCount(initValue) {}

public:
    void setPool(autil::mem_pool::Pool *pool) {}
    inline void begin() { _subSlabCount = 0; }
    void operator()(matchdoc::MatchDoc matchDoc) { ++_subSlabCount; }
    bool isEvaluateSuccess() const { return true; }
    uint32_t getResult() const { return _subSlabCount; }

private:
    uint32_t _subSlabCount;
};

template <typename T, typename ValueRef>
struct FirstProcessorTypeWrapper {
    struct FirstWrapper {
        FirstWrapper(const std::string &) {}
        inline void begin() { _hasFirst = false; }
        inline const T &operator()(const T &a, const T &b) {
            if (!_hasFirst) {
                _first = b;
                _hasFirst = true;
            }
            return _first;
        }
        T _first;
        bool _hasFirst;
    };
    typedef T result_type;
    typedef SubAccProcessor<T, result_type, FirstWrapper, ValueRef> type;
    static const result_type INIT_VALUE;
};

template <typename T, typename ValueRef>
struct MinProcessorTypeWrapper {
    struct MinWrapper {
        MinWrapper(const std::string &) {}
        inline void begin() const {}
        inline const T &operator()(const T &a, const T &b) const { return std::min(a, b); }
    };
    typedef T result_type;
    typedef SubAccProcessor<T, result_type, MinWrapper, ValueRef> type;
    static const result_type INIT_VALUE;
};

template <typename T, typename ValueRef>
struct MaxProcessorTypeWrapper {
    struct MaxWrapper {
        MaxWrapper(const std::string &) {}
        inline void begin() const {}
        inline const T &operator()(const T &a, const T &b) const { return std::max(a, b); }
    };
    typedef T result_type;
    typedef SubAccProcessor<T, result_type, MaxWrapper, ValueRef> type;
    static const result_type INIT_VALUE;
};

template <typename T, typename ValueRef>
struct CountProcessorTypeWrapper {
    typedef CounterProcessor type;
    typedef uint32_t result_type;
    static const result_type INIT_VALUE;
};

template <typename T, typename ValueRef>
struct SumProcessorTypeWrapper {
    struct SumWrapper {
        SumWrapper(const std::string &) {}
        inline void begin() const {}
        inline T operator()(const T &a, const T &b) const { return a + b; }
    };
    typedef T result_type;
    typedef SubAccProcessor<T, result_type, SumWrapper, ValueRef> type;
    static const result_type INIT_VALUE;
};

template <typename T, typename Accumulator, typename ValueRef>
class JoinAttrProcessor {
private:
    typedef autil::MultiChar MultiChar;

public:
    JoinAttrProcessor(ValueRef *valueRef, const std::string &param = std::string())
        : _valueRef(valueRef), _acc(param), _pool(NULL) {}
    JoinAttrProcessor(const JoinAttrProcessor &other)
        : _valueRef(other._valueRef), _acc(other._acc), _pool(other._pool) {}
    void operator=(const JoinAttrProcessor &other) {
        _valueRef = other._valueRef;
        _acc = other._acc;
        _value.clear();
        _pool = other._pool;
    }

public:
    void setPool(autil::mem_pool::Pool *pool) { _pool = pool; }
    void begin() {
        _value.clear();
        _acc.begin();
    }
    void operator()(matchdoc::MatchDoc matchDoc) { _value = _acc(_value, value_helper::getValue(matchDoc, _valueRef)); }
    MultiChar getResult() {
        assert(_pool);
        char *buffer = autil::MultiValueCreator::createMultiValueBuffer(_value.data(), _value.size(), _pool);
        return MultiChar(buffer);
    }

private:
    ValueRef *_valueRef;
    Accumulator _acc;
    std::string _value;
    autil::mem_pool::Pool *_pool;
};

template <typename T, typename ValueRef>
struct JoinProcessorTypeWrapper {
    struct JoinWrapper {
    public:
        typedef autil::MultiChar MultiChar;

    public:
        JoinWrapper(const std::string &split) : _split(split) {}
        inline void begin() {}
        inline std::string operator()(const std::string &a, const T &b) {
            std::string result = a;
            if (!result.empty()) {
                result += _split;
            }
            const typename Type2AttrItemType<T>::AttrItemType &v = Type2AttrItemType<T>::convert(b);
            result += autil::StringUtil::toString(v);
            return result;
        }

    private:
        std::string _split;
    };
    typedef autil::MultiChar result_type;
    typedef JoinAttrProcessor<T, JoinWrapper, ValueRef> type;
};

class JoinVarProcessor {
public:
    JoinVarProcessor(matchdoc::ReferenceBase *valueRef, const std::string &split)
        : _valueRef(valueRef), _acc(split), _pool(NULL) {}
    JoinVarProcessor(const JoinVarProcessor &other)
        : _valueRef(other._valueRef), _acc(other._acc), _pool(other._pool) {}
    void operator=(const JoinVarProcessor &other) {
        _valueRef = other._valueRef;
        _acc = other._acc;
        _value.clear();
        _pool = other._pool;
    }

public:
    void setPool(autil::mem_pool::Pool *pool) { _pool = pool; }
    void begin() {
        _acc.begin();
        _value.clear();
    }
    void operator()(matchdoc::MatchDoc matchDoc) { _value = _acc(_value, _valueRef->toString(matchDoc)); }
    autil::MultiChar getResult() {
        assert(_pool);
        char *buffer = autil::MultiValueCreator::createMultiValueBuffer(_value.data(), _value.size(), _pool);
        return autil::MultiChar(buffer);
    }

private:
    matchdoc::ReferenceBase *_valueRef;
    JoinProcessorTypeWrapper<std::string, matchdoc::ReferenceBase>::JoinWrapper _acc;
    std::string _value;
    autil::mem_pool::Pool *_pool;
};

template <typename T, typename ValueRef>
const typename MinProcessorTypeWrapper<T, ValueRef>::result_type MinProcessorTypeWrapper<T, ValueRef>::INIT_VALUE =
    std::numeric_limits<typename MinProcessorTypeWrapper<T, ValueRef>::result_type>::max();

template <typename T, typename ValueRef>
const typename FirstProcessorTypeWrapper<T, ValueRef>::result_type
    FirstProcessorTypeWrapper<T, ValueRef>::INIT_VALUE = typename FirstProcessorTypeWrapper<T, ValueRef>::result_type();

template <typename T, typename ValueRef>
const typename MaxProcessorTypeWrapper<T, ValueRef>::result_type MaxProcessorTypeWrapper<T, ValueRef>::INIT_VALUE =
    std::numeric_limits<typename MaxProcessorTypeWrapper<T, ValueRef>::result_type>::min();

template <typename T, typename ValueRef>
const typename SumProcessorTypeWrapper<T, ValueRef>::result_type SumProcessorTypeWrapper<T, ValueRef>::INIT_VALUE = 0;

template <typename T, typename ValueRef>
const typename CountProcessorTypeWrapper<T, ValueRef>::result_type CountProcessorTypeWrapper<T, ValueRef>::INIT_VALUE =
    0;

} // namespace turing
} // namespace suez
