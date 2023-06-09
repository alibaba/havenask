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
#ifndef ISEARCH_EXPRESSION_BASICSUBPROCESSORWRAPPER_H
#define ISEARCH_EXPRESSION_BASICSUBPROCESSORWRAPPER_H

#include "expression/common.h"

namespace expression {

template<typename T, typename Accumulator>
class SubAccProcessor {
public:
    typedef AttributeExpressionTyped<T> AttrExpr;

public:
    SubAccProcessor(AttrExpr *attrExpr, T initValue)
        : _attrExpr(attrExpr)
        , _initValue(initValue)
        , _value(initValue)
    { }
public:
    void begin() {
        _value = _initValue;
        _acc.begin();
    }
    
    void operator()(matchdoc::MatchDoc matchDoc) {
        _value = _acc(_value, _attrExpr->getValue(matchDoc));
    }
    T getResult() const {
        return _value;
    }
private:
    AttrExpr *_attrExpr;
    T _initValue;
    T _value;
    Accumulator _acc;
};

template<typename T>
struct FirstProcessorTypeWrapper {
    struct FirstWrapper {
        inline void begin() {
            _hasFirst = false;
        }
        inline const T& operator()(const T& a, const T& b) {
            if (!_hasFirst) {
                _first  = b;
                _hasFirst = true;
            }
            return _first;
        }
        T _first;
        bool _hasFirst;
    };
    typedef SubAccProcessor<T, FirstWrapper> type;
    static const T INIT_VALUE;
};

template<typename T>
struct MinProcessorTypeWrapper {
    struct MinWrapper {
        inline void begin() const {
        }
        inline const T& operator()(const T& a, const T& b) const {
            return std::min(a, b);
        }
    };
    typedef SubAccProcessor<T, MinWrapper> type;
    static const T INIT_VALUE;
};

template<typename T>
struct MaxProcessorTypeWrapper {
    struct MaxWrapper {
        inline void begin() const {
        }
        inline const T& operator()(const T& a, const T& b) const {
            return std::max(a, b);
        }
    };

    typedef SubAccProcessor<T, MaxWrapper> type;
    static const T INIT_VALUE;
};

template<typename T>
struct SumProcessorTypeWrapper {
    struct SumWrapper {
        inline void begin() const {
        }
        inline T operator()(const T& a, const T& b) const {
            return a + b;
        }
    };

    typedef SubAccProcessor<T, SumWrapper> type;
    static const T INIT_VALUE;
};

template<typename T>
const T MinProcessorTypeWrapper<T>::INIT_VALUE = std::numeric_limits<T>::max();

template<typename T>
const T FirstProcessorTypeWrapper<T>::INIT_VALUE = T();

template<typename T>
const T MaxProcessorTypeWrapper<T>::INIT_VALUE = std::numeric_limits<T>::min();

template<typename T>
const T SumProcessorTypeWrapper<T>::INIT_VALUE = 0;


class CounterProcessor {
public:
    CounterProcessor()
        : _subSlabCount(0)
    {}
public:
    inline void begin() {
        _subSlabCount = 0;
    }
    void operator()(matchdoc::MatchDoc matchDoc) {
        ++_subSlabCount;
    }
    bool isEvaluateSuccess() const {
        return true;
    }
    uint32_t getResult() const {
        return _subSlabCount;
    }
private:
    uint32_t _subSlabCount;
};
typedef std::shared_ptr<CounterProcessor> CounterProcessorPtr;

}

#endif //ISEARCH_EXPRESSION_BASICSUBPROCESSORWRAPPER_H
