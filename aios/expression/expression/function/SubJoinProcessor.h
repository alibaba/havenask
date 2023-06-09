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
#ifndef ISEARCH_EXPRESSION_SUBJOINPROCESSOR_H
#define ISEARCH_EXPRESSION_SUBJOINPROCESSOR_H

#include "matchdoc/MatchDoc.h"
#include "autil/MultiValueCreator.h"
#include "autil/mem_pool/Pool.h"
#include "expression/common.h"
#include "expression/framework/AttributeExpressionTyped.h"

namespace expression {

class ValueHelper {
public:
    template <typename T>
    static inline std::string toString(const T &value) {
        return autil::StringUtil::toString(value);
    }

    template <typename T>
    static inline std::string toString(const autil::MultiValueType<T> &value) {
        std::stringstream ss;
        for (size_t i = 0; i < value.size(); ++i) {
            if (i != 0) { ss << " "; }
            ss << value[i];
        }
        return ss.str();
    }
};


template<typename T>
class SubJoinProcessor {
public:
    SubJoinProcessor(AttributeExpressionTyped<T> *expr,
                     const std::string& split = std::string())
        : _expr(expr)
        , _split(split)
        , _pool()
    {}

public:
    void begin() {
        _value.clear();
    }

    void operator()(matchdoc::MatchDoc matchDoc) {
        if (!_value.empty()) {
            _value += _split;
        }
        _value += ValueHelper::toString(_expr->getValue(matchDoc));
    }

    autil::MultiChar getResult() {
        char *buffer = autil::MultiValueCreator::createMultiValueBuffer(
                _value.data(), _value.size(), &_pool);
        return autil::MultiChar(buffer);
    }

private:
    AttributeExpressionTyped<T> *_expr;
    std::string _split;
    std::string _value;
    autil::mem_pool::Pool _pool;
};

class SubJoinVarProcessor {
public:
    enum ValueRefType {
        vrt_single,
        vrt_multi_uint8,
        vrt_multi_uint16,
        vrt_multi_uint32,
        vrt_multi_uint64,
        vrt_multi_int8,
        vrt_multi_int16,
        vrt_multi_int32,
        vrt_multi_int64,
        vrt_multi_float,
        vrt_multi_double,
        vrt_multi_char,
        vrt_multi_string,
    };

public:
    SubJoinVarProcessor(
            matchdoc::ReferenceBase *ref,
            const std::string& split = std::string())
        : _ref(ref)
        , _refType(vrt_single)
        , _split(split)
        , _pool()
    {
        _refType = getRefType(ref);
    }

public:
    void begin() {
        _value.clear();
    }

    void operator()(matchdoc::MatchDoc matchDoc) {
        if (!_value.empty()) {
            _value += _split;
        }
        _value += getStringValue(matchDoc);
    }

    autil::MultiChar getResult() {
        char *buffer = autil::MultiValueCreator::createMultiValueBuffer(
                _value.data(), _value.size(), &_pool);
        return autil::MultiChar(buffer);
    }

private:
    ValueRefType getRefType(matchdoc::ReferenceBase *ref);
    std::string getStringValue(matchdoc::MatchDoc matchDoc);

private:
    matchdoc::ReferenceBase *_ref;
    ValueRefType _refType;
    std::string _split;
    std::string _value;
    autil::mem_pool::Pool _pool;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_SUBJOINPROCESSOR_H
