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
#include <bit>
#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

namespace suez {
namespace turing {

// UnsignedTypeTraits将整形转为对应位长的unsigned整形
template <typename T>
struct UnsignedTypeTraits {
    static_assert(std::is_integral_v<T>);
    typedef T UnsignedType;
};
#define HAMMING_UNSIGNED_TYPE_TRAITS(type, utype)                                                                      \
    template <>                                                                                                        \
    struct UnsignedTypeTraits<type> {                                                                                  \
        typedef utype UnsignedType;                                                                                    \
    };

HAMMING_UNSIGNED_TYPE_TRAITS(int8_t, uint8_t);
HAMMING_UNSIGNED_TYPE_TRAITS(int16_t, uint16_t);
HAMMING_UNSIGNED_TYPE_TRAITS(int32_t, uint32_t);
HAMMING_UNSIGNED_TYPE_TRAITS(int64_t, uint64_t);
#undef HAMMING_UNSIGNED_TYPE_TRAITS

// T1表示第一个参数的类型, T2表示第二个参数类型, R表示返回值类型
template <typename T1, typename T2, typename R>
class HammingFuncInterface : public FunctionInterfaceTyped<R> {
public:
    static_assert(std::is_same_v<R, uint16_t> || std::is_same_v<R, autil::MultiUInt16>);

public:
    HammingFuncInterface(AttributeExpressionTyped<T1> *expr1, AttributeExpressionTyped<T2> *expr2);
    ~HammingFuncInterface();

public:
    bool beginRequest(FunctionProvider *functionProvider) override;
    R evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    AttributeExpressionTyped<T1> *_expr1 = nullptr;
    AttributeExpressionTyped<T2> *_expr2 = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

class HammingFuncInterfaceCreator : public FunctionCreator {
public:
    FunctionProtoInfo getFuncProtoInfo() const { return FunctionProtoInfo(2, vt_hamming_type, false); }
    std::string getFuncName() const { return "hamming"; }
};

class HammingFuncInterfaceCreatorImpl : public HammingFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
