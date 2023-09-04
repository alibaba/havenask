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
#include <algorithm>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
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

// T表示提取param的类型
template <typename T>
class ParamFromQuery : public FunctionInterfaceTyped<T> {
public:
    // SingleValueType为T的单值数据类型
    typedef typename VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, false>::AttrItemType SingleValueType;

public:
    ParamFromQuery(const std::string &key, const std::string &defaultStr);
    ~ParamFromQuery();

public:
    bool beginRequest(FunctionProvider *functionProvider) override;
    T evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    std::string _key;
    std::string _defaultStr;
    T _value;
    autil::mem_pool::Pool *_pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
class ParamFromQueryCreatorImpl : public FunctionCreator {
public:
    ParamFromQueryCreatorImpl(const std::string &funcName);
    ~ParamFromQueryCreatorImpl();

public:
    FunctionProtoInfo getFuncProtoInfo() const;
    std::string getFuncName() const;
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec);

private:
    std::string _funcName;
    AUTIL_LOG_DECLARE();
};

#define PARAM_FROM_QUERY_REGISTE_FUNCTION(type, FuncName)                                                              \
    REGISTE_FUNCTION_CREATOR_WITH_FUNCNAME(ParamFromQueryCreatorImpl<type>, FuncName);

#define PARAM_FROM_QUERY_MACRO_HELPER(MY_MACRO)                                                                        \
    MY_MACRO(int8_t, "int8_param");                                                                                    \
    MY_MACRO(int16_t, "int16_param");                                                                                  \
    MY_MACRO(int32_t, "int32_param");                                                                                  \
    MY_MACRO(int64_t, "int64_param");                                                                                  \
    MY_MACRO(uint8_t, "uint8_param");                                                                                  \
    MY_MACRO(uint16_t, "uint16_param");                                                                                \
    MY_MACRO(uint32_t, "uint32_param");                                                                                \
    MY_MACRO(uint64_t, "uint64_param");                                                                                \
    MY_MACRO(float, "float_param");                                                                                    \
    MY_MACRO(double, "double_param");                                                                                  \
    MY_MACRO(autil::MultiChar, "string_param");                                                                        \
                                                                                                                       \
    MY_MACRO(autil::MultiInt8, "mint8_param");                                                                         \
    MY_MACRO(autil::MultiInt16, "mint16_param");                                                                       \
    MY_MACRO(autil::MultiInt32, "mint32_param");                                                                       \
    MY_MACRO(autil::MultiInt64, "mint64_param");                                                                       \
    MY_MACRO(autil::MultiUInt8, "muint8_param");                                                                       \
    MY_MACRO(autil::MultiUInt16, "muint16_param");                                                                     \
    MY_MACRO(autil::MultiUInt32, "muint32_param");                                                                     \
    MY_MACRO(autil::MultiUInt64, "muint64_param");                                                                     \
    MY_MACRO(autil::MultiFloat, "mfloat_param");                                                                       \
    MY_MACRO(autil::MultiDouble, "mdouble_param");                                                                     \
    MY_MACRO(autil::MultiString, "mstring_param");
} // namespace turing
} // namespace suez
