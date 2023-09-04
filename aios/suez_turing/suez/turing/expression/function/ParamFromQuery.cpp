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
#include "suez/turing/expression/function/ParamFromQuery.h"

#include <stdint.h>

#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace suez {
namespace turing {

template <typename T>
ParamFromQuery<T>::ParamFromQuery(const std::string &key, const std::string &defaultStr)
    : _key(key), _defaultStr(defaultStr) {}

template <typename T>
ParamFromQuery<T>::~ParamFromQuery() {}

template <typename T>
bool ParamFromQuery<T>::beginRequest(FunctionProvider *functionProvider) {
    assert(functionProvider);
    _pool = functionProvider->getPool();
    KeyValueMap kvPairs = functionProvider->getKVMap();
    std::string valueString;
    auto itr = kvPairs.find(_key);
    if (itr != kvPairs.end()) {
        valueString = itr->second;
    }
    if (valueString.empty()) {
        valueString = _defaultStr;
    }
    if (valueString.empty()) {
        AUTIL_LOG(ERROR,
                  "get param for key %s from query error, both param from query and defaultStr not exist",
                  _key.c_str());
        return false;
    }

    if constexpr (TypeHaInfoTraits<T>::IS_MULTI) {
        std::vector<std::string> valueStrVec = autil::StringUtil::split(valueString, std::string(","));
        std::vector<SingleValueType> tmpValueVec;
        tmpValueVec.reserve(valueStrVec.size());
        for (size_t i = 0; i < valueStrVec.size(); ++i) {
            SingleValueType tmpValue;
            if (autil::StringUtil::fromString<SingleValueType>(valueStrVec[i], tmpValue)) {
                tmpValueVec.push_back(tmpValue);
            } else {
                AUTIL_LOG(ERROR, "param_from_query failed. %s can not convert", valueStrVec[i].c_str());
                return false;
            }
        }

        if constexpr (std::is_same_v<T, autil::MultiString>) {
            _value.init(autil::MultiValueCreator::createMultiStringBuffer(tmpValueVec, _pool));
        } else {
            _value.init(autil::MultiValueCreator::createMultiValueBuffer<SingleValueType>(tmpValueVec, _pool));
        }
        return true;
    } else {
        SingleValueType tmpValue;
        bool ret = autil::StringUtil::fromString<SingleValueType>(valueString, tmpValue);
        if (ret == false) {
            AUTIL_LOG(ERROR, "param_from_query failed. %s can not convert", valueString.c_str());
            return false;
        }
        if constexpr (std::is_same_v<T, autil::MultiChar>) {
            _value.init(autil::MultiValueCreator::createMultiValueBuffer(tmpValue.c_str(), tmpValue.size(), _pool));
        } else {
            _value = tmpValue;
        }
        return true;
    }
}

template <typename T>
T ParamFromQuery<T>::evaluate(matchdoc::MatchDoc matchdoc) {
    return _value;
}

AUTIL_LOG_SETUP_TEMPLATE(function, ParamFromQuery, T);

/**********************************************************************************************************/
template <typename T>
ParamFromQueryCreatorImpl<T>::ParamFromQueryCreatorImpl(const std::string &funcName) : _funcName(funcName) {}

template <typename T>
ParamFromQueryCreatorImpl<T>::~ParamFromQueryCreatorImpl(){};

template <typename T>
FunctionProtoInfo ParamFromQueryCreatorImpl<T>::getFuncProtoInfo() const {
    return FunctionProtoInfo(
        FUNCTION_UNLIMITED_ARGUMENT_COUNT, TypeHaInfoTraits<T>::VARIABLE_TYPE, TypeHaInfoTraits<T>::IS_MULTI);
}

template <typename T>
std::string ParamFromQueryCreatorImpl<T>::getFuncName() const {
    return _funcName;
}

template <typename T>
FunctionInterface *ParamFromQueryCreatorImpl<T>::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    size_t argCount = funcSubExprVec.size();
    std::string key;
    std::string defaultStr;

    // 该function输入参数为string，所以其originalString类似于"abc"或'xyz'
    // 后续希望直接使用abc、xyz处理，用substr对首尾的引号进行去除
    if (argCount == 1) {
        key = funcSubExprVec[0]->getOriginalString();
        key = key.substr(1, key.size() - 2);
    } else if (argCount == 2) {
        key = funcSubExprVec[0]->getOriginalString();
        key = key.substr(1, key.size() - 2);
        defaultStr = funcSubExprVec[1]->getOriginalString();
        defaultStr = defaultStr.substr(1, defaultStr.size() - 2);
    } else {
        AUTIL_LOG(ERROR, "function %s need 1 or 2 input, but get %lu", _funcName.c_str(), argCount);
        return nullptr;
    }
    if (key.empty()) {
        AUTIL_LOG(ERROR, "function %s need key param, but get empty", _funcName.c_str());
        return nullptr;
    }
    VariableType vt = TypeHaInfoTraits<T>::VARIABLE_TYPE;
#define CREATE_PARAM_FROM_QUERY_FUNCTION(vt_type)                                                                      \
    case vt_type: {                                                                                                    \
        return new ParamFromQuery<T>(key, defaultStr);                                                                 \
    }
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_STRING(CREATE_PARAM_FROM_QUERY_FUNCTION);
    default:
        AUTIL_LOG(ERROR, "function %s not support type[%s]", _funcName.c_str(), vt2TypeName(vt).c_str());
        return nullptr;
    }
#undef CREATE_PARAM_FROM_QUERY_FUNCTION

    return nullptr;
}
AUTIL_LOG_SETUP_TEMPLATE(function, ParamFromQueryCreatorImpl, T);

// 模板显式实例化，为了声明、实现分离
#define PARAM_FROM_QUERY_TEMPLATE_INSTANTIATION(type, FuncName) template class ParamFromQueryCreatorImpl<type>;

PARAM_FROM_QUERY_MACRO_HELPER(PARAM_FROM_QUERY_TEMPLATE_INSTANTIATION);
} // namespace turing
} // namespace suez
