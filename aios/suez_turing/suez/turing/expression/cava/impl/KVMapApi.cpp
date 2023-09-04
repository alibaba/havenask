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
#include "suez/turing/expression/cava/impl/KVMapApi.h"

#include <cstddef>
#include <map>
#include <utility>

#include "autil/CommonMacros.h"

class CavaCtx;
template <typename T>
class CavaArrayType;

using namespace std;

namespace ha3 {

#define getStringToArrayImpl(type, name)                                                                               \
    CavaArrayType<type> *KVMapApi::get##name##Array(                                                                   \
        CavaCtx *ctx, cava::lang::CString *key, cava::lang::CString *separator) {                                      \
        return stringToArray<type>(ctx, key, separator);                                                               \
    }

getStringToArrayImpl(double, Double) getStringToArrayImpl(float, Float) getStringToArrayImpl(long, Long)
    getStringToArrayImpl(int, Int) getStringToArrayImpl(short, Short) getStringToArrayImpl(cava::byte, Byte)

        cava::lang::CString *KVMapApi::getValue(CavaCtx *ctx, cava::lang::CString *key) {
    const std::string *str = getValue(key);
    if (unlikely(str == NULL)) {
        return NULL;
    }
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, (std::string *)str);
}

const std::string *KVMapApi::getValue(cava::lang::CString *key) {
    if (unlikely(key == NULL)) {
        return NULL;
    }
    suez::turing::KeyValueMap::const_iterator it;
    const std::string &str = key->getStr();
    it = _kvs.find(str);
    if (it == _kvs.end()) {
        return NULL;
    }
    return (std::string *)&it->second;
}

double KVMapApi::getDouble(CavaCtx *ctx, cava::lang::CString *key) {
    auto str = getValue(key);
    if (str == NULL) {
        return 0.0;
    }
    return autil::StringUtil::strToDoubleWithDefault(str->c_str(), 0.0);
}

long KVMapApi::getLong(CavaCtx *ctx, cava::lang::CString *key) {
    auto str = getValue(key);
    if (str == NULL) {
        return 0;
    }
    return autil::StringUtil::strToInt64WithDefault(str->c_str(), 0);
}

} // namespace ha3
