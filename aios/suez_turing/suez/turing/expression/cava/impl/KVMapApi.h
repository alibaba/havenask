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

#include <stddef.h>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "cava/common/Common.h"
#include "cava/runtime/CString.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/common.h"

class CavaCtx;
template <typename T>
class CavaArrayType;

namespace ha3 {

class KVMapApi {
public:
    KVMapApi(const suez::turing::KeyValueMap &kvs) : _kvs(kvs) {}
    ~KVMapApi() {}

private:
    KVMapApi(const KVMapApi &);
    KVMapApi &operator=(const KVMapApi &);

public:
#define getStringToArrayDeclare(type, name)                                                                            \
    CavaArrayType<type> *get##name##Array(CavaCtx *ctx, cava::lang::CString *key, cava::lang::CString *separator);

    getStringToArrayDeclare(double, Double) getStringToArrayDeclare(float, Float) getStringToArrayDeclare(long, Long)
        getStringToArrayDeclare(int, Int) getStringToArrayDeclare(short, Short)
            getStringToArrayDeclare(cava::byte, Byte)

                double getDouble(CavaCtx *ctx, cava::lang::CString *key);
    long getLong(CavaCtx *ctx, cava::lang::CString *key);
    cava::lang::CString *getValue(CavaCtx *ctx, cava::lang::CString *key);

    template <class T>
    CavaArrayType<T> *stringToArray(CavaCtx *ctx, cava::lang::CString *key, cava::lang::CString *separator);

private:
    const std::string *getValue(cava::lang::CString *key);

private:
    const suez::turing::KeyValueMap &_kvs;
};

template <class T>
CavaArrayType<T> *KVMapApi::stringToArray(CavaCtx *ctx, cava::lang::CString *key, cava::lang::CString *separator) {
    auto str = getValue(key);
    if (str == NULL || separator == NULL) {
        return NULL;
    }
    std::vector<T> vals;
    autil::StringUtil::fromString<T>(*str, vals, separator->getStr());
    CavaArrayType<T> *arr = suez::turing::SuezCavaAllocUtil::allocCavaArray<T>(ctx, vals.size());
    if (arr == NULL) {
        return NULL;
    }
    T *data = arr->getData();
    for (size_t i = 0; i < vals.size(); i++) {
        data[i] = vals[i];
    }
    return arr;
}

} // namespace ha3
