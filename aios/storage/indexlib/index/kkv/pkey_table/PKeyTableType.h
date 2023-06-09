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

#include <indexlib/util/Exception.h>
#include <string>

namespace indexlibv2::index {

enum class PKeyTableType {
    SEPARATE_CHAIN,
    DENSE,
    CUCKOO,
    ARRAY,
    UNKNOWN,
};

static constexpr const char KKV_PKEY_DENSE_TABLE_NAME[] = "dense";
static constexpr const char KKV_PKEY_CUCKOO_TABLE_NAME[] = "cuckoo";
static constexpr const char KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME[] = "separate_chain";
static constexpr const char KKV_PKEY_ARRAR_TABLE_NAME[] = "array";

inline PKeyTableType GetPrefixKeyTableType(const std::string& typeStr)
{
    if (typeStr == KKV_PKEY_DENSE_TABLE_NAME) {
        return PKeyTableType::DENSE;
    }
    if (typeStr == KKV_PKEY_CUCKOO_TABLE_NAME) {
        return PKeyTableType::CUCKOO;
    }
    if (typeStr == KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME) {
        return PKeyTableType::SEPARATE_CHAIN;
    }
    if (typeStr == KKV_PKEY_ARRAR_TABLE_NAME) {
        return PKeyTableType::ARRAY;
    }
    INDEXLIB_THROW(indexlib::util::UnSupportedException, "unsupported hash type:[%s]", typeStr.c_str());
    return PKeyTableType::UNKNOWN;
}

inline std::string PrefixKeyTableType2Str(PKeyTableType type)
{
    switch (type) {
    case PKeyTableType::DENSE:
        return KKV_PKEY_DENSE_TABLE_NAME;
    case PKeyTableType::CUCKOO:
        return KKV_PKEY_CUCKOO_TABLE_NAME;
    case PKeyTableType::SEPARATE_CHAIN:
        return KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME;
    case PKeyTableType::ARRAY:
        return KKV_PKEY_ARRAR_TABLE_NAME;
    default:
        INDEXLIB_THROW(indexlib::util::UnSupportedException, "unsupported hash type:[%d]", static_cast<int>(type));
    }
}

} // namespace indexlibv2::index
