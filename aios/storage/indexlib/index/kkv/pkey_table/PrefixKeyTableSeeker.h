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

#include <memory>

#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTable.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::index {

class PrefixKeyTableSeeker
{
public:
    PrefixKeyTableSeeker() {}
    ~PrefixKeyTableSeeker() {}

public:
#define SEEK_FUNC(funcName)                                                                                            \
    template <typename ValueType>                                                                                      \
    static ValueType* funcName(PrefixKeyTableBase<ValueType>* table, uint64_t key)                                     \
    {                                                                                                                  \
        assert(table);                                                                                                 \
        PKeyTableType tableType = table->GetTableType();                                                               \
        switch (tableType) {                                                                                           \
        case PKeyTableType::SEPARATE_CHAIN: {                                                                          \
            using Table = SeparateChainPrefixKeyTable<ValueType>;                                                      \
            Table* scTable = static_cast<Table*>(table);                                                               \
            return scTable->funcName(key);                                                                             \
        }                                                                                                              \
        case PKeyTableType::DENSE: {                                                                                   \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::DENSE>;                                   \
            Table* dhTable = static_cast<Table*>(table);                                                               \
            return dhTable->funcName(key);                                                                             \
        }                                                                                                              \
        case PKeyTableType::CUCKOO: {                                                                                  \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::CUCKOO>;                                  \
            Table* chTable = static_cast<Table*>(table);                                                               \
            return chTable->funcName(key);                                                                             \
        }                                                                                                              \
        /* TODO: support */                                                                                            \
        case PKeyTableType::ARRAY:                                                                                     \
        default:                                                                                                       \
            assert(false);                                                                                             \
        }                                                                                                              \
        return nullptr;                                                                                                \
    }                                                                                                                  \
                                                                                                                       \
    template <typename ValueType>                                                                                      \
    static bool funcName(PrefixKeyTableBase<ValueType>* table, uint64_t key, ValueType& value)                         \
    {                                                                                                                  \
        assert(table);                                                                                                 \
        PKeyTableType tableType = table->GetTableType();                                                               \
        switch (tableType) {                                                                                           \
        case PKeyTableType::SEPARATE_CHAIN: {                                                                          \
            using SCTable = SeparateChainPrefixKeyTable<ValueType>;                                                    \
            SCTable* scTable = static_cast<SCTable*>(table);                                                           \
            ValueType* valuePtr = scTable->funcName(key);                                                              \
            if (valuePtr) {                                                                                            \
                value = *valuePtr;                                                                                     \
                return true;                                                                                           \
            }                                                                                                          \
            return false;                                                                                              \
        }                                                                                                              \
        case PKeyTableType::DENSE: {                                                                                   \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::DENSE>;                                   \
            Table* dhTable = static_cast<Table*>(table);                                                               \
            return dhTable->funcName(key, value);                                                                      \
        }                                                                                                              \
        case PKeyTableType::CUCKOO: {                                                                                  \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::CUCKOO>;                                  \
            Table* chTable = static_cast<Table*>(table);                                                               \
            return chTable->funcName(key, value);                                                                      \
        }                                                                                                              \
        /* TODO: support */                                                                                            \
        case PKeyTableType::ARRAY:                                                                                     \
        default:                                                                                                       \
            assert(false);                                                                                             \
        }                                                                                                              \
        return false;                                                                                                  \
    }                                                                                                                  \
                                                                                                                       \
    template <typename ValueType>                                                                                      \
    static bool funcName(PrefixKeyTableBase<ValueType>* table, uint64_t key, ValueType& value,                         \
                         KKVMetricsCollector* collector)                                                               \
    {                                                                                                                  \
        assert(table);                                                                                                 \
        PKeyTableType tableType = table->GetTableType();                                                               \
        switch (tableType) {                                                                                           \
        case PKeyTableType::SEPARATE_CHAIN: {                                                                          \
            using SCTable = SeparateChainPrefixKeyTable<ValueType>;                                                    \
            SCTable* scTable = static_cast<SCTable*>(table);                                                           \
            ValueType* valuePtr = scTable->funcName(key);                                                              \
            if (valuePtr) {                                                                                            \
                value = *valuePtr;                                                                                     \
                return true;                                                                                           \
            }                                                                                                          \
            return false;                                                                                              \
        }                                                                                                              \
        case PKeyTableType::DENSE: {                                                                                   \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::DENSE>;                                   \
            Table* dhTable = static_cast<Table*>(table);                                                               \
            return dhTable->funcName(key, value, collector);                                                           \
        }                                                                                                              \
        case PKeyTableType::CUCKOO: {                                                                                  \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::CUCKOO>;                                  \
            Table* chTable = static_cast<Table*>(table);                                                               \
            return chTable->funcName(key, value, collector);                                                           \
        }                                                                                                              \
        /* TODO: support */                                                                                            \
        case PKeyTableType::ARRAY:                                                                                     \
        default:                                                                                                       \
            assert(false);                                                                                             \
        }                                                                                                              \
        return false;                                                                                                  \
    }

    SEEK_FUNC(Find)
    SEEK_FUNC(FindForRW)

#undef SEEK_FUNC

#define ASYNC_SEEK_FUNC(funcName)                                                                                      \
    template <typename ValueType>                                                                                      \
    static FL_LAZY(bool)                                                                                               \
        funcName(PrefixKeyTableBase<ValueType>* table, uint64_t key, ValueType& value, KKVMetricsCollector* collector) \
    {                                                                                                                  \
        assert(table);                                                                                                 \
        PKeyTableType tableType = table->GetTableType();                                                               \
        switch (tableType) {                                                                                           \
        case PKeyTableType::SEPARATE_CHAIN: {                                                                          \
            using SCTable = SeparateChainPrefixKeyTable<ValueType>;                                                    \
            SCTable* scTable = static_cast<SCTable*>(table);                                                           \
            ValueType* valuePtr = scTable->funcName(key);                                                              \
            if (valuePtr) {                                                                                            \
                value = *valuePtr;                                                                                     \
                FL_CORETURN true;                                                                                      \
            }                                                                                                          \
            FL_CORETURN false;                                                                                         \
        }                                                                                                              \
        case PKeyTableType::DENSE: {                                                                                   \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::DENSE>;                                   \
            Table* dhTable = static_cast<Table*>(table);                                                               \
            FL_CORETURN FL_COAWAIT dhTable->funcName(key, value, collector);                                           \
            break;                                                                                                     \
        }                                                                                                              \
        case PKeyTableType::CUCKOO: {                                                                                  \
            using Table = ClosedHashPrefixKeyTable<ValueType, PKeyTableType::CUCKOO>;                                  \
            Table* chTable = static_cast<Table*>(table);                                                               \
            FL_CORETURN FL_COAWAIT chTable->funcName(key, value, collector);                                           \
            break;                                                                                                     \
        }                                                                                                              \
        /* TODO: support */                                                                                            \
        case PKeyTableType::ARRAY:                                                                                     \
        default:                                                                                                       \
            assert(false);                                                                                             \
        }                                                                                                              \
        FL_CORETURN false;                                                                                             \
    }

    ASYNC_SEEK_FUNC(FindForRead)
};

} // namespace indexlibv2::index
