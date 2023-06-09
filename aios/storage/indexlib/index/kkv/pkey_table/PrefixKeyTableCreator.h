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

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTable.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"

namespace indexlibv2::index {

template <typename ValueType>
class PrefixKeyTableCreator
{
public:
    PrefixKeyTableCreator() = delete;
    ~PrefixKeyTableCreator() = delete;

public:
    static PrefixKeyTableBase<ValueType>* Create(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                                 const indexlib::config::KKVIndexPreference& indexPreference,
                                                 PKeyTableOpenType openType, uint32_t estimatePkeyCount,
                                                 size_t maxMemoryUseInBytes)
    {
        std::string hashTypeStr = indexPreference.GetHashDictParam().GetHashType();
        int32_t occupancyPct = indexPreference.GetHashDictParam().GetOccupancyPct();
        PKeyTableType type = GetPrefixKeyTableType(hashTypeStr);

        switch (type) {
        case PKeyTableType::DENSE: {
            return CreateClosedHashPKeyTable<PKeyTableType::DENSE>(directory, openType, estimatePkeyCount,
                                                                   occupancyPct);
        }
        case PKeyTableType::CUCKOO: {
            if (openType == PKeyTableOpenType::RW) {
                return CreateClosedHashPKeyTable<PKeyTableType::DENSE>(directory, openType, estimatePkeyCount,
                                                                       std::min(80, occupancyPct));
            } else {
                return CreateClosedHashPKeyTable<PKeyTableType::CUCKOO>(directory, openType, estimatePkeyCount,
                                                                        occupancyPct);
            }
        }
        case PKeyTableType::SEPARATE_CHAIN:
            return CreateSeparateChainPKeyTable(directory, openType, estimatePkeyCount, maxMemoryUseInBytes);
        case PKeyTableType::ARRAY:
            INDEXLIB_THROW(indexlib::util::UnSupportedException, "not support [array] type prefix key table yet!");
        default:
            assert(false);
        }
        return nullptr;
    }

private:
    template <PKeyTableType Type>
    static ClosedHashPrefixKeyTable<ValueType, Type>*
    CreateClosedHashPKeyTable(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                              PKeyTableOpenType openType, uint32_t estimatePkeyCount, int32_t occupancyPct)
    {
        std::unique_ptr<ClosedHashPrefixKeyTable<ValueType, Type>> tablePtr;
        switch (openType) {
        case PKeyTableOpenType::READ: {
            tablePtr.reset(new ClosedHashPrefixKeyTable<ValueType, Type>(occupancyPct));
            break;
        }
        case PKeyTableOpenType::RW:
        case PKeyTableOpenType::WRITE: {
            size_t maxMemUse = ClosedHashPrefixKeyTable<ValueType, Type>::CalculateMemoryUse(
                openType, estimatePkeyCount, occupancyPct);
            tablePtr.reset(new ClosedHashPrefixKeyTable<ValueType, Type>(maxMemUse, occupancyPct));
            break;
        }
        default:
            assert(false);
            return nullptr;
        }
        if (!tablePtr->Open(directory, openType)) {
            INDEXLIB_THROW(indexlib::util::IndexCollapsedException, "open kkv prefix key table in [%s] fail!",
                           directory->DebugString().c_str());
        }
        return tablePtr.release();
    }

    static SeparateChainPrefixKeyTable<ValueType>*
    CreateSeparateChainPKeyTable(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                 PKeyTableOpenType openType, uint32_t estimatePkeyCount, size_t maxMemoryUseInBytes)
    {
        std::unique_ptr<SeparateChainPrefixKeyTable<ValueType>> tablePtr;
        switch (openType) {
        case PKeyTableOpenType::READ: {
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>());
            break;
        }
        case PKeyTableOpenType::RW: {
            uint32_t bucketCount = SeparateChainPrefixKeyTable<ValueType>::GetRecommendBucketCount(estimatePkeyCount);
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>(bucketCount, maxMemoryUseInBytes));
            break;
        }
        case PKeyTableOpenType::WRITE: {
            uint32_t bucketCount = SeparateChainPrefixKeyTable<ValueType>::GetRecommendBucketCount(estimatePkeyCount);
            size_t maxMemUse = SeparateChainPrefixKeyTable<ValueType>::GetDumpSize(estimatePkeyCount, bucketCount);
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>(bucketCount, maxMemUse));
            break;
        }
        default:
            assert(false);
            return nullptr;
        }
        tablePtr->Open(directory, openType);
        return tablePtr.release();
    }
};

} // namespace indexlibv2::index
