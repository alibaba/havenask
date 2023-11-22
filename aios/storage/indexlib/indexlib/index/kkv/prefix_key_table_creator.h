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

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType>
class PrefixKeyTableCreator
{
public:
    PrefixKeyTableCreator() {}
    ~PrefixKeyTableCreator() {}

public:
    static PrefixKeyTableBase<ValueType>*
    Create(const file_system::DirectoryPtr& directory, const config::KKVIndexPreference& indexPreference,
           PKeyTableOpenType openType, uint32_t estimatePkeyCount = 0,
           const config::IndexPartitionOptions& options = config::IndexPartitionOptions())
    {
        std::string hashTypeStr = indexPreference.GetHashDictParam().GetHashType();
        int32_t occupancyPct = indexPreference.GetHashDictParam().GetOccupancyPct();
        PKeyTableType type = GetPrefixKeyTableType(hashTypeStr);

        switch (type) {
        case PKT_DENSE: {
            return CreateClosedHashPKeyTable<PKT_DENSE>(directory, openType, estimatePkeyCount, occupancyPct);
        }
        case PKT_CUCKOO: {
            if (openType == PKOT_RW) {
                return CreateClosedHashPKeyTable<PKT_DENSE>(directory, openType, estimatePkeyCount,
                                                            std::min(80, occupancyPct));
            } else {
                return CreateClosedHashPKeyTable<PKT_CUCKOO>(directory, openType, estimatePkeyCount, occupancyPct);
            }
        }
        case PKT_SEPARATE_CHAIN:
            return CreateSeparateChainPKeyTable(directory, options, openType, estimatePkeyCount);
        case PKT_ARRAY:
            INDEXLIB_THROW(util::UnSupportedException, "not support [array] type prefix key table yet!");
        default:
            assert(false);
        }
        return NULL;
    }

private:
    template <PKeyTableType Type>
    static ClosedHashPrefixKeyTable<ValueType, Type>*
    CreateClosedHashPKeyTable(const file_system::DirectoryPtr& directory, PKeyTableOpenType openType,
                              uint32_t estimatePkeyCount, int32_t occupancyPct)
    {
        std::unique_ptr<ClosedHashPrefixKeyTable<ValueType, Type>> tablePtr;
        switch (openType) {
        case PKOT_READ: {
            tablePtr.reset(new ClosedHashPrefixKeyTable<ValueType, Type>(occupancyPct));
            break;
        }
        case PKOT_RW:
        case PKOT_WRITE: {
            size_t maxMemUse = ClosedHashPrefixKeyTable<ValueType, Type>::CalculateMemoryUse(
                openType, estimatePkeyCount, occupancyPct);
            tablePtr.reset(new ClosedHashPrefixKeyTable<ValueType, Type>(maxMemUse, occupancyPct));
            break;
        }
        default:
            assert(false);
            return NULL;
        }
        if (!tablePtr->Open(directory, openType)) {
            INDEXLIB_THROW(util::IndexCollapsedException, "open kkv prefix key table in [%s] fail!",
                           directory->DebugString().c_str());
        }
        return tablePtr.release();
    }

    static SeparateChainPrefixKeyTable<ValueType>*
    CreateSeparateChainPKeyTable(const file_system::DirectoryPtr& directory,
                                 const config::IndexPartitionOptions& options, PKeyTableOpenType openType,
                                 uint32_t estimatePkeyCount)
    {
        std::unique_ptr<SeparateChainPrefixKeyTable<ValueType>> tablePtr;
        switch (openType) {
        case PKOT_READ: {
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>());
            break;
        }
        case PKOT_RW: {
            uint32_t bucketCount = SeparateChainPrefixKeyTable<ValueType>::GetRecommendBucketCount(estimatePkeyCount);
            size_t maxMemUse = options.GetBuildConfig().buildTotalMemory * 1024 * 1024;
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>(bucketCount, maxMemUse));
            break;
        }
        case PKOT_WRITE: {
            uint32_t bucketCount = SeparateChainPrefixKeyTable<ValueType>::GetRecommendBucketCount(estimatePkeyCount);
            size_t maxMemUse = SeparateChainPrefixKeyTable<ValueType>::GetDumpSize(estimatePkeyCount, bucketCount);
            tablePtr.reset(new SeparateChainPrefixKeyTable<ValueType>(bucketCount, maxMemUse));
            break;
        }
        default:
            assert(false);
            return NULL;
        }
        tablePtr->Open(directory, openType);
        return tablePtr.release();
    }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
