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
#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H

#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/primarykey/block_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyIndexDumper
{
public:
    PrimaryKeyIndexDumper(const config::PrimaryKeyIndexConfigPtr& pkIndexConfig, autil::mem_pool::PoolBase* pool)
        : mPkIndexConfig(pkIndexConfig)
        , mPool(pool)
    {
    }

    ~PrimaryKeyIndexDumper() {}

private:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef std::shared_ptr<PrimaryKeyFormatter<Key>> PrimaryKeyFormatterPtr;

public:
    void DumpHashMap(const HashMapTypedPtr& hashMap, size_t docCount, const file_system::FileWriterPtr& file) const;

    static int64_t EstimateDumpTempMemoryUse(const config::PrimaryKeyIndexConfigPtr& pkIndexConfig, size_t docCount)
    {
        // TODO 封装不行，这里默认dumper知道formatter具体格式
        switch (pkIndexConfig->GetPrimaryKeyIndexType()) {
        case pk_hash_table:
            return docCount * PrimaryKeyHashTable<Key>::EstimateMemoryCostPerDoc();
        case pk_sort_array:
        case pk_block_array:
            // for block_array, index meta is so small. Memory is equal to data size;
            return docCount * sizeof(PKPair<Key>);
        default:
            assert(false);
            return 0;
        }
    }

private:
    config::PrimaryKeyIndexConfigPtr mPkIndexConfig;
    autil::mem_pool::PoolBase* mPool;

private:
    IE_LOG_DECLARE();
};

template <typename Key>
inline void PrimaryKeyIndexDumper<Key>::DumpHashMap(const HashMapTypedPtr& hashMap, size_t docCount,
                                                    const file_system::FileWriterPtr& file) const
{
    PrimaryKeyFormatterPtr pkFormatter = PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByIndexType(mPkIndexConfig);
    pkFormatter->SerializeToFile(hashMap, docCount, mPool, file);
}
}}     // namespace indexlib::index
#endif //__INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H
