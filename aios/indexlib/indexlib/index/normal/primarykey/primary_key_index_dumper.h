#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H

#include <tr1/memory>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyIndexDumper
{
public:
    PrimaryKeyIndexDumper(
        const config::PrimaryKeyIndexConfigPtr& pkIndexConfig,
        autil::mem_pool::PoolBase* pool)
        : mPkIndexConfig(pkIndexConfig)
        , mPool(pool)
    {}
    
    ~PrimaryKeyIndexDumper() {}

private:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::tr1::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef std::tr1::shared_ptr<PrimaryKeyFormatter<Key> > PrimaryKeyFormatterPtr;
    
public:
    void DumpHashMap(const HashMapTypedPtr& hashMap,
                     size_t docCount,
                     const file_system::FileWriterPtr& file) const;

    static int64_t EstimateDumpTempMemoryUse(
            const config::PrimaryKeyIndexConfigPtr& pkIndexConfig,
            size_t docCount)
    {
        if (pkIndexConfig->GetPrimaryKeyIndexType() == pk_hash_table)
        {
            return docCount * PrimaryKeyHashTable<Key>::EstimateMemoryCostPerDoc();
        }
        else
        {
            return docCount * sizeof(PKPair<Key>);
        }
    }

private:
    PrimaryKeyFormatterPtr CreatePKFormatter() const;
    
private:
    config::PrimaryKeyIndexConfigPtr mPkIndexConfig;
    autil::mem_pool::PoolBase* mPool;

private:
    IE_LOG_DECLARE();
};

template <typename Key>
inline void PrimaryKeyIndexDumper<Key>::DumpHashMap(
    const HashMapTypedPtr& hashMap,
    size_t docCount,
    const file_system::FileWriterPtr& file) const
{
    PrimaryKeyFormatterPtr pkFormatter = CreatePKFormatter();
    pkFormatter->SerializeToFile(hashMap, docCount, mPool, file);
}

template <typename Key>
typename PrimaryKeyIndexDumper<Key>::PrimaryKeyFormatterPtr
PrimaryKeyIndexDumper<Key>::CreatePKFormatter() const
{
    PrimaryKeyFormatterPtr pkFormatter;
    PrimaryKeyIndexType pkIndexType = mPkIndexConfig->GetPrimaryKeyIndexType();
    switch (pkIndexType)
    {
    case pk_hash_table:
        pkFormatter.reset(new HashPrimaryKeyFormatter<Key>());
        break;
    case pk_sort_array:
        pkFormatter.reset(new SortedPrimaryKeyFormatter<Key>());
        break;
    default:
        assert(false);
    }
    return pkFormatter;
}

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_PRIMARY_KEY_INDEX_DUMPER_H
