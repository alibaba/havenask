#ifndef __INDEXLIB_OFFLINE_CONFIG_BASE_H
#define __INDEXLIB_OFFLINE_CONFIG_BASE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/configurator_define.h"

IE_NAMESPACE_BEGIN(config);

struct OfflineReaderConfig : public autil::legacy::Jsonizable
{
    OfflineReaderConfig()
        : loadIndex(true)
        , lazyLoadAttribute(true)
        , useSearchCache(false)
        , useBlockCache(true)
        , indexCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
        , summaryCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
        , searchCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
    {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("use_block_cache", useBlockCache, useBlockCache);
        json.Jsonize("use_search_cache", useSearchCache, useSearchCache);
        json.Jsonize("index_block_cache_size", indexCacheSize, indexCacheSize);
        json.Jsonize("summary_block_cache_size", summaryCacheSize, summaryCacheSize);
        json.Jsonize("search_cache_size", searchCacheSize, searchCacheSize);
    }

    // attention: do not add new member to this class
    bool loadIndex;
    bool lazyLoadAttribute;
    bool useSearchCache;
    bool useBlockCache;
    int64_t indexCacheSize;
    int64_t summaryCacheSize;
    int64_t searchCacheSize;
};

class OfflineConfigBase : public autil::legacy::Jsonizable
{
public:
    OfflineConfigBase();
    OfflineConfigBase(const BuildConfig& buildConf,
                      const MergeConfig& mergeConf);

    ~OfflineConfigBase();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    // attention: do not add new member to this class
    // U can add new member to OfflineConfigImpl

    BuildConfig buildConfig;
    MergeConfig mergeConfig;
    OfflineReaderConfig readerConfig;
    bool enableRecoverIndex;
    RecoverStrategyType recoverType;
    bool fullIndexStoreKKVTs;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineConfigBase);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_OFFLINE_CONFIG_BASE_H
