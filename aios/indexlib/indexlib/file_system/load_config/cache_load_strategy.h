#ifndef __INDEXLIB_CACHE_LOAD_STRATEGY_H
#define __INDEXLIB_CACHE_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class CacheLoadStrategy : public LoadStrategy
{
public:
    CacheLoadStrategy();
    CacheLoadStrategy(bool useDirectIO, bool useGlobalCache, bool cacheDecompressFile); // for global cache
    CacheLoadStrategy(size_t cacheSize, size_t blockSize,
                      bool useDirectIO, bool useGlobalCache,
                      bool cacheDecompressFile);
    ~CacheLoadStrategy();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    size_t GetCacheSize() const { return mCacheSize; }
    size_t GetBlockSize() const { return mBlockSize; }
    bool UseDirectIO() const { return mUseDirectIO; }
    bool UseGlobalCache() const { return mUseGlobalCache; }
    bool CacheDecompressFile() const { return mCacheDecompressFile; }
    const std::string& GetLoadStrategyName() const
    { return mUseGlobalCache ? READ_MODE_GLOBAL_CACHE : READ_MODE_CACHE; }

    void Check();
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const;
    bool operator==(const CacheLoadStrategy& loadStrategy) const;

private:
    static const size_t DEFAULT_CACHE_SIZE_MB;
    static const size_t DEFAULT_CACHE_BLOCK_SIZE;

private:
    size_t mCacheSize;
    size_t mBlockSize;
    bool mUseDirectIO;
    bool mUseGlobalCache;
    bool mCacheDecompressFile;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CacheLoadStrategy);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_CACHE_LOAD_STRATEGY_H
