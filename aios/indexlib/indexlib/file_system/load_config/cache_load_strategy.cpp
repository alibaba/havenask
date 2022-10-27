#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CacheLoadStrategy);

const size_t CacheLoadStrategy::DEFAULT_CACHE_SIZE_MB = 1;           // 1MB
const size_t CacheLoadStrategy::DEFAULT_CACHE_BLOCK_SIZE = 4 * 1024; // 4k

CacheLoadStrategy::CacheLoadStrategy()
    : mCacheSize(DEFAULT_CACHE_SIZE_MB * 1024 * 1024)
    , mBlockSize(DEFAULT_CACHE_BLOCK_SIZE)
    , mUseDirectIO(false)
    , mUseGlobalCache(false)
    , mCacheDecompressFile(false)
{
}

CacheLoadStrategy::CacheLoadStrategy(size_t cacheSize, size_t blockSize,
                                     bool useDirectIO, bool useGlobalCache,
                                     bool cacheDecompressFile)
    : mCacheSize(cacheSize)
    , mBlockSize(blockSize)
    , mUseDirectIO(useDirectIO)
    , mUseGlobalCache(useGlobalCache)
    , mCacheDecompressFile(cacheDecompressFile)
{
}

CacheLoadStrategy::CacheLoadStrategy(
    bool useDirectIO, bool useGlobalCache, bool cacheDecompressFile)
    : mUseDirectIO(useDirectIO)
    , mUseGlobalCache(useGlobalCache)
    , mCacheDecompressFile(cacheDecompressFile)
{
}

CacheLoadStrategy::~CacheLoadStrategy() 
{
}

void CacheLoadStrategy::Check()
{
    if (mBlockSize == 0)
    {
        INDEXLIB_THROW(misc::BadParameterException, "block_size can not be 0");
    }

    if (mCacheSize < mBlockSize)
    {
        INDEXLIB_THROW(misc::BadParameterException,
                       "cache size [%ld], block size [%ld]",
                       mCacheSize, mBlockSize);
    }

    if (mUseDirectIO && mBlockSize % 4096 != 0) {
        INDEXLIB_THROW(misc::BadParameterException, 
                       "cache block size should "
                       "be multiple of 4096 when use direct io");
    }
}

void CacheLoadStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    // TODO: default cache_size changed
    json.Jsonize("direct_io", mUseDirectIO, false);
    json.Jsonize("global_cache", mUseGlobalCache, false);
    json.Jsonize("cache_decompress_file", mCacheDecompressFile, false);

    if (!mUseGlobalCache)
    {
        if (json.GetMode() == FROM_JSON)
        {
            size_t cacheSizeMB = 0;
            json.Jsonize("cache_size", cacheSizeMB, DEFAULT_CACHE_SIZE_MB);
            mCacheSize = cacheSizeMB * 1024 * 1024;
        }
        else
        {
            size_t cacheSizeMB = mCacheSize / 1024 / 1024;
            json.Jsonize("cache_size", cacheSizeMB, DEFAULT_CACHE_SIZE_MB);
        }
        json.Jsonize("block_size", mBlockSize, DEFAULT_CACHE_BLOCK_SIZE);
    }
}

bool CacheLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName())
    {
        return false;
    }        
    CacheLoadStrategyPtr right = DYNAMIC_POINTER_CAST(
            CacheLoadStrategy, loadStrategy);        
    assert(right);

    return *this == *right;
}

bool CacheLoadStrategy::operator==(const CacheLoadStrategy& loadStrategy) const
{
    return mCacheSize == loadStrategy.mCacheSize
        && mBlockSize == loadStrategy.mBlockSize
        && mUseDirectIO == loadStrategy.mUseDirectIO;
}

IE_NAMESPACE_END(file_system);

