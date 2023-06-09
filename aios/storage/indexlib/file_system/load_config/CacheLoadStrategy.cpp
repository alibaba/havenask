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
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace file_system {
using util::BlockCacheOption;
AUTIL_LOG_SETUP(indexlib.file_system, CacheLoadStrategy);

const size_t CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE = 4;
CacheLoadStrategy::CacheLoadStrategy()
    : _useDirectIO(false)
    , _useGlobalCache(false)
    , _cacheDecompressFile(false)
    , _useHighPriority(false)
{
}

CacheLoadStrategy::CacheLoadStrategy(const BlockCacheOption& option, bool useDirectIO, bool useGlobalCache,
                                     bool cacheDecompressFile, bool isHighPriority)
    : _cacheOption(option)
    , _useDirectIO(useDirectIO)
    , _useGlobalCache(useGlobalCache)
    , _cacheDecompressFile(cacheDecompressFile)
    , _useHighPriority(isHighPriority)
{
}

CacheLoadStrategy::CacheLoadStrategy(bool useDirectIO, bool cacheDecompressFile)
    : _useDirectIO(useDirectIO)
    , _useGlobalCache(true)
    , _cacheDecompressFile(cacheDecompressFile)
    , _useHighPriority(false)
{
    _cacheOption.ioBatchSize = DEFAULT_CACHE_IO_BATCH_SIZE;
}

CacheLoadStrategy::~CacheLoadStrategy() {}

const BlockCacheOption& CacheLoadStrategy::GetBlockCacheOption() const { return _cacheOption; }

void CacheLoadStrategy::Check()
{
    if (_cacheOption.memorySize < _cacheOption.blockSize && _cacheOption.memorySize != 0) {
        INDEXLIB_THROW(util::BadParameterException, "cache size [%ld], block size [%ld]", _cacheOption.memorySize,
                       _cacheOption.blockSize);
    }
    if (_cacheOption.blockSize == 0) {
        INDEXLIB_THROW(util::BadParameterException, "block size is 0");
    }

    if (_useDirectIO && _cacheOption.blockSize % 4096 != 0) {
        INDEXLIB_THROW(util::BadParameterException,
                       "cache block size should "
                       "be multiple of 4096 when use direct io, block size is [%lu]",
                       _cacheOption.blockSize);
    }
}

void CacheLoadStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    // TODO: default cache_size changed
    json.Jsonize("direct_io", _useDirectIO, false);
    json.Jsonize("global_cache", _useGlobalCache, false);
    json.Jsonize("cache_decompress_file", _cacheDecompressFile, false);
    json.Jsonize("use_high_priority", _useHighPriority, false);
    json.Jsonize("cache_type", _cacheOption.cacheType, _cacheOption.cacheType);
    BlockCacheOption defaultOption;

    if (!_useGlobalCache) {
        if (json.GetMode() == FROM_JSON) {
            // for legacy: cache_size(old) and memory_size(new) are both memory size
            // if both filled, use field "memory_size"
            size_t memorySizeMB = defaultOption.memorySize / 1024 / 1024;
            size_t diskSizeGb = defaultOption.diskSize / 1024 / 1024 / 1024;
            json.Jsonize("cache_size", memorySizeMB, memorySizeMB);
            json.Jsonize("memory_size_in_mb", memorySizeMB, memorySizeMB);
            json.Jsonize("disk_size_in_gb", diskSizeGb, diskSizeGb);
            _cacheOption.memorySize = memorySizeMB * 1024 * 1024;
            _cacheOption.diskSize = diskSizeGb * 1024 * 1024 * 1024;
        } else {
            size_t memorySizeMB = _cacheOption.memorySize / 1024 / 1024;
            size_t diskSizeGB = _cacheOption.diskSize / 1024 / 1024 / 1024;
            json.Jsonize("memory_size_in_mb", memorySizeMB);
            json.Jsonize("disk_size_in_gb", diskSizeGB);
        }
        json.Jsonize("block_size", _cacheOption.blockSize, defaultOption.blockSize);
        json.Jsonize("io_batch_size", _cacheOption.ioBatchSize, defaultOption.ioBatchSize);
        json.Jsonize("cache_params", _cacheOption.cacheParams, defaultOption.cacheParams);
    }
}

bool CacheLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName()) {
        return false;
    }
    CacheLoadStrategyPtr right = std::dynamic_pointer_cast<CacheLoadStrategy>(loadStrategy);
    assert(right);

    return *this == *right;
}

bool CacheLoadStrategy::operator==(const CacheLoadStrategy& loadStrategy) const
{
    return _cacheOption == loadStrategy._cacheOption && _useDirectIO == loadStrategy._useDirectIO &&
           _useGlobalCache == loadStrategy._useGlobalCache &&
           _cacheDecompressFile == loadStrategy._cacheDecompressFile &&
           _useHighPriority == loadStrategy._useHighPriority;
}
}} // namespace indexlib::file_system
