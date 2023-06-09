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
#ifndef __INDEXLIB_OFFLINE_CONFIG_BASE_H
#define __INDEXLIB_OFFLINE_CONFIG_BASE_H

#include <memory>

#include "autil/EnvUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

struct OfflineReaderConfig : public autil::legacy::Jsonizable {
    OfflineReaderConfig()
        : loadIndex(true)
        , readPreference(ReadPreference::RP_SEQUENCE_PREFER)
        , useSearchCache(false)
        , useBlockCache(true)
        , indexCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
        , summaryCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
        , searchCacheSize(CACHE_DEFAULT_SIZE * 1024 * 1024)
        , cacheBlockSize(autil::EnvUtil::getEnv("CACHE_BLOCK_SIZE", CACHE_DEFAULT_BLOCK_SIZE))
        , cacheIOBatchSize(CACHE_DEFAULT_IO_BATCH_SIZE)
    {
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("use_block_cache", useBlockCache, useBlockCache);
        json.Jsonize("use_search_cache", useSearchCache, useSearchCache);
        json.Jsonize("index_block_cache_size", indexCacheSize, indexCacheSize);
        json.Jsonize("summary_block_cache_size", summaryCacheSize, summaryCacheSize);
        json.Jsonize("search_cache_size", searchCacheSize, searchCacheSize);
        json.Jsonize("cache_block_size", cacheBlockSize, cacheBlockSize);
        json.Jsonize("cache_io_batch_size", cacheIOBatchSize, cacheIOBatchSize);
    }

    // attention: do not add new member to this class
    bool loadIndex;
    ReadPreference readPreference;
    bool useSearchCache;
    bool useBlockCache;
    int64_t indexCacheSize;
    int64_t summaryCacheSize;
    int64_t searchCacheSize;
    int64_t cacheBlockSize;
    int64_t cacheIOBatchSize;
};

class OfflineConfigBase : public autil::legacy::Jsonizable
{
public:
    OfflineConfigBase();
    OfflineConfigBase(const BuildConfig& buildConf, const MergeConfig& mergeConf);

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
}} // namespace indexlib::config

#endif //__INDEXLIB_OFFLINE_CONFIG_BASE_H
