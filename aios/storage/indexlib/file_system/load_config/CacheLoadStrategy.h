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

#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/util/cache/BlockCacheOption.h"
namespace indexlib { namespace file_system {

class CacheLoadStrategy : public LoadStrategy
{
public:
public:
    CacheLoadStrategy();
    CacheLoadStrategy(bool useDirectIO, bool cacheDecompressFile); // for global cache
    CacheLoadStrategy(const util::BlockCacheOption& option, bool useDirectIO, bool useGlobalCache,
                      bool cacheDecompressFile, bool isHighPriority);
    ~CacheLoadStrategy();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() override;
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const override;
    bool operator==(const CacheLoadStrategy& loadStrategy) const;

public:
    const std::string& GetLoadStrategyName() const override
    {
        return _useGlobalCache ? READ_MODE_GLOBAL_CACHE : READ_MODE_CACHE;
    }
    void SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit) override {}

    bool UseDirectIO() const { return _useDirectIO; }
    bool UseGlobalCache() const { return _useGlobalCache; }
    bool CacheDecompressFile() const { return _cacheDecompressFile; }
    bool UseHighPriority() const { return _useHighPriority; }
    const util::BlockCacheOption& GetBlockCacheOption() const;

public:
    static const size_t DEFAULT_CACHE_IO_BATCH_SIZE;

private:
    util::BlockCacheOption _cacheOption;
    bool _useDirectIO;
    bool _useGlobalCache;
    bool _cacheDecompressFile;
    bool _useHighPriority;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<CacheLoadStrategy> CacheLoadStrategyPtr;
}} // namespace indexlib::file_system
