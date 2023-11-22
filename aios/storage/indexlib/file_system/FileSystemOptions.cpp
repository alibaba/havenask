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

#include "indexlib/file_system/FileSystemOptions.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace file_system {

FileSystemOptions FileSystemOptions::OfflineWithBlockCache(util::MetricProviderPtr metricProvider)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.useCache = false;
    std::shared_ptr<FileBlockCacheContainer> fileBlockCacheContainer(new FileBlockCacheContainer);
    std::string memory_size_in_mb = autil::EnvUtil::getEnv("INDEXLIB_COMPRESS_READ_BATCH_SIZE", "512");
    fileBlockCacheContainer->Init("memory_size_in_mb=" + memory_size_in_mb +
                                      ";block_size=2097152;io_batch_size=1;num_shard_bits=0",
                                  nullptr, nullptr,
                                  metricProvider); // block size = 2MB, cache size = 512M
    fsOptions.fileBlockCacheContainer = fileBlockCacheContainer;

    LoadConfigList loadConfigList;
    {
        // attribute & pk attribute
        LoadConfig attrLoadConfig;
        LoadStrategyPtr attrCacheLoadStrategy(
            new CacheLoadStrategy(/*direct io*/ false, /*cache decompress file*/ false));
        LoadConfig::FilePatternStringVector attrPattern;
        attrPattern.push_back("_ATTRIBUTE_DATA_");
        attrPattern.push_back("_SECTION_ATTRIBUTE_");
        attrPattern.push_back("_PK_ATTRIBUTE_");
        attrLoadConfig.SetFilePatternString(attrPattern);
        attrLoadConfig.SetLoadStrategyPtr(attrCacheLoadStrategy);
        loadConfigList.PushFront(attrLoadConfig);
    }
    fsOptions.loadConfigList = loadConfigList;
    return fsOptions;
}
}} // namespace indexlib::file_system
