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

#include <string>

#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlib {
namespace util {
class PartitionMemoryQuotaController;
class MetricProvider;
} // namespace util

namespace file_system {
class PackageFileTagConfigList;
class FileBlockCacheContainer;
class PackageFileTagConfigLis;

struct FlushRetryStrategy {
    int32_t retryTimes = 3;
    int32_t retryInterval = 1; // second
};

struct FileSystemOptions {
    LoadConfigList loadConfigList;
    FSMetricPreference metricPref = FSMP_ALL;
    std::shared_ptr<util::PartitionMemoryQuotaController> memoryQuotaController;
    std::shared_ptr<indexlibv2::MemoryQuotaController> memoryQuotaControllerV2;
    std::shared_ptr<FileBlockCacheContainer> fileBlockCacheContainer;
    std::shared_ptr<PackageFileTagConfigList> packageFileTagConfigList;
    std::vector<std::string> memMetricGroupPaths;
    FSStorageType outputStorage = FSST_DISK;
    FlushRetryStrategy flushRetryStrategy;
    std::string fileSystemIdentifier = "UNKNOWN";
    bool needFlush = true;
    bool enableAsyncFlush = true;
    bool useCache = true; // true for enable file node cache
    bool useRootLink = false;
    bool rootLinkWithTs = true;
    bool prohibitInMemDump = false;
    bool isOffline = false;
    bool redirectPhysicalRoot = false;    // true for online partition
    bool enableBackwardCompatible = true; // if false, mount version only use entry table
    bool TEST_useSessionFileCache = false;

    FileSystemOptions() = default;
    std::string DebugString() const { return ""; }

    static FileSystemOptions Offline()
    {
        FileSystemOptions fsOptions;
        fsOptions.isOffline = true;
        return fsOptions;
    }

    static FileSystemOptions OfflineWithBlockCache(std::shared_ptr<util::MetricProvider> metricProvider);
};

typedef std::shared_ptr<FileSystemOptions> FileSystemOptionsPtr;
} // namespace file_system
} // namespace indexlib
