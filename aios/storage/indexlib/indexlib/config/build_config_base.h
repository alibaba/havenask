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
#ifndef __INDEXLIB_BUILD_CONFIG_BASE_H
#define __INDEXLIB_BUILD_CONFIG_BASE_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class BuildConfigBase : public autil::legacy::Jsonizable
{
public:
    enum BuildPhase {
        BP_FULL,
        BP_INC,
        BP_RT,
    };

public:
    BuildConfigBase();
    virtual ~BuildConfigBase();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const BuildConfigBase& other) const;
    static bool GetEnvBuildTotalMemUse(int64_t& mem);
    double GetDefaultBuildResourceMemoryLimitRatio();

public:
    // attention: do not add new member to this class
    // U can add new member to BuildConfigImpl
    int64_t hashMapInitSize;
    int64_t buildTotalMemory;         // unit, MB
    int64_t buildResourceMemoryLimit; // unit, MB. for CustomOfflinePartition
    uint32_t maxDocCount;
    uint32_t keepVersionCount;
    uint32_t dumpThreadCount;
    uint32_t shardingColumnNum;
    indexlibv2::framework::LevelTopology levelTopology;
    uint32_t levelNum;
    int32_t executorThreadCount;
    int64_t ttl; // in second
    bool enablePackageFile;
    bool speedUpPrimaryKeyReader;
    bool useUserTimestamp;
    bool usePathMetaCache;
    BuildPhase buildPhase;
    std::string speedUpPrimaryKeyReaderParam;

public:
    static const uint32_t DEFAULT_HASHMAP_INIT_SIZE = 0;
    static const uint64_t DEFAULT_BUILD_TOTAL_MEMORY = 6 * 1024;       // 6 GB
    static const uint64_t DEFAULT_BUILD_TOTAL_MEMORY_IN_TESTMODE = 64; // 64M
    static const uint32_t DEFAULT_MAX_DOC_COUNT = (uint32_t)-1;
    static const uint32_t DEFAULT_KEEP_VERSION_COUNT = 2;
    static const uint32_t DEFAULT_DUMP_THREADCOUNT = 1;
    static const uint32_t DEFAULT_EXECUTOR_THREADCOUNT = 1;
    static const uint32_t MAX_DUMP_THREAD_COUNT = 20;
    static const uint32_t DEFAULT_SHARDING_COLUMN_NUM = 1;
    static const bool DEFAULT_USE_PACKAGE_FILE;
    static const double DEFAULT_BUILD_RESOURCE_MEM_LIMIT_RATIO;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildConfigBase);
}} // namespace indexlib::config

#endif //__INDEXLIB_BUILD_CONFIG_BASE_H
