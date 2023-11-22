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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/index_dictionary_bloom_filter_param.h"
#include "indexlib/config/online_config_base.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class DisableFieldsConfig;
typedef std::shared_ptr<DisableFieldsConfig> DisableFieldsConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class OnlineConfigImpl;
typedef std::shared_ptr<OnlineConfigImpl> OnlineConfigImplPtr;
} // namespace indexlib::config
namespace indexlib::file_system {
class LoadConfig;
typedef std::shared_ptr<LoadConfig> LoadConfigPtr;
} // namespace indexlib::file_system

namespace indexlib::file_system {
class LifecycleConfig;
class LoadConfigList;
} // namespace indexlib::file_system

namespace indexlib { namespace config {

class OnlineConfig : public OnlineConfigBase
{
public:
    using OnlineConfigBase::DEFAULT_BUILD_TOTAL_MEMORY;
    using OnlineConfigBase::DEFAULT_LOAD_PATCH_THREAD_NUM;
    using OnlineConfigBase::DEFAULT_MAX_CUR_READER_RECLAIMABLE_MEM;
    using OnlineConfigBase::DEFAULT_MAX_REALTIME_DUMP_INTERVAL;
    using OnlineConfigBase::DEFAULT_MAX_REALTIME_MEMORY_SIZE;
    using OnlineConfigBase::DEFAULT_ONLINE_KEEP_VERSION_COUNT;
    using OnlineConfigBase::DEFAULT_PRINT_METRICS_INTERVAL;
    using OnlineConfigBase::DEFAULT_REALTIME_BUILD_HASHMAP_INIT_SIZE;
    using OnlineConfigBase::DEFAULT_REALTIME_BUILD_USE_PACKAGE_FILE;
    using OnlineConfigBase::DEFAULT_REALTIME_DUMP_THREAD_COUNT;
    using OnlineConfigBase::INVALID_MAX_REOPEN_MEMORY_USE;
    using OnlineConfigBase::INVALID_MAX_SWAP_MMAP_FILE_SIZE;

public:
    OnlineConfig();
    OnlineConfig(const OnlineConfig& other);
    ~OnlineConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator=(const OnlineConfig& other);

    using OnlineConfigBase::EnableIntervalDump;
    using OnlineConfigBase::GetMaxReopenMemoryUse;
    using OnlineConfigBase::NeedDeployIndex;
    using OnlineConfigBase::NeedReadRemoteIndex;
    using OnlineConfigBase::SetMaxReopenMemoryUse;

public:
    void SetEnableRedoSpeedup(bool enableFlag);
    bool IsRedoSpeedupEnabled() const;
    bool IsValidateIndexEnabled() const;
    DisableFieldsConfig& GetDisableFieldsConfig();
    const DisableFieldsConfig& GetDisableFieldsConfig() const;
    void SetInitReaderThreadCount(int32_t initReaderThreadCount);
    int32_t GetInitReaderThreadCount() const;
    void SetVersionTsAlignment(int64_t ts);
    int64_t GetVersionTsAlignment() const;
    bool IsReopenRetryOnIOExceptionEnabled() const;
    void EnableOptimizedReopen();
    bool NeedSyncRealtimeIndex() const;
    void SetRealtimeIndexSyncDir(const std::string& dir);
    const std::string& GetRealtimeIndexSyncDir() const;

    bool IsOptimizedReopen() const;
    bool AllowReopenOnRecoverRt() const;
    void SetAllowReopenOnRecoverRt(bool allow);
    void SetValidateIndexEnabled(bool enable);

    uint64_t GetOperationLogCatchUpThreshold() const;
    const indexlib::file_system::LifecycleConfig& GetLifecycleConfig() const;
    void SetOperationLogCatchUpThreshold(uint64_t operationLogCatchUpThreshold);
    void SetOperationLogFlushToDiskConfig(bool enable);
    bool OperationLogFlushToDiskEnabled() const;
    bool NeedLoadWithLifeCycle() const;
    void SetPrimaryNode(bool isPrimary); // online primary node can write direct to remote path
    bool IsPrimaryNode() const;
    const std::vector<IndexDictionaryBloomFilterParam>& GetIndexDictBloomFilterParams() const;
    void SetIndexDictBloomFilterParams(const std::vector<IndexDictionaryBloomFilterParam>& params);
    bool EnableLocalDeployManifestChecking() const;

public:
    static LoadConfig CreateDisableLoadConfig(const std::string& name, const std::vector<std::string>& indexes,
                                              const std::vector<std::string>& attributes,
                                              const std::vector<std::string>& packAttrs,
                                              const bool disableSummaryField = false,
                                              const bool disableSourceField = false,
                                              const std::vector<index::sourcegroupid_t>& disableSourceGroupIds = {});

    static LoadConfig CreateDisableLoadConfig(const std::string& name, const std::set<std::string>& indexes,
                                              const std::set<std::string>& attributes,
                                              const std::set<std::string>& packAttrs,
                                              const bool disableSummaryField = false,
                                              const bool disableSourceField = false,
                                              const std::vector<index::sourcegroupid_t>& disableSourceGroupIds = {});

public:
    // interface for test
    void EnableSyncRealtimeIndex();

private:
    void RewriteLoadConfig();

private:
    OnlineConfigImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OnlineConfig> OnlineConfigPtr;
}} // namespace indexlib::config
