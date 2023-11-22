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
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_dictionary_bloom_filter_param.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class OnlineConfigImpl : public autil::legacy::Jsonizable
{
public:
    OnlineConfigImpl();
    OnlineConfigImpl(const OnlineConfigImpl& other);
    ~OnlineConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const OnlineConfigImpl& other) const;
    void operator=(const OnlineConfigImpl& other);

public:
    void SetEnableRedoSpeedup(bool enableFlag);
    bool IsRedoSpeedupEnabled() const;
    bool IsValidateIndexEnabled() const;
    bool IsReopenRetryOnIOExceptionEnabled() const;
    const DisableFieldsConfig& GetDisableFieldsConfig() const;
    DisableFieldsConfig& GetDisableFieldsConfig();
    void SetInitReaderThreadCount(int32_t initReaderThreadCount);
    int32_t GetInitReaderThreadCount() const;
    void SetVersionTsAlignment(int64_t ts);
    int64_t GetVersionTsAlignment() const;
    void SetRealtimeIndexSyncDir(const std::string& dir);
    const std::string& GetRealtimeIndexSyncDir() const;
    bool NeedSyncRealtimeIndex() const { return mEnableSyncRealtimeIndex; }
    void EnableSyncRealtimeIndex() { mEnableSyncRealtimeIndex = true; }

    void EnableOptimizedReopen() { mIsOptimizedReopen = true; }
    bool IsOptimizedReopen() const { return mIsOptimizedReopen; }

    bool AllowReopenOnRecoverRt() const;
    void SetAllowReopenOnRecoverRt(bool allow);
    void SetValidateIndexEnabled(bool enable);

    uint64_t GetOperationLogCatchUpThreshold() const { return mOperationLogCatchUpThreshold; }
    void SetOperationLogCatchUpThreshold(uint64_t operationLogCatchUpThreshold)
    {
        mOperationLogCatchUpThreshold = operationLogCatchUpThreshold;
    }
    bool OperationLogFlushToDiskEnabled() const { return mOperationLogFlushToDiskEnabled; }
    void SetOperationLogFlushToDiskConfig(bool enable) { mOperationLogFlushToDiskEnabled = enable; }
    void SetPrimaryNode(bool isPrimary) { mIsPrimaryNode = true; }
    bool IsPrimaryNode() const { return mIsPrimaryNode; }

    const std::vector<IndexDictionaryBloomFilterParam>& GetIndexDictBloomFilterParams() const
    {
        return mIndexDictBloomFilterParams;
    }

    void SetIndexDictBloomFilterParams(const std::vector<IndexDictionaryBloomFilterParam>& params)
    {
        mIndexDictBloomFilterParams = params;
    }
    const indexlib::file_system::LifecycleConfig& GetLifecycleConfig() const;

public:
    static constexpr uint64_t DEFAULT_OPERATION_LOG_CATCHUP_THRESHOLD = 100;

private:
    bool mEnableRedoSpeedup;
    bool mEnableValidateIndex;
    bool mEnableReopenRetryOnIOException;
    DisableFieldsConfig mDisableFieldsConfig;
    int32_t mInitReaderThreadCount;
    int64_t mVersionTsAlignment; // in microseconds
    std::string mRealtimeIndexSyncDir;
    bool mEnableSyncRealtimeIndex;
    bool mIsOptimizedReopen;
    bool mAllowReopenOnRecoverRt;
    uint64_t mOperationLogCatchUpThreshold;
    bool mOperationLogFlushToDiskEnabled;
    bool mIsPrimaryNode;
    std::vector<IndexDictionaryBloomFilterParam> mIndexDictBloomFilterParams;
    indexlib::file_system::LifecycleConfig mLifecycleConfig;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OnlineConfigImpl> OnlineConfigImplPtr;
}} // namespace indexlib::config
