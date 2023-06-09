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
#include "indexlib/config/impl/online_config_impl.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, OnlineConfigImpl);

OnlineConfigImpl::OnlineConfigImpl()
    : mEnableRedoSpeedup(false)
    , mEnableValidateIndex(true)
    , mEnableReopenRetryOnIOException(true)
    , mInitReaderThreadCount(0)
    , mVersionTsAlignment(1000000)
    , mEnableSyncRealtimeIndex(false)
    , mIsOptimizedReopen(false)
    , mAllowReopenOnRecoverRt(true)
    , mOperationLogCatchUpThreshold(DEFAULT_OPERATION_LOG_CATCHUP_THRESHOLD)
    , mOperationLogFlushToDiskEnabled(false)
    , mIsPrimaryNode(false)
    , mLifecycleConfig()
{
}

OnlineConfigImpl::OnlineConfigImpl(const OnlineConfigImpl& other)
    : mEnableRedoSpeedup(other.mEnableRedoSpeedup)
    , mEnableValidateIndex(other.mEnableValidateIndex)
    , mEnableReopenRetryOnIOException(other.mEnableReopenRetryOnIOException)
    , mDisableFieldsConfig(other.mDisableFieldsConfig)
    , mInitReaderThreadCount(other.mInitReaderThreadCount)
    , mVersionTsAlignment(other.mVersionTsAlignment)
    , mRealtimeIndexSyncDir(other.mRealtimeIndexSyncDir)
    , mEnableSyncRealtimeIndex(other.mEnableSyncRealtimeIndex)
    , mIsOptimizedReopen(other.mIsOptimizedReopen)
    , mAllowReopenOnRecoverRt(other.mAllowReopenOnRecoverRt)
    , mOperationLogCatchUpThreshold(other.mOperationLogCatchUpThreshold)
    , mOperationLogFlushToDiskEnabled(other.mOperationLogFlushToDiskEnabled)
    , mIsPrimaryNode(other.mIsPrimaryNode)
    , mIndexDictBloomFilterParams(other.mIndexDictBloomFilterParams)
    , mLifecycleConfig(other.mLifecycleConfig)
{
}

OnlineConfigImpl::~OnlineConfigImpl() {}

void OnlineConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("enable_redo_speedup", mEnableRedoSpeedup, mEnableRedoSpeedup);
    json.Jsonize("enable_validate_index", mEnableValidateIndex, mEnableValidateIndex);
    json.Jsonize("enable_reopen_retry_io_exception", mEnableReopenRetryOnIOException, mEnableReopenRetryOnIOException);
    json.Jsonize("disable_fields", mDisableFieldsConfig, mDisableFieldsConfig);
    json.Jsonize("init_reader_thread_count", mInitReaderThreadCount, mInitReaderThreadCount);
    json.Jsonize("version_timestamp_alignment", mVersionTsAlignment, mVersionTsAlignment);
    json.Jsonize("realtime_index_sync_dir", mRealtimeIndexSyncDir, mRealtimeIndexSyncDir);
    json.Jsonize("enable_sync_realtime_index", mEnableSyncRealtimeIndex, mEnableSyncRealtimeIndex);
    json.Jsonize("is_optimized_reopen", mIsOptimizedReopen, mIsOptimizedReopen);
    json.Jsonize("allow_reopen_on_recover_rt", mAllowReopenOnRecoverRt, mAllowReopenOnRecoverRt);
    json.Jsonize("operation_log_catchup_threshold", mOperationLogCatchUpThreshold, mOperationLogCatchUpThreshold);
    json.Jsonize("operation_log_flush_to_disk", mOperationLogFlushToDiskEnabled, mOperationLogFlushToDiskEnabled);
    json.Jsonize("index_dictionary_bloom_filter_param", mIndexDictBloomFilterParams, mIndexDictBloomFilterParams);
    json.Jsonize("lifecycle_config", mLifecycleConfig, mLifecycleConfig);
}

void OnlineConfigImpl::Check() const {}

bool OnlineConfigImpl::operator==(const OnlineConfigImpl& other) const
{
    return mEnableRedoSpeedup == other.mEnableRedoSpeedup && mEnableValidateIndex == other.mEnableValidateIndex &&
           mEnableReopenRetryOnIOException == other.mEnableReopenRetryOnIOException &&
           mDisableFieldsConfig == other.mDisableFieldsConfig &&
           mInitReaderThreadCount == other.mInitReaderThreadCount && mVersionTsAlignment == other.mVersionTsAlignment &&
           mRealtimeIndexSyncDir == other.mRealtimeIndexSyncDir && mIsOptimizedReopen == other.mIsOptimizedReopen &&
           mAllowReopenOnRecoverRt == other.mAllowReopenOnRecoverRt &&
           mEnableSyncRealtimeIndex == other.mEnableSyncRealtimeIndex &&
           mOperationLogCatchUpThreshold == other.mOperationLogCatchUpThreshold &&
           mOperationLogFlushToDiskEnabled == other.mOperationLogFlushToDiskEnabled &&
           mIndexDictBloomFilterParams == other.mIndexDictBloomFilterParams && mIsPrimaryNode == other.mIsPrimaryNode &&
           mLifecycleConfig == other.mLifecycleConfig;
}

void OnlineConfigImpl::operator=(const OnlineConfigImpl& other)
{
    mEnableValidateIndex = other.mEnableValidateIndex;
    mEnableReopenRetryOnIOException = other.mEnableReopenRetryOnIOException;
    mDisableFieldsConfig = other.mDisableFieldsConfig;
    mEnableRedoSpeedup = other.mEnableRedoSpeedup;
    mInitReaderThreadCount = other.mInitReaderThreadCount;
    mVersionTsAlignment = other.mVersionTsAlignment;
    mRealtimeIndexSyncDir = other.mRealtimeIndexSyncDir;
    mEnableSyncRealtimeIndex = other.mEnableSyncRealtimeIndex;

    mIsOptimizedReopen = other.mIsOptimizedReopen;
    mAllowReopenOnRecoverRt = other.mAllowReopenOnRecoverRt;
    mOperationLogCatchUpThreshold = other.mOperationLogCatchUpThreshold;
    mOperationLogFlushToDiskEnabled = other.mOperationLogFlushToDiskEnabled;
    mIsPrimaryNode = other.mIsPrimaryNode;
    mIndexDictBloomFilterParams = other.mIndexDictBloomFilterParams;
    mLifecycleConfig = other.mLifecycleConfig;
}

void OnlineConfigImpl::SetAllowReopenOnRecoverRt(bool allow) { mAllowReopenOnRecoverRt = allow; }

bool OnlineConfigImpl::AllowReopenOnRecoverRt() const { return mAllowReopenOnRecoverRt; }

void OnlineConfigImpl::SetRealtimeIndexSyncDir(const std::string& dir) { mRealtimeIndexSyncDir = dir; }

const std::string& OnlineConfigImpl::GetRealtimeIndexSyncDir() const { return mRealtimeIndexSyncDir; }

void OnlineConfigImpl::SetEnableRedoSpeedup(bool enableFlag) { mEnableRedoSpeedup = enableFlag; }

bool OnlineConfigImpl::IsRedoSpeedupEnabled() const { return mEnableRedoSpeedup; }

bool OnlineConfigImpl::IsValidateIndexEnabled() const { return mEnableValidateIndex; }
void OnlineConfigImpl::SetValidateIndexEnabled(bool enable) { mEnableValidateIndex = enable; }

bool OnlineConfigImpl::IsReopenRetryOnIOExceptionEnabled() const { return mEnableReopenRetryOnIOException; }

const DisableFieldsConfig& OnlineConfigImpl::GetDisableFieldsConfig() const { return mDisableFieldsConfig; }

DisableFieldsConfig& OnlineConfigImpl::GetDisableFieldsConfig() { return mDisableFieldsConfig; }

void OnlineConfigImpl::SetInitReaderThreadCount(int32_t initReaderThreadCount)
{
    mInitReaderThreadCount = initReaderThreadCount;
}
const indexlib::file_system::LifecycleConfig& OnlineConfigImpl::GetLifecycleConfig() const { return mLifecycleConfig; }

int32_t OnlineConfigImpl::GetInitReaderThreadCount() const { return mInitReaderThreadCount; }

void OnlineConfigImpl::SetVersionTsAlignment(int64_t ts) { mVersionTsAlignment = ts; }

int64_t OnlineConfigImpl::GetVersionTsAlignment() const { return mVersionTsAlignment; }

}} // namespace indexlib::config
