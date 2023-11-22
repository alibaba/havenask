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
#include "indexlib/config/online_config.h"

#include "indexlib/config/impl/online_config_impl.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
//#include "indexlib/index_define.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::file_system;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, OnlineConfig);

OnlineConfig::OnlineConfig() { mImpl.reset(new OnlineConfigImpl); }

OnlineConfig::OnlineConfig(const OnlineConfig& other) : OnlineConfigBase(other)
{
    mImpl.reset(new OnlineConfigImpl(*other.mImpl));
}

OnlineConfig::~OnlineConfig() {}

void OnlineConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    OnlineConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
    if (json.GetMode() == FROM_JSON) {
        RewriteLoadConfig();
    }
}

void OnlineConfig::Check() const
{
    OnlineConfigBase::Check();
    mImpl->Check();
}

void OnlineConfig::operator=(const OnlineConfig& other)
{
    *(OnlineConfigBase*)this = (const OnlineConfigBase&)other;
    *mImpl = *(other.mImpl);
}

void OnlineConfig::SetRealtimeIndexSyncDir(const std::string& dir) { mImpl->SetRealtimeIndexSyncDir(dir); }

const std::string& OnlineConfig::GetRealtimeIndexSyncDir() const { return mImpl->GetRealtimeIndexSyncDir(); }

bool OnlineConfig::IsOptimizedReopen() const { return mImpl->IsOptimizedReopen(); }

bool OnlineConfig::AllowReopenOnRecoverRt() const { return mImpl->AllowReopenOnRecoverRt(); }

void OnlineConfig::SetAllowReopenOnRecoverRt(bool allow) { return mImpl->SetAllowReopenOnRecoverRt(allow); }

void OnlineConfig::SetEnableRedoSpeedup(bool enableFlag) { return mImpl->SetEnableRedoSpeedup(enableFlag); }
void OnlineConfig::SetValidateIndexEnabled(bool enable) { return mImpl->SetValidateIndexEnabled(enable); }

void OnlineConfig::EnableOptimizedReopen() { return mImpl->EnableOptimizedReopen(); }

bool OnlineConfig::NeedSyncRealtimeIndex() const { return mImpl->NeedSyncRealtimeIndex(); }

void OnlineConfig::EnableSyncRealtimeIndex() { mImpl->EnableSyncRealtimeIndex(); }

bool OnlineConfig::IsRedoSpeedupEnabled() const { return mImpl->IsRedoSpeedupEnabled(); }

void OnlineConfig::SetVersionTsAlignment(int64_t ts) { mImpl->SetVersionTsAlignment(ts); }

int64_t OnlineConfig::GetVersionTsAlignment() const { return mImpl->GetVersionTsAlignment(); }

bool OnlineConfig::IsValidateIndexEnabled() const { return mImpl->IsValidateIndexEnabled(); }

bool OnlineConfig::IsReopenRetryOnIOExceptionEnabled() const { return mImpl->IsReopenRetryOnIOExceptionEnabled(); }

DisableFieldsConfig& OnlineConfig::GetDisableFieldsConfig() { return mImpl->GetDisableFieldsConfig(); }

const indexlib::file_system::LifecycleConfig& OnlineConfig::GetLifecycleConfig() const
{
    return mImpl->GetLifecycleConfig();
}

const DisableFieldsConfig& OnlineConfig::GetDisableFieldsConfig() const { return mImpl->GetDisableFieldsConfig(); }

bool OnlineConfig::NeedLoadWithLifeCycle() const { return loadConfigList.NeedLoadWithLifeCycle(); }

void OnlineConfig::SetInitReaderThreadCount(int32_t initReaderThreadCount)
{
    mImpl->SetInitReaderThreadCount(initReaderThreadCount);
}

int32_t OnlineConfig::GetInitReaderThreadCount() const { return mImpl->GetInitReaderThreadCount(); }

void OnlineConfig::RewriteLoadConfig()
{
    if (!mImpl->GetDisableFieldsConfig().rewriteLoadConfig) {
        return;
    }

    // for (const auto& loadConfig : loadConfigList.GetLoadConfigs()) {
    //     if (loadConfig.GetName() == DISABLE_FIELDS_LOAD_CONFIG_NAME) {
    //         AUTIL_LOG(ERROR, "duplicate load config [%s]: [%s] exist in loadConfigList",
    //         loadConfig.GetName().c_str(),
    //                ToJsonString(loadConfig).c_str());
    //     }
    // }

    if (mImpl->GetDisableFieldsConfig().attributes.size() > 0 ||
        mImpl->GetDisableFieldsConfig().packAttributes.size() > 0 ||
        mImpl->GetDisableFieldsConfig().indexes.size() > 0 ||
        mImpl->GetDisableFieldsConfig().summarys != DisableFieldsConfig::SDF_FIELD_NONE ||
        mImpl->GetDisableFieldsConfig().sources != DisableFieldsConfig::CDF_FIELD_NONE) {
        LoadConfig loadConfig = CreateDisableLoadConfig(
            DISABLE_FIELDS_LOAD_CONFIG_NAME, mImpl->GetDisableFieldsConfig().indexes,
            mImpl->GetDisableFieldsConfig().attributes, mImpl->GetDisableFieldsConfig().packAttributes,
            mImpl->GetDisableFieldsConfig().summarys != DisableFieldsConfig::SDF_FIELD_NONE,
            mImpl->GetDisableFieldsConfig().sources == DisableFieldsConfig::CDF_FIELD_ALL,
            mImpl->GetDisableFieldsConfig().disabledSourceGroupIds);
        loadConfigList.PushFront(loadConfig);
    }
}

LoadConfig OnlineConfig::CreateDisableLoadConfig(const string& name, const set<string>& indexes,
                                                 const set<string>& attributes, const set<string>& packAttrs,
                                                 const bool disableSummaryField, const bool disableSourceField,
                                                 const vector<index::sourcegroupid_t>& disableSourceGroupIds)
{
    vector<string> indexVec(indexes.begin(), indexes.end());
    vector<string> attrVec(attributes.begin(), attributes.end());
    vector<string> packVec(packAttrs.begin(), packAttrs.end());
    return CreateDisableLoadConfig(name, indexVec, attrVec, packVec, disableSummaryField, disableSourceField,
                                   disableSourceGroupIds);
}

LoadConfig OnlineConfig::CreateDisableLoadConfig(const string& name, const vector<string>& indexes,
                                                 const vector<string>& attributes, const vector<string>& packAttrs,
                                                 const bool disableSummaryField, const bool disableSourceField,
                                                 const vector<index::sourcegroupid_t>& disableSourceGroupIds)
{
#define SEGMENT_P "segment_[0-9]+(_level_[0-9]+)?(/sub_segment)?"
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(LoadStrategyPtr(new CacheLoadStrategy()));
    loadConfig.SetRemote(false);
    loadConfig.SetDeploy(false);
    loadConfig.SetName(name);
    vector<string> filePatterns;
    for (const string& attribute : attributes) {
        filePatterns.push_back(string(SEGMENT_P "/attribute/") + attribute + "($|/)");
    }
    for (const string& packAttribute : packAttrs) {
        filePatterns.push_back(string(SEGMENT_P "/attribute/") + packAttribute + "($|/)");
    }
    for (const string& index : indexes) {
        filePatterns.push_back(string(SEGMENT_P "/index/") + index + "($|/)");
    }
    if (disableSummaryField) {
        filePatterns.push_back(string(SEGMENT_P "/summary/"));
    }
    if (disableSourceField) {
        filePatterns.push_back(string(SEGMENT_P "/source/"));
    }
    for (auto groupId : disableSourceGroupIds) {
        string groupDirName = std::string(index::SOURCE_DATA_DIR_PREFIX) + "_" + autil::StringUtil::toString(groupId);
        filePatterns.push_back(string(SEGMENT_P "/source/") + groupDirName + "($|/)");
    }

    loadConfig.SetFilePatternString(filePatterns);
    loadConfig.Check();
#undef SEGMENT_P
    return loadConfig;
}

uint64_t OnlineConfig::GetOperationLogCatchUpThreshold() const { return mImpl->GetOperationLogCatchUpThreshold(); }

void OnlineConfig::SetOperationLogCatchUpThreshold(uint64_t operationLogCatchUpThreshold)
{
    mImpl->SetOperationLogCatchUpThreshold(operationLogCatchUpThreshold);
}

bool OnlineConfig::OperationLogFlushToDiskEnabled() const { return mImpl->OperationLogFlushToDiskEnabled(); }
void OnlineConfig::SetOperationLogFlushToDiskConfig(bool enable) { mImpl->SetOperationLogFlushToDiskConfig(enable); }

void OnlineConfig::SetPrimaryNode(bool isPrimary) { mImpl->SetPrimaryNode(isPrimary); }
bool OnlineConfig::IsPrimaryNode() const { return mImpl->IsPrimaryNode(); }

const vector<IndexDictionaryBloomFilterParam>& OnlineConfig::GetIndexDictBloomFilterParams() const
{
    return mImpl->GetIndexDictBloomFilterParams();
}

void OnlineConfig::SetIndexDictBloomFilterParams(const std::vector<IndexDictionaryBloomFilterParam>& params)
{
    mImpl->SetIndexDictBloomFilterParams(params);
}

bool OnlineConfig::EnableLocalDeployManifestChecking() const
{
    return NeedDeployIndex() && GetLifecycleConfig().EnableLocalDeployManifestChecking();
}

}} // namespace indexlib::config
