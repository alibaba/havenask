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
#include "indexlib/config/offline_config.h"
#include "indexlib/config/online_config.h"
#include "indexlib/config/updateable_schema_standards.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class IndexPartitionSchema;
typedef std::shared_ptr<IndexPartitionSchema> IndexPartitionSchemaPtr;
} // namespace indexlib::config

namespace indexlib { namespace config {

class IndexPartitionOptionsImpl : public autil::legacy::Jsonizable
{
public:
    IndexPartitionOptionsImpl();
    IndexPartitionOptionsImpl(const IndexPartitionOptionsImpl& other);
    ~IndexPartitionOptionsImpl();

public:
    void SetOnlineConfig(const OnlineConfig& onlineConfig) { mOnlineConfig = onlineConfig; }

    void SetOfflineConfig(const OfflineConfig& offlineConfig) { mOfflineConfig = offlineConfig; }

    const OnlineConfig& GetOnlineConfig() const { return mOnlineConfig; }

    const OfflineConfig& GetOfflineConfig() const { return mOfflineConfig; }

    OnlineConfig& GetOnlineConfig() { return mOnlineConfig; }

    OfflineConfig& GetOfflineConfig() { return mOfflineConfig; }

    void SetIsOnline(bool isOnline) { mIsOnline = isOnline; }

    bool IsOnline() const { return mIsOnline; }

    bool IsOffline() const { return !IsOnline(); }

    bool NeedLoadIndex() const { return IsOnline() ? true : GetOfflineConfig().readerConfig.loadIndex; }

    const BuildConfig& GetBuildConfig(bool isOnline) const
    {
        if (isOnline) {
            return mOnlineConfig.buildConfig;
        }
        return mOfflineConfig.buildConfig;
    }

    BuildConfig& GetBuildConfig(bool isOnline)
    {
        return const_cast<BuildConfig&>(static_cast<const IndexPartitionOptionsImpl*>(this)->GetBuildConfig(isOnline));
    }

    const BuildConfig& GetBuildConfig() const { return GetBuildConfig(mIsOnline); }

    BuildConfig& GetBuildConfig() { return GetBuildConfig(mIsOnline); }

    const MergeConfig& GetMergeConfig() const { return mOfflineConfig.mergeConfig; }

    MergeConfig& GetMergeConfig() { return mOfflineConfig.mergeConfig; }

    // for legacy index_format_version
    void SetEnablePackageFile(bool enablePackageFile = true);

    void RewriteForSchema(const IndexPartitionSchemaPtr& schema);

    void SetReservedVersions(const std::set<versionid_t>& versionSet) { mReservedVersionSet = versionSet; }
    const std::set<versionid_t>& GetReservedVersions() const { return mReservedVersionSet; }

    void SetOngoingModifyOperationIds(const std::vector<schema_opid_t>& ids) { mOngoingModifyOpIds = ids; }

    const std::vector<schema_opid_t>& GetOngoingModifyOperationIds() const { return mOngoingModifyOpIds; }

    bool NeedRewriteFieldType() const { return mNeedRewriteFieldType; }

    void SetNeedRewriteFieldType(bool need) { mNeedRewriteFieldType = need; }

    void AddVersionDesc(const std::string& key, const std::string& value);
    void DelVersionDesc(const std::string& key);
    bool GetVersionDesc(const std::string& key, std::string& value) const;
    const std::map<std::string, std::string>& GetVersionDescriptions() const;

    void SetUpdateableSchemaStandards(const UpdateableSchemaStandards& standards);
    const UpdateableSchemaStandards& GetUpdateableSchemaStandards() const;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void RewriteBuildConfig(TableType tableType, BuildConfig& buildConfig);
    void operator=(const IndexPartitionOptionsImpl& other);

private:
    OfflineConfig mOfflineConfig;
    OnlineConfig mOnlineConfig;
    bool mIsOnline;
    std::set<versionid_t> mReservedVersionSet;
    std::vector<schema_opid_t> mOngoingModifyOpIds;
    bool mNeedRewriteFieldType;
    std::map<std::string, std::string> mVersionDesc;
    UpdateableSchemaStandards mStandards;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionOptionsImpl> IndexPartitionOptionsImplPtr;
}} // namespace indexlib::config
