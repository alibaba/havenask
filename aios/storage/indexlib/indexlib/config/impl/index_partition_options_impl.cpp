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
#include "indexlib/config/impl/index_partition_options_impl.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, IndexPartitionOptionsImpl);

IndexPartitionOptionsImpl::IndexPartitionOptionsImpl() : mIsOnline(true), mNeedRewriteFieldType(true) {}

IndexPartitionOptionsImpl::IndexPartitionOptionsImpl(const IndexPartitionOptionsImpl& other)
    : mOfflineConfig(other.mOfflineConfig)
    , mOnlineConfig(other.mOnlineConfig)
    , mIsOnline(other.mIsOnline)
    , mReservedVersionSet(other.mReservedVersionSet)
    , mOngoingModifyOpIds(other.mOngoingModifyOpIds)
    , mNeedRewriteFieldType(other.mNeedRewriteFieldType)
    , mVersionDesc(other.mVersionDesc)
    , mStandards(other.mStandards)
{
}

IndexPartitionOptionsImpl::~IndexPartitionOptionsImpl() {}

// for legacy index_format_version
void IndexPartitionOptionsImpl::SetEnablePackageFile(bool enablePackageFile)
{
    mOnlineConfig.buildConfig.enablePackageFile = enablePackageFile;
    mOfflineConfig.buildConfig.enablePackageFile = enablePackageFile;
}

void IndexPartitionOptionsImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("online_index_config", mOnlineConfig, mOnlineConfig);
    json.Jsonize("offline_index_config", mOfflineConfig, mOfflineConfig);
}

void IndexPartitionOptionsImpl::Check() const
{
    mOnlineConfig.Check();
    mOfflineConfig.Check();
}

void IndexPartitionOptionsImpl::RewriteForSchema(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    TableType tableType = schema->GetTableType();
    if (tableType == tt_index && IsOnline() && !mOngoingModifyOpIds.empty()) {
        for (auto& id : mOngoingModifyOpIds) {
            if (!schema->MarkOngoingModifyOperation(id)) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "mark ongoing modify operation [%d] fail!", id);
            }
        }
    }
    RewriteBuildConfig(tableType, GetBuildConfig(false));
    RewriteBuildConfig(tableType, GetBuildConfig(true));
}

void IndexPartitionOptionsImpl::RewriteBuildConfig(TableType tableType, BuildConfig& buildConfig)
{
    if (buildConfig.levelTopology == indexlibv2::framework::topo_default) {
        buildConfig.levelTopology = (tableType == tt_kv || tableType == tt_kkv) ? indexlibv2::framework::topo_hash_mod
                                                                                : indexlibv2::framework::topo_sequence;
        AUTIL_LOG(INFO, "default levelTopology use [%s]",
                  indexlibv2::framework::LevelMeta::TopologyToStr(buildConfig.levelTopology).c_str());
    }

    if (buildConfig.levelTopology == indexlibv2::framework::topo_hash_mod && buildConfig.levelNum <= 1) {
        buildConfig.levelNum = 2;
        AUTIL_LOG(INFO, "kv & kkv table use topo_hash_mode, levelNum should >= 1, rewrite to 2");
    }
}

void IndexPartitionOptionsImpl::operator=(const IndexPartitionOptionsImpl& other)
{
    mOfflineConfig = other.mOfflineConfig;
    mOnlineConfig = other.mOnlineConfig;
    mIsOnline = other.mIsOnline;
    mReservedVersionSet = other.mReservedVersionSet;
    mOngoingModifyOpIds = other.mOngoingModifyOpIds;
    mNeedRewriteFieldType = other.mNeedRewriteFieldType;
    mVersionDesc = other.mVersionDesc;
    mStandards = other.mStandards;
}

void IndexPartitionOptionsImpl::AddVersionDesc(const std::string& key, const std::string& value)
{
    auto iter = mVersionDesc.find(key);
    if (iter == mVersionDesc.end()) {
        mVersionDesc.insert(make_pair(key, value));
        return;
    }
    iter->second = value;
}

void IndexPartitionOptionsImpl::DelVersionDesc(const string& key)
{
    auto iter = mVersionDesc.find(key);
    if (iter == mVersionDesc.end()) {
        return;
    }
    mVersionDesc.erase(iter);
}

bool IndexPartitionOptionsImpl::GetVersionDesc(const std::string& key, std::string& value) const
{
    auto iter = mVersionDesc.find(key);
    if (iter == mVersionDesc.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

const map<string, string>& IndexPartitionOptionsImpl::GetVersionDescriptions() const { return mVersionDesc; }
void IndexPartitionOptionsImpl::SetUpdateableSchemaStandards(const UpdateableSchemaStandards& standards)
{
    mStandards = standards;
}

const UpdateableSchemaStandards& IndexPartitionOptionsImpl::GetUpdateableSchemaStandards() const { return mStandards; }

}} // namespace indexlib::config
