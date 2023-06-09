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
#include "indexlib/config/offline_config.h"

#include "indexlib/config/impl/offline_config_impl.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, OfflineConfig);

OfflineConfig::OfflineConfig() { mImpl.reset(new OfflineConfigImpl); }

OfflineConfig::OfflineConfig(const BuildConfig& buildConf, const MergeConfig& mergeConf)
    : OfflineConfigBase(buildConf, mergeConf)
{
    mImpl.reset(new OfflineConfigImpl);
}

OfflineConfig::OfflineConfig(const OfflineConfig& other) : OfflineConfigBase(other)
{
    mImpl.reset(new OfflineConfigImpl(*other.mImpl));
}

OfflineConfig::~OfflineConfig() {}

void OfflineConfig::RebuildTruncateIndex() { mImpl->RebuildTruncateIndex(); }

void OfflineConfig::RebuildAdaptiveBitmap() { mImpl->RebuildAdaptiveBitmap(); }

bool OfflineConfig::NeedRebuildTruncateIndex() const { return mImpl->NeedRebuildTruncateIndex(); }

bool OfflineConfig::NeedRebuildAdaptiveBitmap() const { return mImpl->NeedRebuildAdaptiveBitmap(); }

void OfflineConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    OfflineConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

void OfflineConfig::Check() const
{
    OfflineConfigBase::Check();
    mImpl->Check();
}

void OfflineConfig::operator=(const OfflineConfig& other)
{
    *(OfflineConfigBase*)this = (const OfflineConfigBase&)other;
    *mImpl = *(other.mImpl);
}

const LoadConfigList& OfflineConfig::GetLoadConfigList() const { return mImpl->GetLoadConfigList(); }

void OfflineConfig::SetLoadConfigList(const LoadConfigList& loadConfigList)
{
    mImpl->SetLoadConfigList(loadConfigList);
}

const config::ModuleInfos& OfflineConfig::GetModuleInfos() const { return mImpl->GetModuleInfos(); }
}} // namespace indexlib::config
