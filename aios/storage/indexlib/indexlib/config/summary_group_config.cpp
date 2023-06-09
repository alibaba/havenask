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
#include "indexlib/config/summary_group_config.h"

#include "indexlib/config/impl/summary_group_config_impl.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SummaryGroupConfig);

SummaryGroupConfig::SummaryGroupConfig(const string& groupName, summarygroupid_t groupId,
                                       summaryfieldid_t summaryFieldIdBase)
    : indexlibv2::config::SummaryGroupConfig(groupName, groupId, summaryFieldIdBase)
    , mImpl(new SummaryGroupConfigImpl(groupName, groupId))
{
}

SummaryGroupConfig::~SummaryGroupConfig() {}

void SummaryGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void SummaryGroupConfig::SetGroupName(const string& groupName)
{
    indexlibv2::config::SummaryGroupConfig::SetGroupName(groupName);
    mImpl->SetGroupName(groupName);
}

void SummaryGroupConfig::SetCompress(bool useCompress, const string& compressType)
{
    indexlibv2::config::SummaryGroupConfig::SetCompress(useCompress, compressType);
    mImpl->SetCompress(useCompress, compressType);
}

bool SummaryGroupConfig::IsCompress() const { return mImpl->IsCompress(); }

const std::string& SummaryGroupConfig::GetCompressType() const { return mImpl->GetCompressType(); }

void SummaryGroupConfig::SetSummaryGroupDataParam(const GroupDataParameter& param)
{
    indexlibv2::config::SummaryGroupConfig::SetSummaryGroupDataParam(param);
    mImpl->SetSummaryGroupDataParam(param);
}
void SummaryGroupConfig::SetNeedStoreSummary(bool needStoreSummary)
{
    indexlibv2::config::SummaryGroupConfig::SetNeedStoreSummary(needStoreSummary);
    return mImpl->SetNeedStoreSummary(needStoreSummary);
}

void SummaryGroupConfig::AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig)
{
    indexlibv2::config::SummaryGroupConfig::AddSummaryConfig(summaryConfig);
    mImpl->AddSummaryConfig(summaryConfig);
}

void SummaryGroupConfig::AssertEqual(const SummaryGroupConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void SummaryGroupConfig::AssertCompatible(const SummaryGroupConfig& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

void SummaryGroupConfig::SetEnableAdaptiveOffset(bool enableFlag)
{
    indexlibv2::config::SummaryGroupConfig::SetEnableAdaptiveOffset(enableFlag);
    return mImpl->SetEnableAdaptiveOffset(enableFlag);
}
}} // namespace indexlib::config
