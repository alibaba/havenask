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
#include "indexlib/config/summary_schema.h"

#include "indexlib/config/impl/summary_schema_impl.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SummarySchema);

SummarySchema::SummarySchema() { mImpl.reset(new SummarySchemaImpl()); }

std::shared_ptr<SummaryConfig> SummarySchema::GetSummaryConfig(fieldid_t fieldId) const
{
    return mImpl->GetSummaryConfig(fieldId);
}
std::shared_ptr<SummaryConfig> SummarySchema::GetSummaryConfig(const string& fieldName) const
{
    return mImpl->GetSummaryConfig(fieldName);
}
void SummarySchema::AddSummaryConfig(const std::shared_ptr<SummaryConfig>& summaryConfig, summarygroupid_t groupId)
{
    mImpl->AddSummaryConfig(summaryConfig, groupId);
}
size_t SummarySchema::GetSummaryCount() const { return mImpl->GetSummaryCount(); }

SummarySchema::Iterator SummarySchema::Begin() const { return mImpl->Begin(); }
SummarySchema::Iterator SummarySchema::End() const { return mImpl->End(); }

bool SummarySchema::IsInSummary(fieldid_t fieldId) const { return mImpl->IsInSummary(fieldId); }

summaryfieldid_t SummarySchema::GetSummaryFieldId(fieldid_t fieldId) const { return mImpl->GetSummaryFieldId(fieldId); }

fieldid_t SummarySchema::GetMaxFieldId() const { return mImpl->GetMaxFieldId(); }

void SummarySchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }
void SummarySchema::AssertEqual(const SummarySchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }
void SummarySchema::AssertCompatible(const SummarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

bool SummarySchema::NeedStoreSummary() { return mImpl->NeedStoreSummary(); }
void SummarySchema::SetNeedStoreSummary(bool needStoreSummary) { mImpl->SetNeedStoreSummary(needStoreSummary); }
void SummarySchema::SetNeedStoreSummary(fieldid_t fieldId) { mImpl->SetNeedStoreSummary(fieldId); }

summarygroupid_t SummarySchema::CreateSummaryGroup(const string& groupName)
{
    return mImpl->CreateSummaryGroup(groupName);
}
summarygroupid_t SummarySchema::FieldIdToSummaryGroupId(fieldid_t fieldId) const
{
    return mImpl->FieldIdToSummaryGroupId(fieldId);
}
summarygroupid_t SummarySchema::GetSummaryGroupId(const string& groupName) const
{
    return mImpl->GetSummaryGroupId(groupName);
}
const SummaryGroupConfigPtr& SummarySchema::GetSummaryGroupConfig(const string& groupName) const
{
    return mImpl->GetSummaryGroupConfig(groupName);
}
const SummaryGroupConfigPtr& SummarySchema::GetSummaryGroupConfig(summarygroupid_t groupId) const
{
    return mImpl->GetSummaryGroupConfig(groupId);
}
summarygroupid_t SummarySchema::GetSummaryGroupConfigCount() const { return mImpl->GetSummaryGroupConfigCount(); }

void SummarySchema::DisableAllFields() { return mImpl->DisableAllFields(); }

bool SummarySchema::IsAllFieldsDisabled() const { return mImpl->IsAllFieldsDisabled(); }

std::unique_ptr<indexlibv2::config::SummaryIndexConfig> SummarySchema::MakeSummaryIndexConfigV2()
{
    std::vector<std::shared_ptr<indexlibv2::config::SummaryGroupConfig>> groupConfigs;
    for (size_t i = 0; i < GetSummaryGroupConfigCount(); ++i) {
        groupConfigs.emplace_back(GetSummaryGroupConfig(i));
    }

    auto summaryIndexConfig = std::make_unique<indexlibv2::config::SummaryIndexConfig>(groupConfigs);
    if (IsAllFieldsDisabled()) {
        summaryIndexConfig->DisableAllFields();
    }
    if (!NeedStoreSummary()) {
        summaryIndexConfig->SetNeedStoreSummary(false);
    }
    return summaryIndexConfig;
}
}} // namespace indexlib::config
