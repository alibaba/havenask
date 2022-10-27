#include "indexlib/config/summary_schema.h"
#include "indexlib/config/impl/summary_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SummarySchema);

SummarySchema::SummarySchema()
{
    mImpl.reset(new SummarySchemaImpl());
}

SummaryConfigPtr SummarySchema::GetSummaryConfig(fieldid_t fieldId) const
{
    return mImpl->GetSummaryConfig(fieldId);
}
SummaryConfigPtr SummarySchema::GetSummaryConfig(const string &fieldName) const
{
    return mImpl->GetSummaryConfig(fieldName);
}
void SummarySchema::AddSummaryConfig(const SummaryConfigPtr& summaryConfig,
                                     summarygroupid_t groupId)
{
    mImpl->AddSummaryConfig(summaryConfig, groupId);
}
size_t SummarySchema::GetSummaryCount() const
{
    return mImpl->GetSummaryCount();
}

SummarySchema::Iterator SummarySchema::Begin() const
{
    return mImpl->Begin();
}
SummarySchema::Iterator SummarySchema::End() const
{
    return mImpl->End();
}

bool SummarySchema::IsInSummary(fieldid_t fieldId) const
{
    return mImpl->IsInSummary(fieldId);
}

summaryfieldid_t SummarySchema::GetSummaryFieldId(fieldid_t fieldId) const
{
    return mImpl->GetSummaryFieldId(fieldId);
}

fieldid_t SummarySchema::GetMaxFieldId() const
{
    return mImpl->GetMaxFieldId();
}

void SummarySchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void SummarySchema::AssertEqual(const SummarySchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void SummarySchema::AssertCompatible(const SummarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

bool SummarySchema::NeedStoreSummary()
{
    return mImpl->NeedStoreSummary();
}
void SummarySchema::SetNeedStoreSummary(bool needStoreSummary)
{
    mImpl->SetNeedStoreSummary(needStoreSummary);
}
void SummarySchema::SetNeedStoreSummary(fieldid_t fieldId)
{
    mImpl->SetNeedStoreSummary(fieldId);
}

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
summarygroupid_t SummarySchema::GetSummaryGroupConfigCount() const
{
    return mImpl->GetSummaryGroupConfigCount();
}

IE_NAMESPACE_END(config);

