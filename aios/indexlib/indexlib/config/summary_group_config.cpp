#include "indexlib/config/summary_group_config.h"
#include "indexlib/config/impl/summary_group_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SummaryGroupConfig);

SummaryGroupConfig::SummaryGroupConfig(const string& groupName,
                                       summarygroupid_t groupId,
                                       summaryfieldid_t summaryFieldIdBase)
    : mImpl(new SummaryGroupConfigImpl(groupName, groupId, summaryFieldIdBase))
{}

SummaryGroupConfig::~SummaryGroupConfig()
{}

void SummaryGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

const string& SummaryGroupConfig::GetGroupName() const
{
    return mImpl->GetGroupName();
}
void SummaryGroupConfig::SetGroupName(const string& groupName)
{
    mImpl->SetGroupName(groupName);
}
summarygroupid_t SummaryGroupConfig::GetGroupId() const
{
    return mImpl->GetGroupId();
}
bool SummaryGroupConfig::IsDefaultGroup() const
{
    return mImpl->IsDefaultGroup();
}

void SummaryGroupConfig::SetCompress(bool useCompress,
                                     const string& compressType)
{
    mImpl->SetCompress(useCompress, compressType);
}
bool SummaryGroupConfig::IsCompress() const
{
    return mImpl->IsCompress();
}
const string& SummaryGroupConfig::GetCompressType() const
{
    return mImpl->GetCompressType();
}

bool SummaryGroupConfig::NeedStoreSummary()
{
    return mImpl->NeedStoreSummary();
}
void SummaryGroupConfig::SetNeedStoreSummary(bool needStoreSummary)
{
    return mImpl->SetNeedStoreSummary(needStoreSummary);
}

summaryfieldid_t SummaryGroupConfig::GetSummaryFieldIdBase() const
{
    return mImpl->GetSummaryFieldIdBase();
}

void SummaryGroupConfig::AddSummaryConfig(const SummaryConfigPtr& summaryConfig)
{
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

SummaryGroupConfig::Iterator SummaryGroupConfig::Begin() const
{
    return mImpl->Begin();
}
SummaryGroupConfig::Iterator SummaryGroupConfig::End() const
{
    return mImpl->End();
}
size_t SummaryGroupConfig::GetSummaryFieldsCount() const
{
    return mImpl->GetSummaryFieldsCount();
}

IE_NAMESPACE_END(config);

