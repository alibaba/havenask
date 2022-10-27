#include "indexlib/config/summary_config.h"
#include "indexlib/config/impl/summary_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, SummaryConfig);

SummaryConfig::SummaryConfig()
    : mImpl(new SummaryConfigImpl())
{
}
SummaryConfig::~SummaryConfig()
{
}
FieldType SummaryConfig::GetFieldType() const
{
    return mImpl->GetFieldType();
}
void SummaryConfig::SetFieldConfig(const FieldConfigPtr& fieldConfig)
{
    mImpl->SetFieldConfig(fieldConfig);
}
FieldConfigPtr SummaryConfig::GetFieldConfig() const
{
    return mImpl->GetFieldConfig();
}
fieldid_t SummaryConfig::GetFieldId() const
{
    return mImpl->GetFieldId();
}
const string& SummaryConfig::GetSummaryName() const
{
    return mImpl->GetSummaryName();
}
void SummaryConfig::AssertEqual(const SummaryConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

IE_NAMESPACE_END(config);

