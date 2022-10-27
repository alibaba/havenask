#include "indexlib/config/truncate_profile_schema.h"
#include "indexlib/config/impl/truncate_profile_schema_impl.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateProfileSchema);

TruncateProfileSchema::TruncateProfileSchema()
{
    mImpl.reset(new TruncateProfileSchemaImpl());
}

TruncateProfileConfigPtr TruncateProfileSchema::GetTruncateProfileConfig(
    const string& truncateProfileName) const
{
    return mImpl->GetTruncateProfileConfig(truncateProfileName);
}
void TruncateProfileSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void TruncateProfileSchema::AssertEqual(const TruncateProfileSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

TruncateProfileSchema::Iterator TruncateProfileSchema::Begin() const
{
    return mImpl->Begin();
}
TruncateProfileSchema::Iterator TruncateProfileSchema::End() const
{
    return mImpl->End();
}

size_t TruncateProfileSchema::Size() const
{
    return mImpl->Size();
}

const TruncateProfileSchema::TruncateProfileConfigMap& TruncateProfileSchema::GetTruncateProfileConfigMap()
{
    return mImpl->GetTruncateProfileConfigMap();
}

void TruncateProfileSchema::AddTruncateProfileConfig(
    const TruncateProfileConfigPtr& truncateProfileConfig)
{
    mImpl->AddTruncateProfileConfig(truncateProfileConfig);
}

IE_NAMESPACE_END(config);

