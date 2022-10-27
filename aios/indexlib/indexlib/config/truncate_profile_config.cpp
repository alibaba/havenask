#include "indexlib/config/truncate_profile_config.h"
#include "indexlib/config/impl/truncate_profile_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateProfileConfig);

TruncateProfileConfig::TruncateProfileConfig()
    : mImpl(new TruncateProfileConfigImpl)
{
}

// for test
TruncateProfileConfig::TruncateProfileConfig(
    const std::string& profileName, const std::string& sortDesp)
    : mImpl(new TruncateProfileConfigImpl(profileName, sortDesp))
{
}

TruncateProfileConfig::~TruncateProfileConfig()
{
}

void TruncateProfileConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void TruncateProfileConfig::Check() const
{
    mImpl->Check();
}

void TruncateProfileConfig::AssertEqual(const TruncateProfileConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const string& TruncateProfileConfig::GetTruncateProfileName() const
{
    return mImpl->GetTruncateProfileName();
}

const SortParams& TruncateProfileConfig::GetTruncateSortParams() const
{
    return mImpl->GetTruncateSortParams();
}

IE_NAMESPACE_END(config);

