#include <autil/StringUtil.h>
#include "indexlib/config/build_config.h"
#include "indexlib/config/impl/build_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, BuildConfig);

BuildConfig::BuildConfig()
{
    mImpl.reset(new BuildConfigImpl);
}

BuildConfig::BuildConfig(const BuildConfig& other)
    : BuildConfigBase(other)
{
    mImpl.reset(new BuildConfigImpl(*other.mImpl));
}

BuildConfig::~BuildConfig()
{
}

void BuildConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    BuildConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

void BuildConfig::Check() const
{
    BuildConfigBase::Check();
    mImpl->Check();
}

vector<ModuleClassConfig>& BuildConfig::GetSegmentMetricsUpdaterConfig()
{
    return mImpl->segAttrUpdaterConfig;
}

const storage::RaidConfigPtr& BuildConfig::GetRaidConfig() const
{
    return mImpl->raidConfig;
}

const std::map<std::string, std::string>& BuildConfig::GetCustomizedParams() const
{
    return mImpl->GetCustomizedParams();
}

bool BuildConfig::SetCustomizedParams(const std::string& key, const std::string& value)
{
    return mImpl->SetCustomizedParams(key, value);
}

bool BuildConfig::operator==(const BuildConfig& other) const
{
    return (*mImpl == *other.mImpl) &&
        ((BuildConfigBase*)this)->operator==((const BuildConfigBase&)other);
}

void BuildConfig::operator = (const BuildConfig& other)
{
    *(BuildConfigBase*)this = (const BuildConfigBase&)other;
    *mImpl = *(other.mImpl);
}

IE_NAMESPACE_END(config);

