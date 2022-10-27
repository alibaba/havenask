#include "indexlib/config/offline_config.h"
#include "indexlib/config/impl/offline_config_impl.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OfflineConfig);

OfflineConfig::OfflineConfig()
{
    mImpl.reset(new OfflineConfigImpl);
}

OfflineConfig::OfflineConfig(
    const BuildConfig& buildConf, const MergeConfig& mergeConf)
    : OfflineConfigBase(buildConf, mergeConf)
{
    mImpl.reset(new OfflineConfigImpl);
}

OfflineConfig::OfflineConfig(const OfflineConfig& other)
    : OfflineConfigBase(other)
{
    mImpl.reset(new OfflineConfigImpl(*other.mImpl));
}

OfflineConfig::~OfflineConfig() 
{
}

void OfflineConfig::RebuildTruncateIndex()
{ mImpl->RebuildTruncateIndex(); }

void OfflineConfig::RebuildAdaptiveBitmap()
{ mImpl->RebuildAdaptiveBitmap(); }

bool OfflineConfig::NeedRebuildTruncateIndex() const
{ return mImpl->NeedRebuildTruncateIndex(); }

bool OfflineConfig::NeedRebuildAdaptiveBitmap() const
{ return mImpl->NeedRebuildAdaptiveBitmap(); }

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

const LoadConfigList& OfflineConfig::GetLoadConfigList() const
{
    return mImpl->GetLoadConfigList();
}

void OfflineConfig::SetLoadConfigList(const LoadConfigList& loadConfigList)
{
    mImpl->SetLoadConfigList(loadConfigList);
}

const config::ModuleInfos& OfflineConfig::GetModuleInfos() const
{
    return mImpl->GetModuleInfos();
}

const storage::RaidConfigPtr& OfflineConfig::GetRaidConfig() const
{
    return buildConfig.GetRaidConfig();
}


IE_NAMESPACE_END(config);

