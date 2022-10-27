#include "indexlib/config/merge_config.h"
#include "indexlib/config/impl/merge_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, MergeConfig);

MergeConfig::MergeConfig()
{
    mImpl.reset(new MergeConfigImpl);
}

MergeConfig::MergeConfig(const MergeConfig& other)
    : MergeConfigBase(other)
{
    mImpl.reset(new MergeConfigImpl(*other.mImpl));
}

MergeConfig::~MergeConfig() {}

void MergeConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    MergeConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

void MergeConfig::Check() const
{
    MergeConfigBase::Check();
    mImpl->Check();
}

bool MergeConfig::IsArchiveFileEnable() const
{ return mImpl->IsArchiveFileEnable(); }

void MergeConfig::EnableArchiveFile() const
{ return mImpl->EnableArchiveFile(); }

void MergeConfig::operator=(const MergeConfig& other)
{
     *(MergeConfigBase*)this = (const MergeConfigBase&)other;
     *mImpl = *(other.mImpl);
}

ModuleClassConfig& MergeConfig::GetSplitSegmentConfig()
{
    return mImpl->splitSegmentConfig;
}
bool MergeConfig::GetEnablePackageFile() const
{
    return mImpl->enablePackageFile;
}
const PackageFileTagConfigList& MergeConfig::GetPackageFileTagConfigList() const
{
    return mImpl->packageFileTagConfigList;
}
uint32_t MergeConfig::GetCheckpointInterval() const
{
    return mImpl->checkpointInterval;
}

void MergeConfig::SetEnablePackageFile(bool value)
{
    mImpl->enablePackageFile = value;
}
void MergeConfig::SetCheckpointInterval(uint32_t value)
{
    mImpl->checkpointInterval = value;
}
void MergeConfig::SetEnableInPlanMultiSegmentParallel(bool value)
{
    mImpl->enableInPlanMultiSegmentParallel = value;
}
bool MergeConfig::EnableInPlanMultiSegmentParallel() const
{
    return mImpl->enableInPlanMultiSegmentParallel;
}

IE_NAMESPACE_END(config);
