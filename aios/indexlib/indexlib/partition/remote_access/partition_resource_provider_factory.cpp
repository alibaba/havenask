#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionResourceProviderFactory);

PartitionResourceProviderFactory::PartitionResourceProviderFactory() 
{
}

PartitionResourceProviderFactory::~PartitionResourceProviderFactory() 
{
}

PartitionResourceProviderPtr PartitionResourceProviderFactory::DeclareProvider(
        const string& indexPath, const IndexPartitionOptions& options,
        const string& pluginPath, versionid_t targetVersionId)
{
    string normalPath = PathUtil::NormalizePath(indexPath);
    versionid_t versionId = GetTargetVersionId(normalPath, targetVersionId);
    if (versionId == INVALID_VERSION)
    {
        IE_LOG(ERROR, "not found valid version!");
        return PartitionResourceProviderPtr();
    }

    IE_LOG(INFO, "checking cached provider!");
    ScopedLock lock(mLock);
    for (size_t i = 0; i < mProviderVec.size(); i++)
    {
        if (MatchProvider(mProviderVec[i], normalPath, versionId))
        {
            IE_LOG(INFO, "find cached provider!");
            return mProviderVec[i];
        }
    }

    IE_LOG(INFO, "create new provider!");
    PartitionResourceProviderPtr provider = CreateProvider(normalPath, options, pluginPath, versionId);
    if (provider)
    {
        mProviderVec.push_back(provider);
    }
    return provider;
}

PartitionResourceProviderPtr PartitionResourceProviderFactory::CreateProvider(
        const string& indexPath, const IndexPartitionOptions& options,
        const string& pluginPath, versionid_t targetVersionId)
{
    string normalizedIndexPath = PathUtil::NormalizePath(indexPath);
    string normalizedPluginPath = PathUtil::NormalizePath(pluginPath);
    versionid_t versionId = GetTargetVersionId(normalizedIndexPath, targetVersionId);
    if (versionId == INVALID_VERSION)
    {
        IE_LOG(ERROR, "not found valid version!");
        return PartitionResourceProviderPtr();
    }

    PartitionResourceProviderPtr provider(new PartitionResourceProvider(options));
    if (!provider->Init(normalizedIndexPath, versionId, normalizedPluginPath))
    {
        IE_LOG(ERROR, "PartitionResourceProvider init fail, path [%s] version [%d]!",
               normalizedIndexPath.c_str(), versionId);
        return PartitionResourceProviderPtr();
    }
    return provider;
}

versionid_t PartitionResourceProviderFactory::GetTargetVersionId(
        const string& indexPath, versionid_t versionId)
{
    if (versionId != INVALID_VERSION)
    {
        return versionId;
    }
    Version version;
    VersionLoader::GetVersion(indexPath, version, INVALID_VERSION);
    return version.GetVersionId();
}

bool PartitionResourceProviderFactory::MatchProvider(
        const PartitionResourceProviderPtr& provider,
        const string& indexPath, versionid_t versionId)
{
    return (versionId == provider->GetVersion().GetVersionId())
        && (provider->GetIndexPath() == indexPath);
}

void PartitionResourceProviderFactory::Reset()
{
    IE_LOG(INFO, "Reset PartitionResourceProviderFactory!");
    ScopedLock lock(mLock);
    mProviderVec.clear();
}

IE_NAMESPACE_END(partition);

