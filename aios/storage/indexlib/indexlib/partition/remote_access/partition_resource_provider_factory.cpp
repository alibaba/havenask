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
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionResourceProviderFactory);

PartitionResourceProviderFactory::PartitionResourceProviderFactory() {}

PartitionResourceProviderFactory::~PartitionResourceProviderFactory() {}

PartitionResourceProviderPtr PartitionResourceProviderFactory::DeclareProvider(const string& indexPath,
                                                                               const IndexPartitionOptions& options,
                                                                               const string& pluginPath,
                                                                               versionid_t targetVersionId)
{
    string normalPath = PathUtil::NormalizePath(indexPath);
    versionid_t versionId = GetTargetVersionId(normalPath, targetVersionId);
    if (versionId == INVALID_VERSIONID) {
        IE_LOG(ERROR, "not found valid version at [%s]!", normalPath.c_str());
        return PartitionResourceProviderPtr();
    }

    IE_LOG(INFO, "checking cached provider!");
    ScopedLock lock(mLock);
    for (size_t i = 0; i < mProviderVec.size(); i++) {
        if (MatchProvider(mProviderVec[i], normalPath, targetVersionId)) {
            IE_LOG(INFO, "find cached provider!");
            return mProviderVec[i];
        }
    }

    IE_LOG(INFO, "create new provider!");
    PartitionResourceProviderPtr provider = CreateProvider(normalPath, options, pluginPath, targetVersionId);
    if (provider) {
        mProviderVec.push_back(provider);
    }
    return provider;
}

PartitionResourceProviderPtr PartitionResourceProviderFactory::CreateProvider(const string& indexPath,
                                                                              const IndexPartitionOptions& options,
                                                                              const string& pluginPath,
                                                                              versionid_t targetVersionId)
{
    string normalizedIndexPath = PathUtil::NormalizePath(indexPath);
    string normalizedPluginPath = PathUtil::NormalizePath(pluginPath);
    versionid_t versionId = GetTargetVersionId(normalizedIndexPath, targetVersionId);
    if (versionId == INVALID_VERSIONID) {
        IE_LOG(ERROR, "not found valid version at [%s]!", normalizedIndexPath.c_str());
        return PartitionResourceProviderPtr();
    }

    PartitionResourceProviderPtr provider(new PartitionResourceProvider(options));
    if (!provider->Init(normalizedIndexPath, targetVersionId, normalizedPluginPath)) {
        IE_LOG(ERROR, "PartitionResourceProvider init fail, path [%s] version [%d]!", normalizedIndexPath.c_str(),
               targetVersionId);
        return PartitionResourceProviderPtr();
    }
    return provider;
}

versionid_t PartitionResourceProviderFactory::GetTargetVersionId(const string& indexPath, versionid_t versionId)
{
    if (versionId != INVALID_VERSIONID) {
        return versionId;
    }
    Version version;
    VersionLoader::GetVersionS(indexPath, version, INVALID_VERSIONID);
    return version.GetVersionId();
}

bool PartitionResourceProviderFactory::MatchProvider(const PartitionResourceProviderPtr& provider,
                                                     const string& indexPath, versionid_t versionId)
{
    return (versionId == provider->GetVersion().GetVersionId()) && (provider->GetIndexPath() == indexPath);
}

void PartitionResourceProviderFactory::Reset()
{
    IE_LOG(INFO, "Reset PartitionResourceProviderFactory!");
    ScopedLock lock(mLock);
    mProviderVec.clear();
}
}} // namespace indexlib::partition
