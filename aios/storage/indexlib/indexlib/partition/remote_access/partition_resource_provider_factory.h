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
#ifndef __INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H
#define __INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/util/Singleton.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
namespace indexlib { namespace partition {

class PartitionResourceProviderFactory : public util::Singleton<PartitionResourceProviderFactory>
{
public:
    PartitionResourceProviderFactory();
    ~PartitionResourceProviderFactory();

public:
    // use cached provider if exist
    // create provider if not exist & put in cache
    // This is an optimized version of CreateProvider and is only used in tf_plugin as of
    // (2021/01/14), so the optimization may not be necessary.
    // TODO: Consider remove this.
    PartitionResourceProviderPtr DeclareProvider(const std::string& indexPath,
                                                 const config::IndexPartitionOptions& options,
                                                 const std::string& pluginPath,
                                                 versionid_t targetVersionId = INVALID_VERSION);

    /*
       remove all providers manully
       should call reset when use DeclareProvider interface
       avoid core dump when auto destruction trigger using static logger
    */
    void Reset();

    // create new provider
    PartitionResourceProviderPtr CreateProvider(const std::string& indexPath,
                                                const config::IndexPartitionOptions& options,
                                                const std::string& pluginPath,
                                                versionid_t targetVersionId = INVALID_VERSION);

private:
    bool MatchProvider(const PartitionResourceProviderPtr& provider, const std::string& indexPath, versionid_t version);
    versionid_t GetTargetVersionId(const std::string& indexPath, versionid_t versionId);

private:
    typedef std::vector<PartitionResourceProviderPtr> ProviderVec;
    ProviderVec mProviderVec;
    autil::ThreadMutex mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionResourceProviderFactory);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H
