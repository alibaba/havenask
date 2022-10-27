#ifndef __INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H
#define __INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
IE_NAMESPACE_BEGIN(partition);

class PartitionResourceProviderFactory :
    public misc::Singleton<PartitionResourceProviderFactory>
{
public:
    PartitionResourceProviderFactory();
    ~PartitionResourceProviderFactory();
    
public:
    // use cached provider if exist
    // create provider if not exist & put in cache
    PartitionResourceProviderPtr DeclareProvider(
            const std::string& indexPath,
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
    PartitionResourceProviderPtr CreateProvider(
            const std::string& indexPath,
            const config::IndexPartitionOptions& options,
            const std::string& pluginPath,
            versionid_t targetVersionId = INVALID_VERSION);

private:
    versionid_t GetTargetVersionId(const std::string& indexPath, versionid_t version);
    bool MatchProvider(const PartitionResourceProviderPtr& provider,
                       const std::string& indexPath, versionid_t version);
            
private:
    typedef std::vector<PartitionResourceProviderPtr> ProviderVec;
    ProviderVec mProviderVec;
    autil::ThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionResourceProviderFactory);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_RESOURCE_PROVIDER_FACTORY_H
