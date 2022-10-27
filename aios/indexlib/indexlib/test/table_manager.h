#ifndef __INDEXLIB_TABLE_MANAGER_H
#define __INDEXLIB_TABLE_MANAGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/index_base/index_meta/version.h"
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, IndexBuilder);
DECLARE_REFERENCE_CLASS(testlib, Result);
DECLARE_REFERENCE_CLASS(test, PartitionStateMachine);

IE_NAMESPACE_BEGIN(test);

class TableManager
{
public:
    enum OfflineBuildFlag
    {
        OFB_NEED_DEPLOY = 0x2,
        OFB_NEED_REOPEN = 0x4,
        OFB_NO_DEPLOY = 0x0,
        OFB_DEFAULT = OFB_NEED_DEPLOY | OFB_NEED_REOPEN
    };

    enum SearchFlag
    {
        SF_ONLINE = 0x1,
        SF_OFFLINE = 0x2
    };

    enum ReOpenFlag
    {
        RO_NORMAL = 0x1,
        RO_FORCE = 0x2
    };
    
public:
    TableManager(const config::IndexPartitionSchemaPtr& schema,
                 const config::IndexPartitionOptions& options,
                 const std::string& indexPluginPath,
                 const std::string& offlineIndexDir,
                 const std::string& onlineIndexDir);
    ~TableManager();
public:
    bool Init(const util::KeyValueMap& psmOptions = {});
    bool BuildFull(const std::string& fullDocString,
                   OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);

    bool BuildInc(const std::string& incDocString,
                  OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);
    bool BuildRt(const std::string& docString);
    bool BuildIncAndRt(const std::string& docString,
                       OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);
    
    bool DeployAndLoadVersion(versionid_t versionId = INVALID_VERSION);
    bool DeployVersion(versionid_t versionId = INVALID_VERSION);
    bool ReOpenVersion(versionid_t versionId = INVALID_VERSION,
                       ReOpenFlag falg = ReOpenFlag::RO_NORMAL);

    testlib::ResultPtr Search(const std::string& query,
                              SearchFlag flag = SearchFlag::SF_ONLINE) const;

    bool CheckResult(const std::vector<std::string>& queryVec,
                     const std::vector<std::string>& expectResultVec) const;

    void StopRealtimeBuild();
 
private:
    partition::IndexPartitionPtr PrepareOnlinePartition(
        const partition::IndexPartitionResourcePtr& onlineResource, versionid_t versionId);
    void DoDeployVersion(const index_base::IndexFileList& dpMeta,
                         const index_base::Version& targetVersion,
                         const std::string& srcRoot,
                         const std::string& dstRoot);

    bool DoOfflineBuild(const std::string& docString,
                        bool isFullBuild,
                        TableManager::OfflineBuildFlag flag);

    partition::IndexBuilderPtr InitOnlineBuilder(const partition::IndexPartitionPtr& onlinePartition); 
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mPluginRoot;
    std::string mOfflineIndexRoot;
    std::string mOnlineIndexRoot;
    misc::MetricProviderPtr mMetricProvider;    
    PartitionStateMachinePtr mOfflinePsm;
    partition::IndexPartitionResourcePtr mOnlineResource;
    partition::IndexPartitionPtr mOnlinePartition;
    partition::IndexBuilderPtr mOnlineBuilder; 
    volatile bool mOnlineIsReady;
    std::map<std::string, std::string> mPsmOptions;
  
private:
    mutable autil::ThreadMutex mDeployLock;
    mutable autil::ThreadMutex mOfflineBuildLock;
    mutable autil::ThreadMutex mOnlineBuildLock;
    mutable autil::ThreadMutex mOpenLock;
    mutable autil::ThreadMutex mSearchLock;    

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableManager);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TABLE_MANAGER_H
