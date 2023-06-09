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
#ifndef __INDEXLIB_TABLE_MANAGER_H
#define __INDEXLIB_TABLE_MANAGER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, IndexBuilder);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(test, Result);
DECLARE_REFERENCE_CLASS(test, PartitionStateMachine);

namespace indexlibv2 {
namespace framework {
struct VersionDeployDescription;
}
namespace config {
class TabletOptions;
}
} // namespace indexlibv2

namespace indexlib { namespace test {

class TableManager
{
public:
    enum OfflineBuildFlag {
        OFB_NEED_DEPLOY = 0x2,
        OFB_NEED_REOPEN = 0x4,
        OFB_NO_DEPLOY = 0x0,
        OFB_DEFAULT = OFB_NEED_DEPLOY | OFB_NEED_REOPEN
    };

    enum SearchFlag { SF_ONLINE = 0x1, SF_OFFLINE = 0x2 };

    enum ReOpenFlag { RO_NORMAL = 0x1, RO_FORCE = 0x2 };

public:
    TableManager(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                 const std::string& indexPluginPath, const std::string& offlineIndexDir,
                 const std::string& onlineIndexDir);
    ~TableManager();

public:
    bool Init(const util::KeyValueMap& psmOptions = {}, const std::string& onlineTabletConfigStr = "");
    bool BuildFull(const std::string& fullDocString, OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);

    bool BuildInc(const std::string& incDocString, OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);
    bool BuildRt(const std::string& docString);
    bool BuildIncAndRt(const std::string& docString, OfflineBuildFlag flag = OfflineBuildFlag::OFB_DEFAULT);

    bool DeployAndLoadVersion(versionid_t versionId = INVALID_VERSION);
    bool DeployVersion(versionid_t versionId = INVALID_VERSION);
    bool LegacyDeployVersion(versionid_t versionId = INVALID_VERSION);
    bool ReOpenVersion(versionid_t versionId = INVALID_VERSION, ReOpenFlag falg = ReOpenFlag::RO_NORMAL);

    test::ResultPtr Search(const std::string& query, SearchFlag flag = SearchFlag::SF_ONLINE) const;

    bool CheckResult(const std::vector<std::string>& queryVec, const std::vector<std::string>& expectResultVec) const;

    void StopRealtimeBuild();

    static bool LoadDeployDone(const std::string& doneFile,
                               indexlibv2::framework::VersionDeployDescription& versionDpDescription);
    static bool CheckDeployDone(const std::string& versionFile, const std::string& doneFile,
                                const indexlibv2::framework::VersionDeployDescription& targetDpDescription);
    static bool MarkDeployDone(const std::string& doneFile,
                               const indexlibv2::framework::VersionDeployDescription& targetDpDescription);

    static bool CleanDeployDone(const std::string& doneFile);

    static std::string GetVersionFileName(versionid_t versionI);

private:
    partition::IndexPartitionPtr PrepareOnlinePartition(const partition::IndexPartitionResourcePtr& onlineResource,
                                                        versionid_t versionId);
    void DoDeployVersion(const file_system::IndexFileList& dpMeta, const index_base::Version& targetVersion,
                         const std::string& srcRoot, const std::string& dstRoot);

    bool DoOfflineBuild(const std::string& docString, bool isFullBuild, TableManager::OfflineBuildFlag flag);

    partition::IndexBuilderPtr InitOnlineBuilder(const partition::IndexPartitionPtr& onlinePartition);
    partition::IndexPartitionReaderPtr GetReader() const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mPluginRoot;
    std::string mOfflineIndexRoot;
    std::string mOnlineIndexRoot;
    util::MetricProviderPtr mMetricProvider;
    PartitionStateMachinePtr mOfflinePsm;
    partition::IndexPartitionResourcePtr mOnlineResource;
    partition::IndexPartitionPtr mOnlinePartition;
    partition::IndexBuilderPtr mOnlineBuilder;
    volatile bool mOnlineIsReady;
    std::map<std::string, std::string> mPsmOptions;
    std::string mTabletConfigStr;

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
}} // namespace indexlib::test

#endif //__INDEXLIB_TABLE_MANAGER_H
