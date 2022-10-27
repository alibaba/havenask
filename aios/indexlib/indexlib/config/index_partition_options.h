#ifndef __INDEXLIB_INDEX_PARTITION_OPTIONS_H
#define __INDEXLIB_INDEX_PARTITION_OPTIONS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/config/online_config.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptionsImpl);

IE_NAMESPACE_BEGIN(config);

class IndexPartitionOptions : public autil::legacy::Jsonizable
{
public:
    IndexPartitionOptions();
    IndexPartitionOptions(const IndexPartitionOptions& options);
    ~IndexPartitionOptions();

public:
    void SetOnlineConfig(const OnlineConfig& onlineConfig);
    void SetOfflineConfig(const OfflineConfig& offlineConfig);
    const OnlineConfig& GetOnlineConfig() const;
    const OfflineConfig& GetOfflineConfig() const;
    OnlineConfig& GetOnlineConfig();
    OfflineConfig& GetOfflineConfig();
    void SetIsOnline(bool isOnline);
    bool IsOnline() const;
    bool IsOffline() const;
    bool NeedLoadIndex() const;
    const BuildConfig& GetBuildConfig(bool isOnline) const;
    BuildConfig& GetBuildConfig(bool isOnline);
    const BuildConfig& GetBuildConfig() const;
    BuildConfig& GetBuildConfig();
    const MergeConfig& GetMergeConfig() const;
    MergeConfig& GetMergeConfig();
    // for legacy index_format_version
    void SetEnablePackageFile(bool enablePackageFile = true);
    void RewriteForSchema(const IndexPartitionSchemaPtr& schema);
    void SetReservedVersions(const std::set<versionid_t>& versionSet);
    const std::set<versionid_t>& GetReservedVersions() const;

    void SetOngoingModifyOperationIds(const std::vector<schema_opid_t>& ids);
    const std::vector<schema_opid_t>& GetOngoingModifyOperationIds() const;

    bool NeedRewriteFieldType() const;
    void SetNeedRewriteFieldType(bool need);
                          
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void RewriteBuildConfig(TableType tableType, BuildConfig& buildConfig);
    void operator=(const IndexPartitionOptions& options);
private:
    IndexPartitionOptionsImplPtr mImpl;

public:
    bool TEST_mQuickExit;
    bool TEST_mReadOnly;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionOptions);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_PARTITION_OPTIONS_H
