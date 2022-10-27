#ifndef __INDEXLIB_INDEX_PARTITION_OPTIONS_IMPL_H
#define __INDEXLIB_INDEX_PARTITION_OPTIONS_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/config/online_config.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(config);

class IndexPartitionOptionsImpl : public autil::legacy::Jsonizable
{
public:
    IndexPartitionOptionsImpl();
    IndexPartitionOptionsImpl(const IndexPartitionOptionsImpl& other);
    ~IndexPartitionOptionsImpl();

public:
    void SetOnlineConfig(const OnlineConfig& onlineConfig)
    {  mOnlineConfig = onlineConfig; }

    void SetOfflineConfig(const OfflineConfig& offlineConfig)
    { mOfflineConfig = offlineConfig; }

    const OnlineConfig& GetOnlineConfig() const
    { return mOnlineConfig; }

    const OfflineConfig& GetOfflineConfig() const
    { return mOfflineConfig; }

    OnlineConfig& GetOnlineConfig()
    { return mOnlineConfig; }

    OfflineConfig& GetOfflineConfig()
    { return mOfflineConfig; }

    void SetIsOnline(bool isOnline)
    { mIsOnline = isOnline; }

    bool IsOnline() const
    { return mIsOnline; }

    bool IsOffline() const
    { return !IsOnline(); }

    bool NeedLoadIndex() const
    {
        return IsOnline() ? true : GetOfflineConfig().readerConfig.loadIndex;
    }

    const BuildConfig& GetBuildConfig(bool isOnline) const
    {
        if (isOnline)
        {
            return mOnlineConfig.buildConfig;
        }
        return mOfflineConfig.buildConfig;
    }

    BuildConfig& GetBuildConfig(bool isOnline)
    {
        return const_cast<BuildConfig&>(
            static_cast<const IndexPartitionOptionsImpl*>(this)->GetBuildConfig(isOnline));
    }

    const BuildConfig& GetBuildConfig() const
    {
        return GetBuildConfig(mIsOnline);
    }

    BuildConfig& GetBuildConfig()
    {
        return GetBuildConfig(mIsOnline);
    }

    const MergeConfig& GetMergeConfig() const 
    {
        return mOfflineConfig.mergeConfig;
    }

    MergeConfig& GetMergeConfig()
    {
        return mOfflineConfig.mergeConfig;
    }

    // for legacy index_format_version
    void SetEnablePackageFile(bool enablePackageFile = true);

    void RewriteForSchema(const IndexPartitionSchemaPtr& schema);

    void SetReservedVersions(const std::set<versionid_t>& versionSet)
    {
        mReservedVersionSet = versionSet;
    }
    const std::set<versionid_t>& GetReservedVersions() const
    {
        return mReservedVersionSet;
    }

    void SetOngoingModifyOperationIds(const std::vector<schema_opid_t>& ids)
    {
        mOngoingModifyOpIds = ids;
    }
    
    const std::vector<schema_opid_t>& GetOngoingModifyOperationIds() const
    {
        return mOngoingModifyOpIds;
    }
    
    bool NeedRewriteFieldType() const
    {
        return mNeedRewriteFieldType;
    }
    
    void SetNeedRewriteFieldType(bool need)
    {
        mNeedRewriteFieldType = need;
    }
                          
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void RewriteBuildConfig(TableType tableType, BuildConfig& buildConfig);
    void operator = (const IndexPartitionOptionsImpl& other);
private:
    OfflineConfig mOfflineConfig;
    OnlineConfig mOnlineConfig;
    bool mIsOnline;
    std::set<versionid_t> mReservedVersionSet;
    std::vector<schema_opid_t> mOngoingModifyOpIds;
    bool mNeedRewriteFieldType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionOptionsImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_PARTITION_OPTIONS_IMPL_H
