#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/impl/index_partition_options_impl.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexPartitionOptions);

IndexPartitionOptions::IndexPartitionOptions()
    : TEST_mQuickExit(false)
    , TEST_mReadOnly(false)
{
    mImpl.reset(new IndexPartitionOptionsImpl);
}

IndexPartitionOptions::IndexPartitionOptions(const IndexPartitionOptions& other)
    : TEST_mQuickExit(other.TEST_mQuickExit)
    , TEST_mReadOnly(other.TEST_mReadOnly)
{
    mImpl.reset(new IndexPartitionOptionsImpl(*other.mImpl));
}

IndexPartitionOptions::~IndexPartitionOptions() 
{
}

void IndexPartitionOptions::SetOnlineConfig(const OnlineConfig& onlineConfig)
{
    mImpl->SetOnlineConfig(onlineConfig);
}

void IndexPartitionOptions::SetOfflineConfig(const OfflineConfig& offlineConfig)
{
    mImpl->SetOfflineConfig(offlineConfig);
}

const OnlineConfig& IndexPartitionOptions::GetOnlineConfig() const
{
    return mImpl->GetOnlineConfig();
}

const OfflineConfig& IndexPartitionOptions::GetOfflineConfig() const
{
    return mImpl->GetOfflineConfig();
}

OnlineConfig& IndexPartitionOptions::GetOnlineConfig()
{
    return mImpl->GetOnlineConfig();
}

OfflineConfig& IndexPartitionOptions::GetOfflineConfig()
{
    return mImpl->GetOfflineConfig();
}

void IndexPartitionOptions::SetIsOnline(bool isOnline)
{
    mImpl->SetIsOnline(isOnline);
}

bool IndexPartitionOptions::IsOnline() const
{
    return mImpl->IsOnline();
}

bool IndexPartitionOptions::IsOffline() const
{
    return mImpl->IsOffline();
}

bool IndexPartitionOptions::NeedLoadIndex() const
{
    return mImpl->NeedLoadIndex();
}

const BuildConfig& IndexPartitionOptions::GetBuildConfig(bool isOnline) const
{
    return mImpl->GetBuildConfig(isOnline);
}

BuildConfig& IndexPartitionOptions::GetBuildConfig(bool isOnline)
{
    return mImpl->GetBuildConfig(isOnline);
}

const BuildConfig& IndexPartitionOptions::GetBuildConfig() const
{
    return mImpl->GetBuildConfig();
}

BuildConfig& IndexPartitionOptions::GetBuildConfig()
{
    return mImpl->GetBuildConfig();
}

const MergeConfig& IndexPartitionOptions::GetMergeConfig() const 
{
    return mImpl->GetMergeConfig();
}

MergeConfig& IndexPartitionOptions::GetMergeConfig()
{
    return mImpl->GetMergeConfig();
}

void IndexPartitionOptions::SetReservedVersions(const std::set<versionid_t>& versionSet)
{
    return mImpl->SetReservedVersions(versionSet);
}

const std::set<versionid_t>& IndexPartitionOptions::GetReservedVersions() const
{
    return mImpl->GetReservedVersions();
}

void IndexPartitionOptions::SetOngoingModifyOperationIds(const std::vector<schema_opid_t>& ids)
{
    mImpl->SetOngoingModifyOperationIds(ids);
}

const vector<schema_opid_t>& IndexPartitionOptions::GetOngoingModifyOperationIds() const
{
    return mImpl->GetOngoingModifyOperationIds();
}

void IndexPartitionOptions::SetEnablePackageFile(bool enablePackageFile)
{
    return mImpl->SetEnablePackageFile(enablePackageFile);
}

void IndexPartitionOptions::Jsonize(Jsonizable::JsonWrapper& json)
{
    return mImpl->Jsonize(json);
}

void IndexPartitionOptions::Check() const
{
    return mImpl->Check();
}

void IndexPartitionOptions::RewriteForSchema(
        const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    mImpl->RewriteForSchema(schema);
}

void IndexPartitionOptions::RewriteBuildConfig(
        TableType tableType, BuildConfig& buildConfig)
{
    mImpl->RewriteBuildConfig(tableType, buildConfig);
}

void IndexPartitionOptions::operator=(const IndexPartitionOptions& options)
{
    *mImpl = *(options.mImpl);
    TEST_mQuickExit = options.TEST_mQuickExit;
    TEST_mReadOnly = options.TEST_mReadOnly;
}

bool IndexPartitionOptions::NeedRewriteFieldType() const
{
    return mImpl->NeedRewriteFieldType();
}

void IndexPartitionOptions::SetNeedRewriteFieldType(bool need)
{
    mImpl->SetNeedRewriteFieldType(need);
}

IE_NAMESPACE_END(config);

