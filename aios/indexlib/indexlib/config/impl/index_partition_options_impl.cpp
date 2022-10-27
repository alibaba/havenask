#include "indexlib/config/impl/index_partition_options_impl.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/configurator_define.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexPartitionOptionsImpl);

IndexPartitionOptionsImpl::IndexPartitionOptionsImpl()
    : mIsOnline(true)
    , mNeedRewriteFieldType(true)
{
}

IndexPartitionOptionsImpl::IndexPartitionOptionsImpl(const IndexPartitionOptionsImpl& other)
    : mOfflineConfig(other.mOfflineConfig)
    , mOnlineConfig(other.mOnlineConfig)
    , mIsOnline(other.mIsOnline)
    , mReservedVersionSet(other.mReservedVersionSet)
    , mOngoingModifyOpIds(other.mOngoingModifyOpIds)
    , mNeedRewriteFieldType(other.mNeedRewriteFieldType)
{
}

IndexPartitionOptionsImpl::~IndexPartitionOptionsImpl() 
{
}

// for legacy index_format_version
void IndexPartitionOptionsImpl::SetEnablePackageFile(bool enablePackageFile)
{
    mOnlineConfig.buildConfig.enablePackageFile = enablePackageFile;
    mOfflineConfig.buildConfig.enablePackageFile = enablePackageFile;
}

void IndexPartitionOptionsImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("online_index_config", mOnlineConfig, mOnlineConfig);
    json.Jsonize("offline_index_config", mOfflineConfig, mOfflineConfig);
}

void IndexPartitionOptionsImpl::Check() const
{
    mOnlineConfig.Check();
    mOfflineConfig.Check();
}

void IndexPartitionOptionsImpl::RewriteForSchema(
        const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    TableType tableType = schema->GetTableType();
    if (tableType == tt_index && IsOnline() && !mOngoingModifyOpIds.empty())
    {
        for (auto &id : mOngoingModifyOpIds)
        {
            if (!schema->MarkOngoingModifyOperation(id))
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "mark ongoing modify operation [%d] fail!", id);
            }
        }
    }
    RewriteBuildConfig(tableType, GetBuildConfig(false));
    RewriteBuildConfig(tableType, GetBuildConfig(true));
}

void IndexPartitionOptionsImpl::RewriteBuildConfig(
        TableType tableType, BuildConfig& buildConfig)
{
    if (buildConfig.levelTopology == topo_default)
    {
        buildConfig.levelTopology = 
                (tableType == tt_kv || tableType == tt_kkv) ? topo_hash_mod : topo_sequence;
        IE_LOG(INFO, "default levelTopology use [%s]",
               TopologyToStr(buildConfig.levelTopology).c_str());
    }

    if (buildConfig.levelTopology == topo_hash_mod && buildConfig.levelNum <= 1)
    {
        buildConfig.levelNum = 2;
        IE_LOG(INFO, "kv & kkv table use topo_hash_mode, levelNum should >= 1, rewrite to 2");
    }
}

void IndexPartitionOptionsImpl::operator=(const IndexPartitionOptionsImpl& other)
{
    mOfflineConfig = other.mOfflineConfig;
    mOnlineConfig = other.mOnlineConfig;
    mIsOnline = other.mIsOnline;
    mReservedVersionSet = other.mReservedVersionSet;
    mOngoingModifyOpIds = other.mOngoingModifyOpIds;
}

IE_NAMESPACE_END(config);

