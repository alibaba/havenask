#include "indexlib/config/offline_config_base.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OfflineConfigBase);

OfflineConfigBase::OfflineConfigBase()
    : enableRecoverIndex(true)
    , recoverType(RecoverStrategyType::RST_SEGMENT_LEVEL)
    , fullIndexStoreKKVTs(false)
{
}

OfflineConfigBase::OfflineConfigBase(
        const BuildConfig& buildConf,
        const MergeConfig& mergeConf)
    : buildConfig(buildConf)
    , mergeConfig(mergeConf)
    , enableRecoverIndex(true)
    , recoverType(RecoverStrategyType::RST_SEGMENT_LEVEL)
    , fullIndexStoreKKVTs(false)
{
}

OfflineConfigBase::~OfflineConfigBase() 
{
}

void OfflineConfigBase::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("build_config", buildConfig, buildConfig);
    json.Jsonize("merge_config", mergeConfig, mergeConfig);
    json.Jsonize("full_index_store_kkv_ts", fullIndexStoreKKVTs, fullIndexStoreKKVTs);
    json.Jsonize("offline_reader_config", readerConfig, readerConfig);
}

void OfflineConfigBase::Check() const
{
    buildConfig.Check();
    mergeConfig.Check();
}

IE_NAMESPACE_END(config);

