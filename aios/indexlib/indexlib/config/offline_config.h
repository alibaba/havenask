#ifndef __INDEXLIB_OFFLINE_CONFIG_H
#define __INDEXLIB_OFFLINE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/offline_config_base.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/module_info.h"
#include "indexlib/storage/raid_config.h"

DECLARE_REFERENCE_CLASS(config, OfflineConfigImpl);
IE_NAMESPACE_BEGIN(config);

class OfflineConfig : public OfflineConfigBase
{
    static_assert(sizeof(OfflineConfigBase) == 312, "OfflineConfigBase size change!");
public:
    OfflineConfig();
    OfflineConfig(const OfflineConfig& other);
    OfflineConfig(const BuildConfig& buildConf,
                  const MergeConfig& mergeConf);

    ~OfflineConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator=(const OfflineConfig& other);
    
    void RebuildTruncateIndex();
    void RebuildAdaptiveBitmap();
    bool NeedRebuildTruncateIndex() const;
    bool NeedRebuildAdaptiveBitmap() const;
    const LoadConfigList& GetLoadConfigList() const;
    void SetLoadConfigList(const LoadConfigList& loadConfigList);

    const config::ModuleInfos& GetModuleInfos() const;
    const storage::RaidConfigPtr& GetRaidConfig() const;

private:
    OfflineConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_OFFLINE_CONFIG_H
