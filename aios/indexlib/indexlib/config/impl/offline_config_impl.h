#ifndef __INDEXLIB_OFFLINE_CONFIG_IMPL_H
#define __INDEXLIB_OFFLINE_CONFIG_IMPL_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/module_info.h"

IE_NAMESPACE_BEGIN(config);

class OfflineConfigImpl : public autil::legacy::Jsonizable
{
public:
    OfflineConfigImpl();
    OfflineConfigImpl(const OfflineConfigImpl& other);
    ~OfflineConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator = (const OfflineConfigImpl& other);
    void RebuildTruncateIndex() { mNeedRebuildTruncateIndex = true; }
    void RebuildAdaptiveBitmap() { mNeedRebuildAdaptiveBitmap = true; }
    bool NeedRebuildTruncateIndex() const { return mNeedRebuildTruncateIndex; }
    bool NeedRebuildAdaptiveBitmap() const { return mNeedRebuildAdaptiveBitmap; }
    const LoadConfigList& GetLoadConfigList() const { return mLoadConfigList; }
    void SetLoadConfigList(const LoadConfigList& loadConfigList);

    const ModuleInfos& GetModuleInfos() const { return mModuleInfos; }

private:
    // TODO: add new config
    bool mNeedRebuildAdaptiveBitmap;
    bool mNeedRebuildTruncateIndex;
    LoadConfigList mLoadConfigList;
    ModuleInfos mModuleInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_OFFLINE_CONFIG_IMPL_H
