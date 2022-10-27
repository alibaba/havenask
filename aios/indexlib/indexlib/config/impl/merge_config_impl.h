#ifndef __INDEXLIB_MERGE_CONFIG_IMPL_H
#define __INDEXLIB_MERGE_CONFIG_IMPL_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/config/package_file_tag_config_list.h"

IE_NAMESPACE_BEGIN(config);

class MergeConfigImpl : public autil::legacy::Jsonizable
{
public:
    MergeConfigImpl();
    MergeConfigImpl(const MergeConfigImpl& other);
    ~MergeConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator = (const MergeConfigImpl& other);
    bool IsArchiveFileEnable() const { return enableArchiveFile; }
    void EnableArchiveFile() { enableArchiveFile = true; }
public:
    ModuleClassConfig splitSegmentConfig;
    PackageFileTagConfigList packageFileTagConfigList; // valid when enablePackageFile == true
    uint32_t checkpointInterval;  // valid when enableMergeItemCheckPoint == true && enablePackageFile == true
    bool enablePackageFile;
    bool enableInPlanMultiSegmentParallel;
    bool enableArchiveFile;

private:
    static const uint32_t DEFAULT_CHECKPOINT_INTERVAL = 600; // 600s

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MERGE_CONFIG_IMPL_H
