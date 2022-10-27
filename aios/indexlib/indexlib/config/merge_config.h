#ifndef __INDEXLIB_MERGE_CONFIG_H
#define __INDEXLIB_MERGE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_config_base.h"
#include "indexlib/config/module_class_config.h"

DECLARE_REFERENCE_CLASS(config, MergeConfigImpl);
DECLARE_REFERENCE_CLASS(config, PackageFileTagConfigList);

IE_NAMESPACE_BEGIN(config);

class MergeConfig : public MergeConfigBase
{
public:
    static_assert(sizeof(MergeConfigBase) == 144, "MergeConfigBase size change!");
    
public:
    MergeConfig();
    MergeConfig(const MergeConfig& other);
    ~MergeConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

    void operator=(const MergeConfig& other);
    
public:
    using MergeConfigBase::DEFAULT_MAX_MEMORY_USE_FOR_MERGE;
    using MergeConfigBase::DEFAULT_MERGE_THREAD_COUNT;
    using MergeConfigBase::MAX_MERGE_THREAD_COUNT;
    using MergeConfigBase::DEFAULT_MERGE_STRATEGY;
    using MergeConfigBase::DEFAULT_UNIQ_ENCODE_PACK_ATTR_MERGE_BUFFER_SIZE;

public:
    ModuleClassConfig& GetSplitSegmentConfig();
    uint32_t GetCheckpointInterval() const;
    bool GetEnablePackageFile() const;
    const PackageFileTagConfigList& GetPackageFileTagConfigList() const;
    void SetEnablePackageFile(bool value);
    void SetCheckpointInterval(uint32_t value);

    void SetEnableInPlanMultiSegmentParallel(bool value);
    bool EnableInPlanMultiSegmentParallel() const;
    bool IsArchiveFileEnable() const;
    void EnableArchiveFile() const;
private:
    // TODO: merge config impl
    MergeConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MERGE_CONFIG_H
