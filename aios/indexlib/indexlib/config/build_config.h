#ifndef __INDEXLIB_BUILD_CONFIG_H
#define __INDEXLIB_BUILD_CONFIG_H

#include <tr1/memory>
#include <vector>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/storage/raid_config.h"

DECLARE_REFERENCE_CLASS(config, BuildConfigImpl);
IE_NAMESPACE_BEGIN(config);

class BuildConfig : public BuildConfigBase
{
public:
    static_assert(sizeof(BuildConfigBase) == 80, "BuildConfigBase size change!");
    using BuildConfigBase::BuildPhase;

public:
    BuildConfig();
    BuildConfig(const BuildConfig& other);
    ~BuildConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator == (const BuildConfig& other) const;
    void operator = (const BuildConfig& other);
    using BuildConfigBase::GetEnvBuildTotalMemUse;
    
public:
    using BuildConfigBase::DEFAULT_HASHMAP_INIT_SIZE;
    using BuildConfigBase::DEFAULT_BUILD_TOTAL_MEMORY;
    using BuildConfigBase::DEFAULT_MAX_DOC_COUNT;
    using BuildConfigBase::DEFAULT_KEEP_VERSION_COUNT;
    using BuildConfigBase::DEFAULT_DUMP_THREADCOUNT;
    using BuildConfigBase::MAX_DUMP_THREAD_COUNT;
    using BuildConfigBase::DEFAULT_SHARDING_COLUMN_NUM;
    using BuildConfigBase::DEFAULT_USE_PACKAGE_FILE;
    
public:
    std::vector<ModuleClassConfig>& GetSegmentMetricsUpdaterConfig();
    const storage::RaidConfigPtr& GetRaidConfig() const;
    const std::map<std::string, std::string>& GetCustomizedParams() const;
    bool SetCustomizedParams(const std::string& key, const std::string& value);
    
private:
    // attention: add new parameter to impl
    BuildConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_BUILD_CONFIG_H
