#ifndef __INDEXLIB_ONLINE_CONFIG_H
#define __INDEXLIB_ONLINE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/online_config_base.h"

DECLARE_REFERENCE_CLASS(config, DisableFieldsConfig);
DECLARE_REFERENCE_CLASS(config, OnlineConfigImpl);
DECLARE_REFERENCE_CLASS(file_system, LoadConfig);
IE_NAMESPACE_BEGIN(config);

class OnlineConfig : public OnlineConfigBase
{
public:
    static_assert(sizeof(OnlineConfigBase) == 400, "OnlineConfigBase size change!");
    
public:
    using OnlineConfigBase::DEFAULT_MAX_REALTIME_MEMORY_SIZE;
    using OnlineConfigBase::DEFAULT_MAX_REALTIME_DUMP_INTERVAL;
    using OnlineConfigBase::DEFAULT_ONLINE_KEEP_VERSION_COUNT;
    using OnlineConfigBase::DEFAULT_BUILD_TOTAL_MEMORY;
    using OnlineConfigBase::DEFAULT_REALTIME_BUILD_HASHMAP_INIT_SIZE;
    using OnlineConfigBase::DEFAULT_REALTIME_DUMP_THREAD_COUNT;
    using OnlineConfigBase::INVALID_MAX_REOPEN_MEMORY_USE;
    using OnlineConfigBase::DEFAULT_REALTIME_BUILD_USE_PACKAGE_FILE;
    using OnlineConfigBase::DEFAULT_MAX_CUR_READER_RECLAIMABLE_MEM;
    using OnlineConfigBase::DEFAULT_PRINT_METRICS_INTERVAL;
    using OnlineConfigBase::INVALID_MAX_SWAP_MMAP_FILE_SIZE;
    using OnlineConfigBase::DEFAULT_LOAD_PATCH_THREAD_NUM;

public:
    OnlineConfig();
    OnlineConfig(const OnlineConfig& other);
    ~OnlineConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator=(const OnlineConfig& other);

    using OnlineConfigBase::SetMaxReopenMemoryUse;
    using OnlineConfigBase::NeedReadRemoteIndex;
    using OnlineConfigBase::NeedDeployIndex;
    using OnlineConfigBase::GetMaxReopenMemoryUse;
    using OnlineConfigBase::EnableIntervalDump;

public:
    void SetEnableRedoSpeedup(bool enableFlag);
    bool IsRedoSpeedupEnabled() const; 
    bool IsValidateIndexEnabled() const;
    DisableFieldsConfig& GetDisableFieldsConfig();
    const DisableFieldsConfig& GetDisableFieldsConfig() const;
    void SetInitReaderThreadCount(int32_t initReaderThreadCount);
    int32_t GetInitReaderThreadCount() const;
    void SetVersionTsAlignment(int64_t ts); 
    int64_t GetVersionTsAlignment() const;    
    bool IsReopenRetryOnIOExceptionEnabled() const;

public:
    static LoadConfig CreateDisableLoadConfig(const std::string& name,
                                              const std::vector<std::string>& indexes,
                                              const std::vector<std::string>& attributes,
                                              const std::vector<std::string>& packAttrs);
    
    static LoadConfig CreateDisableLoadConfig(const std::string& name,
                                              const std::set<std::string>& indexes,
                                              const std::set<std::string>& attributes,
                                              const std::set<std::string>& packAttrs);

private:
    void RewriteLoadConfig();

private:
    OnlineConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ONLINE_CONFIG_H
