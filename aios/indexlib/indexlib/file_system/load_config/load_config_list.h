#ifndef __INDEXLIB_FS_LOAD_CONFIG_LIST_H
#define __INDEXLIB_FS_LOAD_CONFIG_LIST_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/load_config.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigList : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<LoadConfig> LoadConfigVector;
public:
    LoadConfigList();
    ~LoadConfigList();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    const LoadConfigVector& GetLoadConfigs() const { return mLoadConfigs; }
    LoadConfigVector& GetLoadConfigs() { return mLoadConfigs; }

    const LoadConfig& GetLoadConfig(size_t idx) const
    { 
        assert(idx < mLoadConfigs.size());
        return mLoadConfigs[idx]; 
    }

    const LoadConfig& GetDefaultLoadConfig() const
    { return mDefaultLoadConfig; }

    size_t GetTotalCacheSize() const
    { return mTotalCacheSize; }

    void PushFront(const LoadConfig& loadConfig);
    void PushBack(const LoadConfig& loadConfig);
    
    void Clear() { mLoadConfigs.clear(); }

    size_t Size() const { return mLoadConfigs.size(); }

    void Check() const;

    void EnableLoadSpeedLimit()
    { mLoadSpeedLimitSwitch->Enable(); }
    void DisableLoadSpeedLimit()
    { mLoadSpeedLimitSwitch->Disable(); }

    void SetLoadMode(LoadConfig::LoadMode mode);

private:
    size_t GetCacheSize(const LoadConfig& loadConfig) const;

private:
    LoadConfigVector mLoadConfigs;
    LoadConfig mDefaultLoadConfig;
    size_t mTotalCacheSize;
    LoadSpeedLimitSwitchPtr mLoadSpeedLimitSwitch;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LoadConfigList);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_LOAD_CONFIG_LIST_H
