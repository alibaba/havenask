#include <autil/StringUtil.h>
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigList);

LoadConfigList::LoadConfigList()
    : mTotalCacheSize(0)
{
    LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");
    mDefaultLoadConfig.SetFilePatternString(pattern);
    mLoadSpeedLimitSwitch.reset(new LoadSpeedLimitSwitch);
}

LoadConfigList::~LoadConfigList() 
{
}

// TODO: use load_config instead of LoadConfig
void LoadConfigList::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("load_config", mLoadConfigs);
    if (json.GetMode() == FROM_JSON)
    {
        mTotalCacheSize = 0;
        for (size_t i = 0; i < mLoadConfigs.size(); ++i)
        {
            LoadConfig& loadConfig = mLoadConfigs[i];
            loadConfig.GetLoadStrategy()->SetLoadSpeedLimitSwitch(mLoadSpeedLimitSwitch);
            if (loadConfig.GetName().empty())
            {
                loadConfig.SetName(string("load_config") + StringUtil::toString(i));
            }
            mTotalCacheSize += GetCacheSize(loadConfig);
        }   
    }
}

void LoadConfigList::Check() const
{
    set<string> nameSet;
    for (size_t i = 0; i < mLoadConfigs.size(); ++i)
    {
        mLoadConfigs[i].Check();
        const string& name = mLoadConfigs[i].GetName();
        if (nameSet.count(name) > 0)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                           "duplicate load config name [%s]", name.c_str());
        }
        nameSet.insert(name);
    }
}

void LoadConfigList::PushFront(const LoadConfig& loadConfig)
{
    loadConfig.GetLoadStrategy()->SetLoadSpeedLimitSwitch(mLoadSpeedLimitSwitch);
    mLoadConfigs.insert(mLoadConfigs.begin(), loadConfig);
    mTotalCacheSize += GetCacheSize(loadConfig);
}

void LoadConfigList::PushBack(const LoadConfig& loadConfig) 
{
    loadConfig.GetLoadStrategy()->SetLoadSpeedLimitSwitch(mLoadSpeedLimitSwitch);
    mLoadConfigs.push_back(loadConfig); 
    mTotalCacheSize += GetCacheSize(loadConfig);
}

size_t LoadConfigList::GetCacheSize(const LoadConfig& loadConfig) const
{
    const LoadStrategyPtr& strategy = loadConfig.GetLoadStrategy();
    if (strategy->GetLoadStrategyName() != READ_MODE_CACHE)
    {
        return 0;
    }
    CacheLoadStrategyPtr cacheStrategy = DYNAMIC_POINTER_CAST(CacheLoadStrategy, strategy);
    assert(cacheStrategy);
    return cacheStrategy->GetCacheSize();
}

void LoadConfigList::SetLoadMode(LoadConfig::LoadMode mode)
{
    for (size_t i = 0; i < mLoadConfigs.size(); ++i)
    {
        mLoadConfigs[i].SetLoadMode(mode);
    }
    mDefaultLoadConfig.SetLoadMode(mode);
}

IE_NAMESPACE_END(file_system);
