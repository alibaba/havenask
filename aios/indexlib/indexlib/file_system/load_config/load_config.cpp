#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/load_config/load_config_impl.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(config, LoadConfig);
LoadConfig::LoadConfig()
    : mImpl(new LoadConfigImpl())
{
}

LoadConfig::~LoadConfig() 
{
}

LoadConfig::LoadConfig(const LoadConfig& other)
    : LoadConfigBase(other)
    , mImpl(new LoadConfigImpl(*other.mImpl))
{
}

LoadConfig& LoadConfig::operator=(const LoadConfig& other)
{
    if (this != &other)
    {
        *static_cast<LoadConfigBase*>(this) = static_cast<const LoadConfigBase&>(other);
        mImpl.reset(new LoadConfigImpl(*other.mImpl));
    }
    return *this;
}

bool LoadConfig::EqualWith(const LoadConfig& other) const
{
    assert(mImpl.get());
    assert(other.mImpl.get());
    return LoadConfigBase::EqualWith(static_cast<const LoadConfigBase&>( other))
        && mImpl->EqualWith(*other.mImpl);
}

void LoadConfig::Check() const
{
    assert(mImpl.get());
    LoadConfigBase::Check();
    mImpl->Check();
}

void LoadConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    LoadConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

bool LoadConfig::Match(const string& filePath, const string& lifecycle) const
{
    if (!LoadConfigBase::Match(filePath))
    {
        return false;
    }
    return mImpl->GetLifecycle().empty() || lifecycle == mImpl->GetLifecycle();
}

IE_NAMESPACE_END(file_system);

