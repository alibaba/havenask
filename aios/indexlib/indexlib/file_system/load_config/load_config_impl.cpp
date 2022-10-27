#include "indexlib/file_system/load_config/load_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigImpl);

LoadConfigImpl::LoadConfigImpl() 
{
}

LoadConfigImpl::~LoadConfigImpl() 
{
}

void LoadConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("lifecycle", mLifecycle, mLifecycle);
}

void LoadConfigImpl::Check() const
{
}

bool LoadConfigImpl::EqualWith(const LoadConfigImpl& other) const
{
    return mLifecycle == other.mLifecycle;
}

IE_NAMESPACE_END(file_system);

