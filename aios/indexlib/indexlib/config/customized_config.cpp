#include <autil/legacy/any.h>
#include <autil/legacy/json.h>
#include "indexlib/config/customized_config.h"
#include "indexlib/config/impl/customized_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedConfig);

CustomizedConfig::CustomizedConfig()
    : mImpl(new CustomizedConfigImpl())
{
}

CustomizedConfig::CustomizedConfig(const CustomizedConfig& other)
    : mImpl(new CustomizedConfigImpl(*(other.mImpl.get())))
{
}

CustomizedConfig::~CustomizedConfig() 
{
}

void CustomizedConfig::Jsonize(legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void CustomizedConfig::AssertEqual(const CustomizedConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const string& CustomizedConfig::GetPluginName() const
{
    return mImpl->GetPluginName();
}

const bool CustomizedConfig::GetParameters(const string& key,
                                           string& value) const
{
    return mImpl->GetParameters(key, value);
}

const map<string, string>& CustomizedConfig::GetParameters() const
{
    return mImpl->GetParameters();
}

const string& CustomizedConfig::GetId() const
{
    return mImpl->GetId();
}

void CustomizedConfig::SetId(const string& id)
{
    mImpl->SetId(id);
}

void CustomizedConfig::SetPluginName(const string& pluginName)
{
    mImpl->SetPluginName(pluginName);
}

IE_NAMESPACE_END(config);

