#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/impl/customized_table_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedTableConfig);

CustomizedTableConfig::CustomizedTableConfig()
    : mImpl(new CustomizedTableConfigImpl)
{
}

CustomizedTableConfig::CustomizedTableConfig(const CustomizedTableConfig& other)
    : mImpl(new CustomizedTableConfigImpl(*(other.mImpl.get())))
{
}

CustomizedTableConfig::~CustomizedTableConfig() 
{
}

void CustomizedTableConfig::Jsonize(legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void CustomizedTableConfig::AssertEqual(const CustomizedTableConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const string& CustomizedTableConfig::GetIdentifier() const
{
    return mImpl->GetIdentifier();
}

const string& CustomizedTableConfig::GetPluginName() const
{
    return mImpl->GetPluginName();
}

const bool CustomizedTableConfig::GetParameters(const string& key,
                                                string& value) const
{
    return mImpl->GetParameters(key, value);
}

const map<string, string>& CustomizedTableConfig::GetParameters() const
{
    return mImpl->GetParameters();
}

IE_NAMESPACE_END(config);

