#include "indexlib/config/adaptive_dictionary_config.h"
#include "indexlib/config/impl/adaptive_dictionary_config_impl.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AdaptiveDictionaryConfig);


AdaptiveDictionaryConfig::AdaptiveDictionaryConfig()
    : mImpl(new AdaptiveDictionaryConfigImpl())
{
}

AdaptiveDictionaryConfig::AdaptiveDictionaryConfig(
        const std::string& ruleName, 
        const std::string& dictType,
        int32_t threshold)
    : mImpl(new AdaptiveDictionaryConfigImpl(ruleName, dictType, threshold))
{
}

AdaptiveDictionaryConfig::~AdaptiveDictionaryConfig() 
{
}

const std::string& AdaptiveDictionaryConfig::GetRuleName() const
{
    return mImpl->GetRuleName();
}

const AdaptiveDictionaryConfig::DictType& AdaptiveDictionaryConfig::GetDictType() const
{
    return mImpl->GetDictType();
}

int32_t AdaptiveDictionaryConfig::GetThreshold() const
{
    return mImpl->GetThreshold();
}

void AdaptiveDictionaryConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void AdaptiveDictionaryConfig::AssertEqual(
        const AdaptiveDictionaryConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

IE_NAMESPACE_END(config);

