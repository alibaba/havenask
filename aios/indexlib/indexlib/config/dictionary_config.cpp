#include "indexlib/config/dictionary_config.h"
#include "indexlib/config/impl/dictionary_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DictionaryConfig);

DictionaryConfig::DictionaryConfig(const string& dictName, 
                                   const string& content)
    : mImpl(new DictionaryConfigImpl(dictName, content))
{
}

DictionaryConfig::~DictionaryConfig() 
{
}

void DictionaryConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void DictionaryConfig::AssertEqual(const DictionaryConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

DictionaryConfig::DictionaryConfig(const DictionaryConfig& other)
    : mImpl(new DictionaryConfigImpl(*(other.mImpl.get())))
{
}

const std::string& DictionaryConfig::GetDictName() const
{
    return mImpl->GetDictName();
}

const std::string& DictionaryConfig::GetContent() const
{
    return mImpl->GetContent();
}

IE_NAMESPACE_END(config);

