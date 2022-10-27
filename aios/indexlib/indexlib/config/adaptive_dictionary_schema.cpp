#include "indexlib/config/adaptive_dictionary_schema.h"
#include "indexlib/config/impl/adaptive_dictionary_schema_impl.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AdaptiveDictionarySchema);

AdaptiveDictionarySchema::AdaptiveDictionarySchema()
{
    mImpl.reset(new AdaptiveDictionarySchemaImpl());
}
AdaptiveDictionaryConfigPtr AdaptiveDictionarySchema::GetAdaptiveDictionaryConfig(
    const string& ruleName) const
{
    return mImpl->GetAdaptiveDictionaryConfig(ruleName);
}

void AdaptiveDictionarySchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void AdaptiveDictionarySchema::AssertEqual(const AdaptiveDictionarySchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void AdaptiveDictionarySchema::AssertCompatible(const AdaptiveDictionarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

void AdaptiveDictionarySchema::AddAdaptiveDictionaryConfig(const AdaptiveDictionaryConfigPtr& config)
{
    mImpl->AddAdaptiveDictionaryConfig(config);
}

IE_NAMESPACE_END(config);

