#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/impl/dictionary_schema_impl.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DictionarySchema);

DictionarySchema::DictionarySchema()
    : mImpl(new DictionarySchemaImpl)
{
}

DictionarySchema::~DictionarySchema() 
{
}

void DictionarySchema::AddDictionaryConfig(const DictionaryConfigPtr& dictConfig)
{
    mImpl->AddDictionaryConfig(dictConfig);
}

DictionaryConfigPtr DictionarySchema::GetDictionaryConfig(
    const string& dictName) const
{
    return mImpl->GetDictionaryConfig(dictName);
}

void DictionarySchema::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void DictionarySchema::AssertEqual(const DictionarySchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

void DictionarySchema::AssertCompatible(const DictionarySchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

DictionarySchema::Iterator DictionarySchema::Begin() const
{
    return mImpl->Begin();
}
DictionarySchema::Iterator DictionarySchema::End() const
{
    return mImpl->End();
} 


IE_NAMESPACE_END(config);

