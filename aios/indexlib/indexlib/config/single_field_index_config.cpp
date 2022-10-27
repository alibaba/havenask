#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SingleFieldIndexConfig);

SingleFieldIndexConfig::SingleFieldIndexConfig(const string& indexName,
                                               IndexType indexType)
    : IndexConfig(indexName, indexType)
    , mImpl(new SingleFieldIndexConfigImpl(indexName, indexType))
{
    IndexConfig::ResetImpl(mImpl);
}

SingleFieldIndexConfig::SingleFieldIndexConfig(const SingleFieldIndexConfig& other)
    : IndexConfig(other)
    , mImpl(new SingleFieldIndexConfigImpl(*(other.mImpl)))
{
    IndexConfig::ResetImpl(mImpl);
}

void SingleFieldIndexConfig::ResetImpl(IndexConfigImpl* impl)
{
    IndexConfig::ResetImpl(impl);
    mImpl = (SingleFieldIndexConfigImpl*)impl;
}

SingleFieldIndexConfig::~SingleFieldIndexConfig()
{}

uint32_t SingleFieldIndexConfig::GetFieldCount() const
{
    return mImpl->GetFieldCount();
}
void SingleFieldIndexConfig::Check() const
{
    mImpl->Check();
}
bool SingleFieldIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return mImpl->IsInIndex(fieldId);
}
void SingleFieldIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void SingleFieldIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const SingleFieldIndexConfig& other2 = (const SingleFieldIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void SingleFieldIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const SingleFieldIndexConfig& other2 = (const SingleFieldIndexConfig&)other;
    mImpl->AssertCompatible(*(other2.mImpl));
}
IndexConfig* SingleFieldIndexConfig::Clone() const
{
    return new SingleFieldIndexConfig(*this);
}

IndexConfig::Iterator SingleFieldIndexConfig::CreateIterator() const  
{
    return mImpl->CreateIterator();
}
int32_t SingleFieldIndexConfig::GetFieldIdxInPack(fieldid_t id) const 
{ 
    return mImpl->GetFieldIdxInPack(id);
}
bool SingleFieldIndexConfig::CheckFieldType(FieldType ft) const
{
    return mImpl->CheckFieldType(ft);
}
void SingleFieldIndexConfig::SetFieldConfig(const FieldConfigPtr& fieldConfig)
{
    mImpl->SetFieldConfig(fieldConfig);
}
FieldConfigPtr SingleFieldIndexConfig::GetFieldConfig() const
{
    return mImpl->GetFieldConfig();
}

//TODO: outsize used in plugin, expect merge to primarykey config
bool SingleFieldIndexConfig::HasPrimaryKeyAttribute() const
{
    return mImpl->HasPrimaryKeyAttribute();
}
void SingleFieldIndexConfig::SetPrimaryKeyAttributeFlag(bool flag)
{
    mImpl->SetPrimaryKeyAttributeFlag(flag);
}

IE_NAMESPACE_END(config);

