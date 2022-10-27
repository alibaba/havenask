#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PrimaryKeyIndexConfig);

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const std::string& indexName, IndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(new PrimaryKeyIndexConfigImpl(indexName, indexType))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(new PrimaryKeyIndexConfigImpl(*(other.mImpl)))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

PrimaryKeyIndexConfig::~PrimaryKeyIndexConfig() {}

IndexConfig* PrimaryKeyIndexConfig::Clone() const
{
    return new PrimaryKeyIndexConfig(*this);
}

void PrimaryKeyIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void PrimaryKeyIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const PrimaryKeyIndexConfig& other2 = (const PrimaryKeyIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void PrimaryKeyIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const PrimaryKeyIndexConfig& other2 = (const PrimaryKeyIndexConfig&)other;
    mImpl->AssertCompatible(*(other2.mImpl));    
}
void PrimaryKeyIndexConfig::Check() const
{
    mImpl->Check();
}
void PrimaryKeyIndexConfig::SetPrimaryKeyLoadParam(    
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
    bool lookupReverse, const std::string& param)
{
    mImpl->SetPrimaryKeyLoadParam(loadMode, lookupReverse, param);
}
PrimaryKeyLoadStrategyParamPtr PrimaryKeyIndexConfig::GetPKLoadStrategyParam() const
{
    return mImpl->GetPKLoadStrategyParam();
}
void PrimaryKeyIndexConfig::SetPrimaryKeyIndexType(PrimaryKeyIndexType type)
{
    mImpl->SetPrimaryKeyIndexType(type);
}
PrimaryKeyIndexType PrimaryKeyIndexConfig::GetPrimaryKeyIndexType() const
{
    return mImpl->GetPrimaryKeyIndexType();
}
void PrimaryKeyIndexConfig::SetPrimaryKeyHashType(PrimaryKeyHashType type)
{
    mImpl->SetPrimaryKeyHashType(type);
}
PrimaryKeyHashType PrimaryKeyIndexConfig::GetPrimaryKeyHashType() const
{
    return mImpl->GetPrimaryKeyHashType();
}

IE_NAMESPACE_END(config);
