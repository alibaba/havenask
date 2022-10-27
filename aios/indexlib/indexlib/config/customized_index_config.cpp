#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/impl/customized_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedIndexConfig);

CustomizedIndexConfig::CustomizedIndexConfig(
        const string& indexName, IndexType indexType)
    : PackageIndexConfig(indexName, indexType)
    , mImpl(new CustomizedIndexConfigImpl(indexName, indexType))
{
    PackageIndexConfig::ResetImpl(mImpl);
}

CustomizedIndexConfig::CustomizedIndexConfig(
        const CustomizedIndexConfig& other)
    : PackageIndexConfig(other)
    , mImpl(new CustomizedIndexConfigImpl(*(other.mImpl)))
{
    PackageIndexConfig::ResetImpl(mImpl);
}

CustomizedIndexConfig::~CustomizedIndexConfig() 
{
}

void CustomizedIndexConfig::Check() const
{
}

void CustomizedIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void CustomizedIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const CustomizedIndexConfig& other2 = (const CustomizedIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}

IndexConfig* CustomizedIndexConfig::Clone() const
{
    return new CustomizedIndexConfig(*this);
}

void CustomizedIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const CustomizedIndexConfig& other2 = (const CustomizedIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
} 

const std::string& CustomizedIndexConfig::GetIndexerName() const
{
    return mImpl->GetIndexerName();
}
const util::KeyValueMap& CustomizedIndexConfig::GetParameters() const
{
    return mImpl->GetParameters();
}
void CustomizedIndexConfig::SetIndexer(const std::string& indexerName)
{
    mImpl->SetIndexer(indexerName);
}
void CustomizedIndexConfig::SetParameters(const util::KeyValueMap& params)
{
    mImpl->SetParameters(params);
}
bool CustomizedIndexConfig::CheckFieldType(FieldType ft) const
{
    return mImpl->CheckFieldType(ft);
}

IE_NAMESPACE_END(config);

