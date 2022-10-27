#include "indexlib/config/date_index_config.h"
#include "indexlib/config/impl/date_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DateIndexConfig);

DateIndexConfig::DateIndexConfig(const string& indexName, IndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(new DateIndexConfigImpl(indexName, indexType))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

DateIndexConfig::DateIndexConfig(const DateIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(new DateIndexConfigImpl(*(other.mImpl)))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

DateIndexConfig::~DateIndexConfig() 
{
}

void DateIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void DateIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const DateIndexConfig& other2 = (const DateIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}

void DateIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const DateIndexConfig& other2 = (const DateIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}

void DateIndexConfig::Check() const
{
    mImpl->Check();
}

IndexConfig* DateIndexConfig::Clone() const
{
    return new DateIndexConfig(*this);
}

DateIndexConfig::Granularity DateIndexConfig::GetBuildGranularity() const
{
    return mImpl->GetBuildGranularity();
}

DateLevelFormat DateIndexConfig::GetDateLevelFormat() const
{
    return mImpl->GetDateLevelFormat();
}

void DateIndexConfig::SetBuildGranularity(Granularity granularity)
{
    mImpl->SetBuildGranularity(granularity);
}
void DateIndexConfig::SetDateLevelFormat(DateLevelFormat format)
{
    mImpl->SetDateLevelFormat(format);
}



IE_NAMESPACE_END(config);

