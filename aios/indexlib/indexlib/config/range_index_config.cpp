#include "indexlib/config/range_index_config.h"
#include "indexlib/config/impl/range_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, RangeIndexConfig);

RangeIndexConfig::RangeIndexConfig(const string& indexName,
                                   IndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(new RangeIndexConfigImpl(indexName, indexType))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

RangeIndexConfig::RangeIndexConfig(const RangeIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(new RangeIndexConfigImpl(*(other.mImpl)))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

RangeIndexConfig::RangeIndexConfig(RangeIndexConfigImpl* impl)
    : SingleFieldIndexConfig(impl->GetIndexName(), impl->GetIndexType())
    , mImpl(impl)
{
    SingleFieldIndexConfig::ResetImpl(impl);
}

RangeIndexConfig::~RangeIndexConfig() 
{
}

void RangeIndexConfig::Check() const
{
    mImpl->Check();
}

IndexConfig* RangeIndexConfig::Clone() const
{
    return new RangeIndexConfig(*this);
}

string RangeIndexConfig::GetBottomLevelIndexName(const std::string& indexName)
{
    return indexName + "_@_bottom";
}

string RangeIndexConfig::GetHighLevelIndexName(const std::string& indexName)
{
    return indexName + "_@_high";
}

IndexConfigPtr RangeIndexConfig::GetBottomLevelIndexConfig()
{
    return mImpl->GetBottomLevelIndexConfig();
}

IndexConfigPtr RangeIndexConfig::GetHighLevelIndexConfig()
{
    return mImpl->GetHighLevelIndexConfig();
}

bool RangeIndexConfig::CheckFieldType(FieldType ft) const
{
    return mImpl->CheckFieldType(ft);
}
IE_NAMESPACE_END(config);

