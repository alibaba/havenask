#include "indexlib/config/truncate_index_config.h"
#include "indexlib/config/impl/truncate_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateIndexConfig);

TruncateIndexConfig::TruncateIndexConfig()
    : mImpl(new TruncateIndexConfigImpl)
{
}

TruncateIndexConfig::~TruncateIndexConfig() 
{
}

const string& TruncateIndexConfig::GetIndexName() const
{
    return mImpl->GetIndexName();
}
void TruncateIndexConfig::SetIndexName(const string& indexName)  
{ 
    mImpl->SetIndexName(indexName); 
}

void TruncateIndexConfig::AddTruncateIndexProperty(const TruncateIndexProperty& property)
{
    mImpl->AddTruncateIndexProperty(property);
}

const TruncateIndexPropertyVector&
TruncateIndexConfig::GetTruncateIndexProperties() const 
{
    return mImpl->GetTruncateIndexProperties();
}

const TruncateIndexProperty&
TruncateIndexConfig::GetTruncateIndexProperty(size_t i) const
{
    return mImpl->GetTruncateIndexProperty(i);
}

TruncateIndexProperty& TruncateIndexConfig::GetTruncateIndexProperty(size_t i)
{
    return mImpl->GetTruncateIndexProperty(i);
}

size_t TruncateIndexConfig::GetTruncateIndexPropertySize() const
{
    return mImpl->GetTruncateIndexPropertySize();
}

IE_NAMESPACE_END(config);

