#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/config/impl/range_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, RangeIndexConfigImpl);

RangeIndexConfigImpl::RangeIndexConfigImpl(const string& indexName,
                                           IndexType indexType)
    : SingleFieldIndexConfigImpl(indexName, indexType)
{
    
}

RangeIndexConfigImpl::~RangeIndexConfigImpl() 
{
}

void RangeIndexConfigImpl::Check() const
{
    SingleFieldIndexConfigImpl::Check();
    if (mFieldConfig->IsMultiValue())
    {
        stringstream ss;
        ss << "range index: [" << mIndexName
           << "] not support multi value FieldType";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (IsHashTypedDictionary())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "range index should not set use_hash_typed_dictionary");
    }
}

IndexConfigImpl* RangeIndexConfigImpl::Clone() const
{
    return new RangeIndexConfigImpl(*this);
}

IndexConfigPtr RangeIndexConfigImpl::GetBottomLevelIndexConfig()
{
    if (!mBottomLevelConfig)
    {
        RangeIndexConfigImpl* bottomIndexCfg = (RangeIndexConfigImpl*)this->Clone();
        mBottomLevelConfig.reset(new RangeIndexConfig(bottomIndexCfg));
        mBottomLevelConfig->SetIndexName(
                RangeIndexConfig::GetBottomLevelIndexName(mIndexName));
    }
    return mBottomLevelConfig;
}

IndexConfigPtr RangeIndexConfigImpl::GetHighLevelIndexConfig()
{
    if (!mHighLevelConfig)
    {
        RangeIndexConfigImpl* highIndexCfg = (RangeIndexConfigImpl*)this->Clone();
        mHighLevelConfig.reset(new RangeIndexConfig(highIndexCfg));
        mHighLevelConfig->SetIndexName(
                RangeIndexConfig::GetHighLevelIndexName(mIndexName));
    }
    return mHighLevelConfig;
}

bool RangeIndexConfigImpl::CheckFieldType(FieldType ft) const
{
    if (!SingleFieldIndexConfigImpl::CheckFieldType(ft))
    {
        return false;
                
    }
    if (ft == ft_uint64)
    {
        IE_LOG(ERROR, "range index not support uint64 type");
        return false;
                
    }
    return true;       
}

IE_NAMESPACE_END(config);

