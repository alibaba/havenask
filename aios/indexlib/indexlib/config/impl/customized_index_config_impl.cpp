#include "indexlib/config/impl/customized_index_config_impl.h"
#include "indexlib/config/configurator_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CustomizedIndexConfigImpl);

CustomizedIndexConfigImpl::CustomizedIndexConfigImpl(
        const string& indexName, IndexType indexType)
    : PackageIndexConfigImpl(indexName, indexType)
    , mIndexerName("")
{
}

CustomizedIndexConfigImpl::CustomizedIndexConfigImpl(
        const CustomizedIndexConfigImpl& other)
    : PackageIndexConfigImpl(other)
    , mIndexerName(other.mIndexerName)
    , mParams(other.mParams)
{
    
}

CustomizedIndexConfigImpl::~CustomizedIndexConfigImpl() 
{
}

void CustomizedIndexConfigImpl::Check() const
{
}

void CustomizedIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    PackageIndexConfigImpl::Jsonize(json);
    json.Jsonize(CUSTOMIZED_INDEXER_NAME, mIndexerName, mIndexerName);
    json.Jsonize(CUSTOMIZED_INDEXER_PARAMS, mParams, mParams);
}

void CustomizedIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    PackageIndexConfigImpl::AssertEqual(other);
    const CustomizedIndexConfigImpl& typedOther = (const CustomizedIndexConfigImpl&) other;
    IE_CONFIG_ASSERT_EQUAL(mIndexerName, typedOther.mIndexerName, 
                           "customized indexer name not equal");
    IE_CONFIG_ASSERT_EQUAL(mParams, typedOther.mParams, 
                           "indexer parameters not equal"); 
}

IndexConfigImpl* CustomizedIndexConfigImpl::Clone() const
{
    return new CustomizedIndexConfigImpl(*this);
}

IE_NAMESPACE_END(config);

