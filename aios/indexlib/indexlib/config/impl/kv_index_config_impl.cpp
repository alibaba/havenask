#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KVIndexConfigImpl);

KVIndexConfigImpl::KVIndexConfigImpl(const string& indexName, IndexType indexType)
    : SingleFieldIndexConfigImpl(indexName, indexType)
    , mTTL(INVALID_TTL)
    , mUseNumberHash(true)
    , mUseSwapMmapFile(false)
    , mMaxSwapMmapFileSize(-1)
    , mRegionId(INVALID_REGIONID)
    , mRegionCount(0)
{
    SetOptionFlag(of_none);
}
    
KVIndexConfigImpl::~KVIndexConfigImpl() { };

void KVIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfigImpl::Jsonize(json);
    json.Jsonize(INDEX_PREFERENCE_NAME, mIndexPreference, mIndexPreference);
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize(USE_NUMBER_HASH, mUseNumberHash);
    }
    else
    {
        json.Jsonize(USE_NUMBER_HASH, mUseNumberHash, mUseNumberHash);
    }
}

void KVIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    IndexConfigImpl::AssertEqual(other);
    const KVIndexConfigImpl& kvIndexConfig = dynamic_cast<const KVIndexConfigImpl&>(other);
    IE_CONFIG_ASSERT_EQUAL(mTTL, kvIndexConfig.mTTL, "ttl not equal");
    // TODO: how about value other config
}

void KVIndexConfigImpl::AssertCompatible(const IndexConfigImpl& other) const
{
    IndexConfigImpl::AssertEqual(other);
}

void KVIndexConfigImpl::Check() const
{
    if (mFieldConfig->IsMultiValue())
    {
        INDEXLIB_FATAL_ERROR(Schema, "key not support multi_value field!");
    }
    
    IndexConfigImpl::Check();
    mIndexPreference.Check();
    if (mIndexPreference.GetHashDictParam().GetHashType() != "dense"
        && mIndexPreference.GetHashDictParam().GetHashType() != "cuckoo")
    {
        INDEXLIB_FATAL_ERROR(Schema, "key only support dense or cuckoo now");
    }
}

IndexConfigImpl* KVIndexConfigImpl::Clone() const
{
    return new KVIndexConfigImpl(*this);
}

const string& KVIndexConfigImpl::GetKeyFieldName() const
{
    assert(mFieldConfig);
    return mFieldConfig->GetFieldName();
}

HashFunctionType KVIndexConfigImpl::GetHashFunctionType(
        FieldType fieldType, bool useNumberHash)
{
    if (!useNumberHash)
    {
        return hft_murmur;
    }
    switch(fieldType)
    {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_int64:
        return hft_int64;
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
    case ft_uint64:
        return hft_uint64;
    default:
        return hft_murmur;
    }
}

IE_NAMESPACE_END(config);

