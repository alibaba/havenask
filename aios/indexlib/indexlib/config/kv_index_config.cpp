#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KVIndexConfig);
KVIndexConfig::KVIndexConfig(const string& indexName, IndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(new KVIndexConfigImpl(indexName, indexType))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

KVIndexConfig::KVIndexConfig(const KVIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(new KVIndexConfigImpl(*(other.mImpl)))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}

KVIndexConfig::~KVIndexConfig() { };

void KVIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const KVIndexConfig& other2 = (const KVIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void KVIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const KVIndexConfig& other2 = (const KVIndexConfig&)other;
    mImpl->AssertCompatible(*(other2.mImpl));
}
IndexConfig* KVIndexConfig::Clone() const
{
    return new KVIndexConfig(*this);
}
void KVIndexConfig::Check() const
{
    mImpl->Check();
}
void KVIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

const ValueConfigPtr& KVIndexConfig::GetValueConfig() const
{
    return mImpl->GetValueConfig();
}
void KVIndexConfig::SetValueConfig(const ValueConfigPtr& valueConfig)
{
    mImpl->SetValueConfig(valueConfig);
}

bool KVIndexConfig::TTLEnabled() const
{
    return mImpl->TTLEnabled();
}
void KVIndexConfig::DisableTTL()
{
    mImpl->DisableTTL();
}
int64_t KVIndexConfig::GetTTL() const
{
    return mImpl->GetTTL();
}
void KVIndexConfig::SetTTL(int64_t ttl)
{
    mImpl->SetTTL(ttl);
}
    
size_t KVIndexConfig::GetRegionCount() const
{
    return mImpl->GetRegionCount();
}
regionid_t KVIndexConfig::GetRegionId() const
{
    return mImpl->GetRegionId();
}

const std::string& KVIndexConfig::GetKeyFieldName() const
{
    return mImpl->GetKeyFieldName();
}
const KVIndexPreference& KVIndexConfig::GetIndexPreference() const
{
    return mImpl->GetIndexPreference();
}
KVIndexPreference& KVIndexConfig::GetIndexPreference()
{
    return mImpl->GetIndexPreference();
}

void KVIndexConfig::SetIndexPreference(const KVIndexPreference& indexPreference)
{
    mImpl->SetIndexPreference(indexPreference);
}

HashFunctionType KVIndexConfig::GetKeyHashFunctionType() const
{
    return GetHashFunctionType(mImpl->GetFieldConfig()->GetFieldType(),
                               mImpl->UseNumberHash());
}
void KVIndexConfig::SetUseSwapMmapFile(bool useSwapMmapFile)
{
    mImpl->SetUseSwapMmapFile(useSwapMmapFile);
}
bool KVIndexConfig::GetUseSwapMmapFile()
{
    return mImpl->GetUseSwapMmapFile();
}

void KVIndexConfig::SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize)
{
    mImpl->SetMaxSwapMmapFileSize(maxSwapMmapFileSize);
}
int64_t KVIndexConfig::GetMaxSwapMmapFileSize() const
{
    return mImpl->GetMaxSwapMmapFileSize();
}

void KVIndexConfig::SetRegionInfo(regionid_t regionId, size_t regionCount)
{
    mImpl->SetRegionInfo(regionId, regionCount);
}
    
bool KVIndexConfig::UseNumberHash() const
{
    return mImpl->UseNumberHash();
}

bool KVIndexConfig::IsCompactHashKey() const
{
    KVIndexPreference preference = GetIndexPreference();
    if (!preference.GetHashDictParam().HasEnableCompactHashKey()) 
    {
        return false;
    }
    HashFunctionType hashType = GetKeyHashFunctionType();
    if (hashType == hft_murmur)
    {
        return false;
    }
    assert(hashType == hft_int64 || hashType == hft_uint64);
    FieldType ft = GetFieldConfig()->GetFieldType();
    switch (ft)
    {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
        return true;
    default:
        break;
    }
    return false;
}

HashFunctionType KVIndexConfig::GetHashFunctionType(
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

void KVIndexConfig::ResetImpl(IndexConfigImpl* impl)
{
    SingleFieldIndexConfig::ResetImpl(impl);
    mImpl = (KVIndexConfigImpl*)impl;
}
IE_NAMESPACE_END(config);

