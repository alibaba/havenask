#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KKVIndexConfig);

const string KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME = "$TIME_STAMP";

KKVIndexConfig::KKVIndexConfig(const string& indexName, IndexType indexType)
    : KVIndexConfig(indexName, indexType)
    , mImpl(new KKVIndexConfigImpl(indexName, indexType))
{
    KVIndexConfig::ResetImpl(mImpl);
}

KKVIndexConfig::KKVIndexConfig(const KKVIndexConfig& other)
    : KVIndexConfig(other)
    , mImpl(new KKVIndexConfigImpl(*(other.mImpl)))
{
    KVIndexConfig::ResetImpl(mImpl);
}

KKVIndexConfig::~KKVIndexConfig() { }

uint32_t KKVIndexConfig::GetFieldCount() const
{
    return mImpl->GetFieldCount();
}
IndexConfig::Iterator KKVIndexConfig::CreateIterator() const
{
    return mImpl->CreateIterator();
}
bool KKVIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return mImpl->IsInIndex(fieldId);
}
void KKVIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const KKVIndexConfig& other2 = (const KKVIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void KKVIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const KKVIndexConfig& other2 = (const KKVIndexConfig&)other;
    mImpl->AssertCompatible(*(other2.mImpl));
}
IndexConfig* KKVIndexConfig::Clone() const
{
    return new KKVIndexConfig(*this);
}
void KKVIndexConfig::Check() const
{
    mImpl->Check();
}
void KKVIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void KKVIndexConfig::SetPrefixFieldConfig(const FieldConfigPtr& fieldConfig)
{
    mImpl->SetPrefixFieldConfig(fieldConfig);
}
void KKVIndexConfig::SetSuffixFieldConfig(const FieldConfigPtr& fieldConfig)
{
    mImpl->SetSuffixFieldConfig(fieldConfig);
}
const std::string& KKVIndexConfig::GetPrefixFieldName()
{
    return mImpl->GetPrefixFieldName();
}
const std::string& KKVIndexConfig::GetSuffixFieldName()
{
    return mImpl->GetSuffixFieldName();
}
void KKVIndexConfig::SetPrefixFieldInfo(const KKVIndexFieldInfo& fieldInfo)
{
    mImpl->SetPrefixFieldInfo(fieldInfo);
}
void KKVIndexConfig::SetSuffixFieldInfo(const KKVIndexFieldInfo& fieldInfo)
{
    mImpl->SetSuffixFieldInfo(fieldInfo);
}

void KKVIndexConfig::SetIndexPreference(const KKVIndexPreference& indexPreference)
{
    mImpl->SetIndexPreference(indexPreference);
}

const KKVIndexFieldInfo& KKVIndexConfig::GetPrefixFieldInfo() const
{
    return mImpl->GetPrefixFieldInfo();
}
const KKVIndexFieldInfo& KKVIndexConfig::GetSuffixFieldInfo() const
{
    return mImpl->GetSuffixFieldInfo();
}

const FieldConfigPtr& KKVIndexConfig::GetPrefixFieldConfig() const
{
    return mImpl->GetPrefixFieldConfig();
}
const FieldConfigPtr& KKVIndexConfig::GetSuffixFieldConfig() const
{
    return mImpl->GetSuffixFieldConfig();
}

const KKVIndexPreference& KKVIndexConfig::GetIndexPreference() const
{
    return mImpl->GetIndexPreference();
}
KKVIndexPreference& KKVIndexConfig::GetIndexPreference()
{
    return mImpl->GetIndexPreference();
}

bool KKVIndexConfig::NeedSuffixKeyTruncate() const
{
    return mImpl->NeedSuffixKeyTruncate();
}
uint32_t KKVIndexConfig::GetSuffixKeyTruncateLimits() const
{
    return mImpl->GetSuffixKeyTruncateLimits();
}
uint32_t KKVIndexConfig::GetSuffixKeySkipListThreshold() const
{
    return mImpl->GetSuffixKeySkipListThreshold();
}
uint32_t KKVIndexConfig::GetSuffixKeyProtectionThreshold() const
{
    return mImpl->GetSuffixKeyProtectionThreshold();
}
const SortParams& KKVIndexConfig::GetSuffixKeyTruncateParams() const
{
    return mImpl->GetSuffixKeyTruncateParams();
}

bool KKVIndexConfig::EnableSuffixKeyKeepSortSequence() const
{
    return mImpl->EnableSuffixKeyKeepSortSequence();
}
void KKVIndexConfig::SetSuffixKeyTruncateLimits(uint32_t countLimits)
{
    mImpl->SetSuffixKeyTruncateLimits(countLimits);
}
void KKVIndexConfig::SetSuffixKeyProtectionThreshold(uint32_t countLimits)
{
    mImpl->SetSuffixKeyProtectionThreshold(countLimits);
}
void KKVIndexConfig::SetSuffixKeySkipListThreshold(uint32_t countLimits)
{
    mImpl->SetSuffixKeySkipListThreshold(countLimits);
}


void KKVIndexConfig::SetOptimizedStoreSKey(bool optStoreSKey)
{
    mImpl->SetOptimizedStoreSKey(optStoreSKey);
}

bool KKVIndexConfig::OptimizedStoreSKey() const
{
    return mImpl->OptimizedStoreSKey();
}

HashFunctionType KKVIndexConfig::GetPrefixHashFunctionType() const
{
    return GetHashFunctionType(mImpl->GetPrefixFieldConfig()->GetFieldType(),
                               mImpl->UseNumberHash());
}
HashFunctionType KKVIndexConfig::GetSuffixHashFunctionType() const
{
    return GetHashFunctionType(mImpl->GetSuffixFieldConfig()->GetFieldType(),
                               true);
}


IE_NAMESPACE_END(config);

