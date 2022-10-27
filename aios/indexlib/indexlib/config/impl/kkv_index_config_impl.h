#ifndef __INDEXLIB_KKV_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_KKV_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/kkv_index_preference.h"
#include "indexlib/config/pack_attribute_config.h"

IE_NAMESPACE_BEGIN(config);

class KKVIndexConfigImpl : public KVIndexConfigImpl
{
public:
    KKVIndexConfigImpl(const std::string& indexName, IndexType indexType);
    ~KKVIndexConfigImpl();
public:
    uint32_t GetFieldCount() const override { return 2; }
    IndexConfig::Iterator CreateIterator() const override; 
    bool IsInIndex(fieldid_t fieldId) const override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override;
    IndexConfigImpl* Clone() const override;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    void SetPrefixFieldConfig(const FieldConfigPtr& fieldConfig)
    { SetFieldConfig(fieldConfig); mPrefixFieldConfig = fieldConfig; }
    void SetSuffixFieldConfig(const FieldConfigPtr& fieldConfig)
    { mSuffixFieldConfig = fieldConfig; }
    const std::string& GetPrefixFieldName()
    { return mPrefixInfo.fieldName; }
    const std::string& GetSuffixFieldName()
    { return mSuffixInfo.fieldName; }
    void SetPrefixFieldInfo(const KKVIndexFieldInfo& fieldInfo)
    { mPrefixInfo = fieldInfo; }
    void SetSuffixFieldInfo(const KKVIndexFieldInfo& fieldInfo)
    { mSuffixInfo = fieldInfo; }

    void SetIndexPreference(const KKVIndexPreference& indexPreference)
    { mIndexPreference = indexPreference; }

    const KKVIndexFieldInfo& GetPrefixFieldInfo() const { return mPrefixInfo; }
    const KKVIndexFieldInfo& GetSuffixFieldInfo() const { return mSuffixInfo; }

    const FieldConfigPtr& GetPrefixFieldConfig() const { return mPrefixFieldConfig; }
    const FieldConfigPtr& GetSuffixFieldConfig() const { return mSuffixFieldConfig; }

    const KKVIndexPreference& GetIndexPreference() const { return mIndexPreference; }
    KKVIndexPreference& GetIndexPreference() { return mIndexPreference; }

    bool NeedSuffixKeyTruncate() const { return mSuffixInfo.NeedTruncate(); }
    uint32_t GetSuffixKeyTruncateLimits() const { return mSuffixInfo.countLimits; }
    uint32_t GetSuffixKeySkipListThreshold() const { return mSuffixInfo.skipListThreshold; }
    uint32_t GetSuffixKeyProtectionThreshold() const { return mSuffixInfo.protectionThreshold; }
    const SortParams& GetSuffixKeyTruncateParams() const { return mSuffixInfo.sortParams; }

    bool EnableSuffixKeyKeepSortSequence() const { return mSuffixInfo.enableKeepSortSequence; }
    void SetSuffixKeyTruncateLimits(uint32_t countLimits)
    { mSuffixInfo.countLimits = countLimits; }
    void SetSuffixKeyProtectionThreshold(uint32_t countLimits)
    { mSuffixInfo.protectionThreshold = countLimits; }
    void SetSuffixKeySkipListThreshold(uint32_t countLimits)
    { mSuffixInfo.skipListThreshold = countLimits; }


    void SetOptimizedStoreSKey(bool optStoreSKey) { mOptStoreSKey = optStoreSKey; }
    bool OptimizedStoreSKey() const { return mOptStoreSKey; }

    HashFunctionType GetPrefixHashFunctionType() const
    {
        return GetHashFunctionType(GetPrefixFieldConfig()->GetFieldType(), mUseNumberHash);
    }
    HashFunctionType GetSuffixHashFunctionType() const
    {
        return GetHashFunctionType(GetSuffixFieldConfig()->GetFieldType(), true);
    }

private:
    void InitDefaultSortParams();
    void InitDefaultSkipListThreshold();
    void DisableUnsupportParam();
    void CheckSortParams() const;
    
private:
    FieldConfigPtr mPrefixFieldConfig;
    FieldConfigPtr mSuffixFieldConfig;
    KKVIndexFieldInfo mPrefixInfo;
    KKVIndexFieldInfo mSuffixInfo;
    KKVIndexPreference mIndexPreference;
    bool mOptStoreSKey;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KKV_INDEX_CONFIG_IMPL_H
