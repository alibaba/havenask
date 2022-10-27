#ifndef __INDEXLIB_KKV_INDEX_CONFIG_H
#define __INDEXLIB_KKV_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/kkv_index_field_info.h"
#include "indexlib/config/kkv_index_preference.h"
#include "indexlib/config/pack_attribute_config.h"

DECLARE_REFERENCE_CLASS(config, KKVIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);
class KKVIndexConfig : public KVIndexConfig
{
public:
    KKVIndexConfig(const std::string& indexName, IndexType indexType);
    KKVIndexConfig(const KKVIndexConfig& other);
    ~KKVIndexConfig();
public:
    uint32_t GetFieldCount() const override;
    IndexConfig::Iterator CreateIterator() const override; 
    bool IsInIndex(fieldid_t fieldId) const override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    void SetPrefixFieldConfig(const FieldConfigPtr& fieldConfig);
    void SetSuffixFieldConfig(const FieldConfigPtr& fieldConfig);
    const std::string& GetPrefixFieldName();
    const std::string& GetSuffixFieldName();
    void SetPrefixFieldInfo(const KKVIndexFieldInfo& fieldInfo);
    void SetSuffixFieldInfo(const KKVIndexFieldInfo& fieldInfo);

    void SetIndexPreference(const KKVIndexPreference& indexPreference);

    const KKVIndexFieldInfo& GetPrefixFieldInfo() const;
    const KKVIndexFieldInfo& GetSuffixFieldInfo() const;

    const FieldConfigPtr& GetPrefixFieldConfig() const;
    const FieldConfigPtr& GetSuffixFieldConfig() const;

    const KKVIndexPreference& GetIndexPreference() const;
    KKVIndexPreference& GetIndexPreference();

    bool NeedSuffixKeyTruncate() const;
    uint32_t GetSuffixKeyTruncateLimits() const;
    uint32_t GetSuffixKeySkipListThreshold() const;
    uint32_t GetSuffixKeyProtectionThreshold() const;
    const SortParams& GetSuffixKeyTruncateParams() const;

    bool EnableSuffixKeyKeepSortSequence() const;
    void SetSuffixKeyTruncateLimits(uint32_t countLimits);
    void SetSuffixKeyProtectionThreshold(uint32_t countLimits);
    void SetSuffixKeySkipListThreshold(uint32_t countLimits);

    void SetOptimizedStoreSKey(bool optStoreSKey);
    bool OptimizedStoreSKey() const;

    HashFunctionType GetPrefixHashFunctionType() const;
    HashFunctionType GetSuffixHashFunctionType() const;

private:
    KKVIndexConfigImpl* mImpl;
public:
    static const uint32_t DEFAULT_SKEY_SKIPLIST_THRESHOLD = 100;
    static const std::string DEFAULT_SKEY_TS_TRUNC_FIELD_NAME;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KKV_INDEX_CONFIG_H
