/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "indexlib/config/SortParam.h"
#include "indexlib/index/kkv/config/KKVIndexFieldInfo.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlib::config {
class KKVIndexConfigImpl;
}

namespace indexlibv2::config {
class KKVIndexConfig : public KVIndexConfig
{
public:
    KKVIndexConfig();
    ~KKVIndexConfig();

public:
    void Check() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckCompatible(const IIndexConfig* other) const override;

public:
    const std::string& GetIndexType() const override;
    void SetPrefixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);
    void SetSuffixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);
    const std::string& GetPrefixFieldName();
    const std::string& GetSuffixFieldName();
    void SetPrefixFieldInfo(const indexlib::config::KKVIndexFieldInfo& fieldInfo);
    void SetSuffixFieldInfo(const indexlib::config::KKVIndexFieldInfo& fieldInfo);

    void SetIndexPreference(const indexlib::config::KKVIndexPreference& indexPreference);

    const indexlib::config::KKVIndexFieldInfo& GetPrefixFieldInfo() const;
    const indexlib::config::KKVIndexFieldInfo& GetSuffixFieldInfo() const;

    const std::shared_ptr<FieldConfig>& GetPrefixFieldConfig() const;
    const std::shared_ptr<FieldConfig>& GetSuffixFieldConfig() const;

    const indexlib::config::KKVIndexPreference& GetIndexPreference() const;
    indexlib::config::KKVIndexPreference& GetIndexPreference();

    bool NeedSuffixKeyTruncate() const;
    uint32_t GetSuffixKeyTruncateLimits() const;
    uint32_t GetSuffixKeySkipListThreshold() const;
    uint32_t GetSuffixKeyProtectionThreshold() const;
    const indexlib::config::SortParams& GetSuffixKeyTruncateParams() const;

    bool EnableSuffixKeyKeepSortSequence() const;
    void SetSuffixKeyTruncateLimits(uint32_t countLimits);
    void SetSuffixKeyProtectionThreshold(uint32_t countLimits);
    void SetSuffixKeySkipListThreshold(uint32_t countLimits);

    void SetOptimizedStoreSKey(bool optStoreSKey);
    bool OptimizedStoreSKey() const;

    indexlib::HashFunctionType GetPrefixHashFunctionType() const;
    indexlib::HashFunctionType GetSuffixHashFunctionType() const;

    bool DenyEmptySuffixKey() const;
    void SetDenyEmptySuffixKey(bool flag);

    inline FieldType GetSKeyDictKeyType() const
    {
        FieldType orgType = GetSuffixFieldConfig()->GetFieldType();
        if (orgType == ft_string) {
            return ft_uint64;
        }
        // TODO(chekong.ygm): 限制SKEY类型的范围，除了integer和string以外的类型，不应该支持
        return orgType;
    }

private:
    void InitDefaultSortParams();
    void InitDefaultSkipListThreshold();
    void DisableUnsupportParam();
    void CheckSortParams() const;
    void InitStorageKKVOptimize();
    void InitKeyFieldConfig(const config::IndexConfigDeserializeResource& resource);
    void OptimizeKKVSKeyStore();
    void InitValueAttributeConfig(const config::IndexConfigDeserializeResource& resource);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

public:
    static constexpr const char DEFAULT_SKEY_TS_TRUNC_FIELD_NAME[] = "$TIME_STAMP";
    static constexpr const char INDEX_FIELDS[] = "index_fields";

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
