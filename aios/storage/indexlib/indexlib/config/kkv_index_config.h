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
#ifndef __INDEXLIB_KKV_INDEX_CONFIG_H
#define __INDEXLIB_KKV_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/kkv/config/KKVIndexFieldInfo.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KKVIndexConfigImpl);

namespace indexlibv2::config {
class KKVIndexConfig;
}

namespace indexlib { namespace config {
class KKVIndexConfig : public KVIndexConfig
{
public:
    KKVIndexConfig(const std::string& indexName, InvertedIndexType indexType);
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

    std::unique_ptr<indexlibv2::config::KKVIndexConfig> MakeKKVIndexConfigV2() const;
    void FillKKVIndexConfigV2(indexlibv2::config::KKVIndexConfig* configV2) const;

public:
    void SetPrefixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);
    void SetSuffixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);
    const std::string& GetPrefixFieldName();
    const std::string& GetSuffixFieldName();
    void SetPrefixFieldInfo(const KKVIndexFieldInfo& fieldInfo);
    void SetSuffixFieldInfo(const KKVIndexFieldInfo& fieldInfo);

    void SetIndexPreference(const KKVIndexPreference& indexPreference);

    const KKVIndexFieldInfo& GetPrefixFieldInfo() const;
    const KKVIndexFieldInfo& GetSuffixFieldInfo() const;

    const std::shared_ptr<FieldConfig>& GetPrefixFieldConfig() const;
    const std::shared_ptr<FieldConfig>& GetSuffixFieldConfig() const;

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
    void InitDefaultSortParams();
    void InitDefaultSkipListThreshold();
    void DisableUnsupportParam();
    void CheckSortParams() const;
    void InitStorageKKVOptimize();

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

public:
    static const std::string DEFAULT_SKEY_TS_TRUNC_FIELD_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_KKV_INDEX_CONFIG_H
