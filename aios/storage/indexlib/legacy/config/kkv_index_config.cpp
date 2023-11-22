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
#include "indexlib/config/kkv_index_config.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, KKVIndexConfig);

namespace {
static const uint32_t DEFAULT_SKIPLIT_THRESHOLD = []() {
    uint32_t ret = autil::EnvUtil::getEnv("indexlib_kkv_default_skiplist_threshold", 100);
    return ret;
}();
}

const string KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME = "$TIME_STAMP";

struct KKVIndexConfig::Impl {
    std::shared_ptr<FieldConfig> prefixFieldConfig;
    std::shared_ptr<FieldConfig> suffixFieldConfig;
    KKVIndexFieldInfo prefixInfo;
    KKVIndexFieldInfo suffixInfo;
    KKVIndexPreference indexPreference;
    bool optStoreSKey = false;
};

KKVIndexConfig::KKVIndexConfig(const string& indexName, InvertedIndexType indexType)
    : KVIndexConfig(indexName, indexType)
    , mImpl(std::make_unique<Impl>())
{
    // attention: set property format version id according to function log
    [[maybe_unused]] auto status = SetMaxSupportedIndexFormatVersionId(0);
}

KKVIndexConfig::KKVIndexConfig(const KKVIndexConfig& other)
    : KVIndexConfig(other)
    , mImpl(std::make_unique<Impl>(*other.mImpl))
{
}

KKVIndexConfig::~KKVIndexConfig() {}

uint32_t KKVIndexConfig::GetFieldCount() const { return 2; }

IndexConfig::Iterator KKVIndexConfig::CreateIterator() const
{
    FieldConfigVector fieldConfigVec;
    fieldConfigVec.push_back(mImpl->prefixFieldConfig);
    fieldConfigVec.push_back(mImpl->suffixFieldConfig);
    return IndexConfig::Iterator(fieldConfigVec);
}

bool KKVIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return (mImpl->prefixFieldConfig && mImpl->prefixFieldConfig->GetFieldId() == fieldId) ||
           (mImpl->suffixFieldConfig && mImpl->suffixFieldConfig->GetFieldId() == fieldId);
}

void KKVIndexConfig::AssertEqual(const IndexConfig& other) const
{
    // TODO: use code below when kkv support no ttl
    // KVIndexConfig::AssertEqual(other);
    IndexConfig::AssertEqual(other);

    const auto& typedOther = (const KKVIndexConfig&)other;
    mImpl->prefixFieldConfig->AssertEqual(*(typedOther.mImpl->prefixFieldConfig));
    mImpl->suffixFieldConfig->AssertEqual(*(typedOther.mImpl->suffixFieldConfig));
    auto status = mImpl->prefixInfo.CheckEqual(typedOther.mImpl->prefixInfo);
    THROW_IF_STATUS_ERROR(status);
    status = mImpl->suffixInfo.CheckEqual(typedOther.mImpl->suffixInfo);
    THROW_IF_STATUS_ERROR(status);
}

void KKVIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }

IndexConfig* KKVIndexConfig::Clone() const { return new KKVIndexConfig(*this); }

void KKVIndexConfig::Check() const
{
    IndexConfig::Check();
    mImpl->indexPreference.Check();

    if (mImpl->prefixInfo.keyType != KKVKeyType::PREFIX) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key type wrong");
    }
    if (mImpl->prefixInfo.NeedTruncate()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set count_limits");
    }
    if (mImpl->prefixInfo.protectionThreshold != KKVIndexFieldInfo::DEFAULT_PROTECTION_THRESHOLD) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set %s", KKV_BUILD_PROTECTION_THRESHOLD.c_str());
    }
    if (!mImpl->prefixInfo.sortParams.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set trunc_sort_params");
    }

    if (mImpl->suffixInfo.keyType != KKVKeyType::SUFFIX) {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key type wrong");
    }

    if (mImpl->prefixFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support multi_value field!");
    }

    if (mImpl->suffixFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key not support multi_value field!");
    }

    if (mImpl->suffixInfo.NeedTruncate() && mImpl->suffixInfo.countLimits > mImpl->suffixInfo.protectionThreshold) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "suffix count_limits [%u] "
                             "should be less than %s [%u]",
                             mImpl->suffixInfo.countLimits, KKV_BUILD_PROTECTION_THRESHOLD.c_str(),
                             mImpl->suffixInfo.protectionThreshold);
    }

    CheckSortParams();
}

void KKVIndexConfig::CheckSortParams() const
{
    vector<AttributeConfigPtr> subAttrConfigs;
    const ValueConfigPtr& valueConfig = GetValueConfig();
    size_t attributeCount = valueConfig->GetAttributeCount();
    subAttrConfigs.reserve(attributeCount);
    for (size_t i = 0; i < attributeCount; ++i) {
        subAttrConfigs.push_back(valueConfig->GetAttributeConfig(i));
    }

    for (size_t i = 0; i < mImpl->suffixInfo.sortParams.size(); i++) {
        SortParam param = mImpl->suffixInfo.sortParams[i];
        string sortField = param.GetSortField();
        if (sortField == KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME) {
            continue;
        }
        if (sortField == mImpl->suffixFieldConfig->GetFieldName()) {
            if (mImpl->suffixFieldConfig->GetFieldType() == ft_string) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "sort field [%s] in trunc_sort_params "
                                     "must be non string type!",
                                     sortField.c_str());
            }
            continue;
        }
        bool match = false;
        for (size_t j = 0; j < subAttrConfigs.size(); ++j) {
            if (sortField == subAttrConfigs[j]->GetAttrName()) {
                if (subAttrConfigs[j]->GetFieldType() == ft_string || subAttrConfigs[j]->IsMultiValue()) {
                    INDEXLIB_FATAL_ERROR(Schema,
                                         "sort field [%s] in trunc_sort_params "
                                         "must be non string type & single value field!",
                                         sortField.c_str());
                }
                match = true;
            }
        }
        if (!match) {
            INDEXLIB_FATAL_ERROR(Schema, "sort field [%s] in trunc_sort_params not exist!", sortField.c_str());
        }
    }
}

void KKVIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    IndexConfig::Jsonize(json);
    if (json.GetMode() == TO_JSON) {
        vector<KKVIndexFieldInfo> fieldInfo;
        fieldInfo.push_back(mImpl->prefixInfo);
        fieldInfo.push_back(mImpl->suffixInfo);
        json.Jsonize(INDEX_FIELDS, fieldInfo);
        json.Jsonize(INDEX_PREFERENCE_NAME, mImpl->indexPreference);
        json.Jsonize(USE_NUMBER_HASH, UseNumberHash());
    } else {
        vector<KKVIndexFieldInfo> fieldInfo;
        json.Jsonize(INDEX_FIELDS, fieldInfo);
        if (fieldInfo.size() != 2) {
            INDEXLIB_FATAL_ERROR(Schema, "kkv index config must has two fields");
        }
        if (fieldInfo[0].keyType == KKVKeyType::PREFIX) {
            mImpl->prefixInfo = fieldInfo[0];
            mImpl->suffixInfo = fieldInfo[1];
        } else {
            mImpl->prefixInfo = fieldInfo[1];
            mImpl->suffixInfo = fieldInfo[0];
        }

        json.Jsonize(INDEX_PREFERENCE_NAME, mImpl->indexPreference, mImpl->indexPreference);
        InitDefaultSortParams();
        InitDefaultSkipListThreshold();
        InitStorageKKVOptimize();
        DisableUnsupportParam();
        bool useNumberHash = UseNumberHash();
        json.Jsonize(USE_NUMBER_HASH, useNumberHash, useNumberHash);
        SetUseNumberHash(useNumberHash);
    }
}

std::unique_ptr<indexlibv2::config::KKVIndexConfig> KKVIndexConfig::MakeKKVIndexConfigV2() const
{
    auto kkvConfigV2 = std::make_unique<indexlibv2::config::KKVIndexConfig>();
    FillKKVIndexConfigV2(kkvConfigV2.get());
    return kkvConfigV2;
}

void KKVIndexConfig::FillKKVIndexConfigV2(indexlibv2::config::KKVIndexConfig* configV2) const
{
    assert(configV2);
    KVIndexConfig::FillKVIndexConfigV2(configV2);
    configV2->SetPrefixFieldConfig(GetPrefixFieldConfig());
    configV2->SetSuffixFieldConfig(GetSuffixFieldConfig());
    configV2->SetPrefixFieldInfo(GetPrefixFieldInfo());
    configV2->SetSuffixFieldInfo(GetSuffixFieldInfo());
    configV2->SetIndexPreference(GetIndexPreference());
    configV2->SetSuffixKeyTruncateLimits(GetSuffixKeyTruncateLimits());
    configV2->SetSuffixKeyProtectionThreshold(GetSuffixKeyProtectionThreshold());
    configV2->SetSuffixKeySkipListThreshold(GetSuffixKeySkipListThreshold());
    configV2->SetOptimizedStoreSKey(OptimizedStoreSKey());
}

void KKVIndexConfig::InitStorageKKVOptimize()
{
    if (!autil::EnvUtil::getEnv("INDEXLIB_OPT_KV_STORE", false)) {
        return;
    }

    AUTIL_LOG(INFO, "enable kkv storage optimize.");
    auto skeyParam = mImpl->indexPreference.GetSkeyParam();
    if (!skeyParam.GetFileCompressType().empty()) {
        skeyParam.SetCompressParameter("encode_address_mapper", "true");
    }
    auto valueParam = mImpl->indexPreference.GetValueParam();
    if (!valueParam.GetFileCompressType().empty()) {
        valueParam.SetCompressParameter("encode_address_mapper", "true");
    }
    valueParam.EnableValueImpact(true);
    mImpl->indexPreference.SetSkeyParam(skeyParam);
    mImpl->indexPreference.SetValueParam(valueParam);
}

void KKVIndexConfig::InitDefaultSortParams()
{
    if (mImpl->suffixInfo.sortParams.empty() &&
        (mImpl->suffixInfo.NeedTruncate() || mImpl->suffixInfo.enableKeepSortSequence)) {
        SortParam param;
        param.SetSortField(KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME);
        AUTIL_LOG(INFO, "use default sort param : [%s]", param.toSortDescription().c_str());
        mImpl->suffixInfo.sortParams.push_back(param);
    }
}

void KKVIndexConfig::InitDefaultSkipListThreshold()
{
    if (mImpl->suffixInfo.skipListThreshold == KKVIndexFieldInfo::INVALID_SKIPLIST_THRESHOLD) {
        mImpl->suffixInfo.skipListThreshold = DEFAULT_SKIPLIT_THRESHOLD;
    }
}

void KKVIndexConfig::DisableUnsupportParam()
{
    AUTIL_LOG(INFO, "Disable enable_compact_hash_key & enable_shorten_offset for kkv table");

    KKVIndexPreference::HashDictParam dictParam = mImpl->indexPreference.GetHashDictParam();
    dictParam.SetEnableCompactHashKey(false);
    dictParam.SetEnableShortenOffset(false);
    mImpl->indexPreference.SetHashDictParam(dictParam);
}

void KKVIndexConfig::SetPrefixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    SetFieldConfig(fieldConfig);
    mImpl->prefixFieldConfig = fieldConfig;
}
void KKVIndexConfig::SetSuffixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    mImpl->suffixFieldConfig = fieldConfig;
}
const std::string& KKVIndexConfig::GetPrefixFieldName() { return mImpl->prefixInfo.fieldName; }
const std::string& KKVIndexConfig::GetSuffixFieldName() { return mImpl->suffixInfo.fieldName; }
void KKVIndexConfig::SetPrefixFieldInfo(const KKVIndexFieldInfo& fieldInfo) { mImpl->prefixInfo = fieldInfo; }
void KKVIndexConfig::SetSuffixFieldInfo(const KKVIndexFieldInfo& fieldInfo) { mImpl->suffixInfo = fieldInfo; }

const KKVIndexFieldInfo& KKVIndexConfig::GetPrefixFieldInfo() const { return mImpl->prefixInfo; }
const KKVIndexFieldInfo& KKVIndexConfig::GetSuffixFieldInfo() const { return mImpl->suffixInfo; }

const std::shared_ptr<FieldConfig>& KKVIndexConfig::GetPrefixFieldConfig() const { return mImpl->prefixFieldConfig; }
const std::shared_ptr<FieldConfig>& KKVIndexConfig::GetSuffixFieldConfig() const { return mImpl->suffixFieldConfig; }

bool KKVIndexConfig::NeedSuffixKeyTruncate() const { return mImpl->suffixInfo.NeedTruncate(); }
uint32_t KKVIndexConfig::GetSuffixKeyTruncateLimits() const { return mImpl->suffixInfo.countLimits; }
uint32_t KKVIndexConfig::GetSuffixKeySkipListThreshold() const { return mImpl->suffixInfo.skipListThreshold; }
uint32_t KKVIndexConfig::GetSuffixKeyProtectionThreshold() const { return mImpl->suffixInfo.protectionThreshold; }
const SortParams& KKVIndexConfig::GetSuffixKeyTruncateParams() const { return mImpl->suffixInfo.sortParams; }

bool KKVIndexConfig::EnableSuffixKeyKeepSortSequence() const { return mImpl->suffixInfo.enableKeepSortSequence; }
void KKVIndexConfig::SetSuffixKeyTruncateLimits(uint32_t countLimits) { mImpl->suffixInfo.countLimits = countLimits; }
void KKVIndexConfig::SetSuffixKeyProtectionThreshold(uint32_t countLimits)
{
    mImpl->suffixInfo.protectionThreshold = countLimits;
}
void KKVIndexConfig::SetSuffixKeySkipListThreshold(uint32_t countLimits)
{
    mImpl->suffixInfo.skipListThreshold = countLimits;
}

void KKVIndexConfig::SetOptimizedStoreSKey(bool optStoreSKey) { mImpl->optStoreSKey = optStoreSKey; }

bool KKVIndexConfig::OptimizedStoreSKey() const { return mImpl->optStoreSKey; }

HashFunctionType KKVIndexConfig::GetPrefixHashFunctionType() const
{
    return GetHashFunctionType(mImpl->prefixFieldConfig->GetFieldType(), UseNumberHash());
}
HashFunctionType KKVIndexConfig::GetSuffixHashFunctionType() const
{
    return GetHashFunctionType(mImpl->suffixFieldConfig->GetFieldType(), true);
}

void KKVIndexConfig::SetIndexPreference(const KKVIndexPreference& indexPreference)
{
    auto valueConfig = GetValueConfig();
    if (valueConfig) {
        valueConfig->EnableValueImpact(indexPreference.GetValueParam().IsValueImpact());
        valueConfig->EnablePlainFormat(indexPreference.GetValueParam().IsPlainFormat());
    }
    mImpl->indexPreference = indexPreference;
}

const KKVIndexPreference& KKVIndexConfig::GetIndexPreference() const { return mImpl->indexPreference; }
KKVIndexPreference& KKVIndexConfig::GetIndexPreference() { return mImpl->indexPreference; }

}} // namespace indexlib::config
