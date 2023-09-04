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
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, KKVIndexConfig);

namespace {
static const uint32_t DEFAULT_SKIPLIT_THRESHOLD = []() {
    uint32_t ret =
        autil::EnvUtil::getEnv("indexlib_kkv_default_skiplist_threshold", index::KKV_DEFAULT_SKIPLIST_THRESHOLD);
    return ret;
}();
}

struct KKVIndexConfig::Impl {
    std::shared_ptr<FieldConfig> prefixFieldConfig;
    std::shared_ptr<FieldConfig> suffixFieldConfig;
    indexlib::config::KKVIndexFieldInfo prefixInfo;
    indexlib::config::KKVIndexFieldInfo suffixInfo;
    indexlib::config::KKVIndexPreference indexPreference;
    bool optStoreSKey = false;
    bool denyEmptySKey = false;
};

KKVIndexConfig::KKVIndexConfig() : _impl(std::make_unique<Impl>()) {}

KKVIndexConfig::~KKVIndexConfig() {}

const std::string& KKVIndexConfig::GetIndexType() const
{
    static const std::string RESULT = indexlibv2::index::KKV_INDEX_TYPE_STR;
    return RESULT;
}

void KKVIndexConfig::Check() const
{
    _impl->indexPreference.Check();

    if (_impl->prefixInfo.keyType != indexlib::config::KKVKeyType::PREFIX) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key type wrong");
    }
    if (_impl->prefixInfo.NeedTruncate()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set count_limits");
    }
    if (_impl->prefixInfo.protectionThreshold != indexlib::config::KKVIndexFieldInfo::DEFAULT_PROTECTION_THRESHOLD) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set %s",
                             indexlib::config::KKVIndexFieldInfo::KKV_BUILD_PROTECTION_THRESHOLD);
    }
    if (!_impl->prefixInfo.sortParams.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set trunc_sort_params");
    }

    if (_impl->suffixInfo.keyType != indexlib::config::KKVKeyType::SUFFIX) {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key type wrong");
    }

    if (_impl->prefixFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support multi_value field!");
    }

    if (_impl->suffixFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key not support multi_value field!");
    }

    if (_impl->suffixInfo.NeedTruncate() && _impl->suffixInfo.countLimits > _impl->suffixInfo.protectionThreshold) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "suffix count_limits [%u] "
                             "should be less than %s [%u]",
                             _impl->suffixInfo.countLimits,
                             indexlib::config::KKVIndexFieldInfo::KKV_BUILD_PROTECTION_THRESHOLD,
                             _impl->suffixInfo.protectionThreshold);
    }

    CheckSortParams();
}

void KKVIndexConfig::CheckSortParams() const
{
    vector<std::shared_ptr<index::AttributeConfig>> subAttrConfigs;
    const auto& valueConfig = GetValueConfig();
    size_t attributeCount = valueConfig->GetAttributeCount();
    subAttrConfigs.reserve(attributeCount);
    for (size_t i = 0; i < attributeCount; ++i) {
        subAttrConfigs.push_back(valueConfig->GetAttributeConfig(i));
    }

    for (size_t i = 0; i < _impl->suffixInfo.sortParams.size(); i++) {
        auto& param = _impl->suffixInfo.sortParams[i];
        string sortField = param.GetSortField();
        if (sortField == KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME) {
            continue;
        }
        if (sortField == _impl->suffixFieldConfig->GetFieldName()) {
            if (_impl->suffixFieldConfig->GetFieldType() == ft_string) {
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

void KKVIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                 const config::IndexConfigDeserializeResource& resource)
{
    KVIndexConfig::Deserialize(any, idxInJsonArray, resource);
    auto json = autil::legacy::Jsonizable::JsonWrapper(any);

    vector<indexlib::config::KKVIndexFieldInfo> fieldInfo;
    json.Jsonize("index_fields", fieldInfo);
    if (fieldInfo.size() != 2) {
        INDEXLIB_FATAL_ERROR(Schema, "kkv index config must has two fields");
    }
    if (fieldInfo[0].keyType == indexlib::config::KKVKeyType::PREFIX) {
        _impl->prefixInfo = fieldInfo[0];
        _impl->suffixInfo = fieldInfo[1];
    } else {
        _impl->prefixInfo = fieldInfo[1];
        _impl->suffixInfo = fieldInfo[0];
    }

    json.Jsonize(INDEX_PREFERENCE_NAME, _impl->indexPreference, _impl->indexPreference);
    InitDefaultSortParams();
    InitDefaultSkipListThreshold();
    InitStorageKKVOptimize();
    DisableUnsupportParam();
    bool useNumberHash = UseNumberHash();
    json.Jsonize(USE_NUMBER_HASH, useNumberHash, useNumberHash);
    SetUseNumberHash(useNumberHash);

    InitKeyFieldConfig(resource);
    OptimizeKKVSKeyStore();
    InitValueAttributeConfig(resource);
}

void KKVIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    KVIndexConfig::Serialize(json);
    vector<indexlib::config::KKVIndexFieldInfo> fieldInfo;
    fieldInfo.push_back(_impl->prefixInfo);
    fieldInfo.push_back(_impl->suffixInfo);
    json.Jsonize(INDEX_FIELDS, fieldInfo);
    json.Jsonize(INDEX_PREFERENCE_NAME, _impl->indexPreference);
    json.Jsonize(USE_NUMBER_HASH, UseNumberHash());
}

void KKVIndexConfig::InitStorageKKVOptimize()
{
    if (!autil::EnvUtil::getEnv("INDEXLIB_OPT_KV_STORE", false)) {
        return;
    }

    AUTIL_LOG(INFO, "enable kkv storage optimize.");
    auto skeyParam = _impl->indexPreference.GetSkeyParam();
    if (!skeyParam.GetFileCompressType().empty()) {
        skeyParam.SetCompressParameter("encode_address_mapper", "true");
    }
    auto valueParam = _impl->indexPreference.GetValueParam();
    if (!valueParam.GetFileCompressType().empty()) {
        valueParam.SetCompressParameter("encode_address_mapper", "true");
    }
    valueParam.EnableValueImpact(true);
    _impl->indexPreference.SetSkeyParam(skeyParam);
    _impl->indexPreference.SetValueParam(valueParam);
}

void KKVIndexConfig::InitKeyFieldConfig(const config::IndexConfigDeserializeResource& resource)
{
    const auto& pkeyFieldConfig = resource.GetFieldConfig(_impl->prefixInfo.fieldName);
    if (!pkeyFieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "pkeyFieldConfig is null");
    }
    const auto& skeyFieldConfig = resource.GetFieldConfig(_impl->suffixInfo.fieldName);
    if (!skeyFieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "skeyFieldConfig is null");
    }
    SetPrefixFieldConfig(pkeyFieldConfig);
    SetSuffixFieldConfig(skeyFieldConfig);
}

void KKVIndexConfig::OptimizeKKVSKeyStore()
{
    if (!GetSuffixFieldInfo().enableStoreOptimize) {
        AUTIL_LOG(WARN, "StoreOptimize is disabled");
        return;
    }
    if (GetSuffixHashFunctionType() == indexlib::hft_murmur) {
        return;
    }
    const auto& skeyFieldConfig = GetSuffixFieldConfig();
    const auto& valueConfig = GetValueConfig();
    size_t attrCount = valueConfig->GetAttributeCount();
    vector<std::shared_ptr<index::AttributeConfig>> optAttrConfigs;
    optAttrConfigs.reserve(attrCount);
    for (size_t i = 0; i < attrCount; ++i) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(i);
        if (attrConfig->GetAttrName() == skeyFieldConfig->GetFieldName()) {
            continue;
        }
        optAttrConfigs.push_back(attrConfig);
    }
    if (optAttrConfigs.empty()) {
        return;
    }

    SetOptimizedStoreSKey(true);

    if (optAttrConfigs.size() != attrCount) {
        // TODO(qisa.cb) hard code, have to change valueConfig
        auto newValueConfig = std::make_shared<indexlibv2::config::ValueConfig>();
        auto status = newValueConfig->Init(optAttrConfigs);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "KKVIndexConfig valueConfig init failed, indexName: %s, status: %s",
                      GetIndexName().c_str(), status.ToString().c_str());
            return;
        }
        newValueConfig->EnableValueImpact(GetIndexPreference().GetValueParam().IsValueImpact());
        newValueConfig->EnablePlainFormat(GetIndexPreference().GetValueParam().IsPlainFormat());
        SetValueConfig(newValueConfig);
    }
}

void KKVIndexConfig::InitValueAttributeConfig(const config::IndexConfigDeserializeResource& resource)
{
    // set field_id for document parser;
    const auto& valueConfig = GetValueConfig();
    size_t attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; ++i) {
        auto& innerFieldConfig = valueConfig->GetAttributeConfig(i)->GetFieldConfig();
        const auto& fieldConfig = resource.GetFieldConfig(innerFieldConfig->GetFieldName());
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "set field config failed");
        }
        innerFieldConfig->SetFieldId(fieldConfig->GetFieldId());
    }
}

void KKVIndexConfig::InitDefaultSortParams()
{
    if (_impl->suffixInfo.sortParams.empty() &&
        (_impl->suffixInfo.NeedTruncate() || _impl->suffixInfo.enableKeepSortSequence)) {
        indexlib::config::SortParam param;
        param.SetSortField(KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME);
        AUTIL_LOG(INFO, "use default sort param : [%s]", param.toSortDescription().c_str());
        _impl->suffixInfo.sortParams.push_back(param);
    }
}

void KKVIndexConfig::InitDefaultSkipListThreshold()
{
    if (_impl->suffixInfo.skipListThreshold == indexlib::config::KKVIndexFieldInfo::INVALID_SKIPLIST_THRESHOLD) {
        _impl->suffixInfo.skipListThreshold = DEFAULT_SKIPLIT_THRESHOLD;
    }
}

void KKVIndexConfig::DisableUnsupportParam()
{
    AUTIL_LOG(INFO, "Disable enable_compact_hash_key & enable_shorten_offset for kkv table");

    indexlib::config::KKVIndexPreference::HashDictParam dictParam = _impl->indexPreference.GetHashDictParam();
    dictParam.SetEnableCompactHashKey(false);
    dictParam.SetEnableShortenOffset(false);
    _impl->indexPreference.SetHashDictParam(dictParam);
}

void KKVIndexConfig::SetPrefixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    SetFieldConfig(fieldConfig);
    _impl->prefixFieldConfig = fieldConfig;
}
void KKVIndexConfig::SetSuffixFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    _impl->suffixFieldConfig = fieldConfig;
}
const std::string& KKVIndexConfig::GetPrefixFieldName() { return _impl->prefixInfo.fieldName; }
const std::string& KKVIndexConfig::GetSuffixFieldName() { return _impl->suffixInfo.fieldName; }
void KKVIndexConfig::SetPrefixFieldInfo(const indexlib::config::KKVIndexFieldInfo& fieldInfo)
{
    _impl->prefixInfo = fieldInfo;
}
void KKVIndexConfig::SetSuffixFieldInfo(const indexlib::config::KKVIndexFieldInfo& fieldInfo)
{
    _impl->suffixInfo = fieldInfo;
}

const indexlib::config::KKVIndexFieldInfo& KKVIndexConfig::GetPrefixFieldInfo() const { return _impl->prefixInfo; }
const indexlib::config::KKVIndexFieldInfo& KKVIndexConfig::GetSuffixFieldInfo() const { return _impl->suffixInfo; }

const std::shared_ptr<FieldConfig>& KKVIndexConfig::GetPrefixFieldConfig() const { return _impl->prefixFieldConfig; }
const std::shared_ptr<FieldConfig>& KKVIndexConfig::GetSuffixFieldConfig() const { return _impl->suffixFieldConfig; }

bool KKVIndexConfig::NeedSuffixKeyTruncate() const { return _impl->suffixInfo.NeedTruncate(); }
uint32_t KKVIndexConfig::GetSuffixKeyTruncateLimits() const { return _impl->suffixInfo.countLimits; }
uint32_t KKVIndexConfig::GetSuffixKeySkipListThreshold() const { return _impl->suffixInfo.skipListThreshold; }
uint32_t KKVIndexConfig::GetSuffixKeyProtectionThreshold() const { return _impl->suffixInfo.protectionThreshold; }
const indexlib::config::SortParams& KKVIndexConfig::GetSuffixKeyTruncateParams() const
{
    return _impl->suffixInfo.sortParams;
}

bool KKVIndexConfig::EnableSuffixKeyKeepSortSequence() const { return _impl->suffixInfo.enableKeepSortSequence; }
void KKVIndexConfig::SetSuffixKeyTruncateLimits(uint32_t countLimits) { _impl->suffixInfo.countLimits = countLimits; }
void KKVIndexConfig::SetSuffixKeyProtectionThreshold(uint32_t countLimits)
{
    _impl->suffixInfo.protectionThreshold = countLimits;
}
void KKVIndexConfig::SetSuffixKeySkipListThreshold(uint32_t countLimits)
{
    _impl->suffixInfo.skipListThreshold = countLimits;
}

void KKVIndexConfig::SetOptimizedStoreSKey(bool optStoreSKey) { _impl->optStoreSKey = optStoreSKey; }

bool KKVIndexConfig::OptimizedStoreSKey() const { return _impl->optStoreSKey; }

indexlib::HashFunctionType KKVIndexConfig::GetPrefixHashFunctionType() const
{
    return GetHashFunctionType(_impl->prefixFieldConfig->GetFieldType(), UseNumberHash());
}
indexlib::HashFunctionType KKVIndexConfig::GetSuffixHashFunctionType() const
{
    return GetHashFunctionType(_impl->suffixFieldConfig->GetFieldType(), true);
}

bool KKVIndexConfig::DenyEmptySuffixKey() const { return _impl->denyEmptySKey; }
void KKVIndexConfig::SetDenyEmptySuffixKey(bool flag) { _impl->denyEmptySKey = flag; }

void KKVIndexConfig::SetIndexPreference(const indexlib::config::KKVIndexPreference& indexPreference)
{
    auto valueConfig = GetValueConfig();
    if (valueConfig) {
        valueConfig->EnableValueImpact(indexPreference.GetValueParam().IsValueImpact());
        valueConfig->EnablePlainFormat(indexPreference.GetValueParam().IsPlainFormat());
    }
    _impl->indexPreference = indexPreference;
}

const indexlib::config::KKVIndexPreference& KKVIndexConfig::GetIndexPreference() const
{
    return _impl->indexPreference;
}
indexlib::config::KKVIndexPreference& KKVIndexConfig::GetIndexPreference() { return _impl->indexPreference; }

Status KKVIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const KKVIndexConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to KKVIndexConfig failed");
    }
    auto status = KVIndexConfig::CheckCompatible(other);
    RETURN_IF_STATUS_ERROR(status, "kv index config check compatible failed");
    CHECK_CONFIG_EQUAL(_impl->prefixFieldConfig->GetFieldName(), typedOther->_impl->prefixFieldConfig->GetFieldName(),
                       "prefix field not equal");
    CHECK_CONFIG_EQUAL(_impl->suffixFieldConfig->GetFieldName(), typedOther->_impl->suffixFieldConfig->GetFieldName(),
                       "suffix field not equal");
    status = _impl->prefixInfo.CheckEqual(typedOther->_impl->prefixInfo);
    RETURN_IF_STATUS_ERROR(status, "prefix info not equal");
    status = _impl->suffixInfo.CheckEqual(typedOther->_impl->suffixInfo);
    RETURN_IF_STATUS_ERROR(status, "suffix info not equal");
    CHECK_CONFIG_EQUAL(_impl->optStoreSKey, typedOther->_impl->optStoreSKey, "opt store skey not equal");
    return Status::OK();
}

} // namespace indexlibv2::config
