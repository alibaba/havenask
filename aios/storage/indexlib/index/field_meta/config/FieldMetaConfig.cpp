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
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

#include "indexlib/index/common/Types.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaConfig);

struct FieldMetaConfig::Impl {
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig;
    std::string indexName;
    std::vector<FieldMetaInfo> metas;
    IndexStatus status = indexlib::is_normal;
    MetaSourceType metaSourceType = FieldMetaConfig::MetaSourceType::MST_NONE;
};

FieldMetaConfig::FieldMetaConfig() : _impl(std::make_unique<Impl>()) {}
FieldMetaConfig::~FieldMetaConfig() {}

Status FieldMetaConfig::Init(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                             const std::string& indexName)
{
    if (!fieldConfig) {
        return Status::InternalError("field meta config init failed. field config is nullptr");
    }
    _impl = std::make_unique<Impl>();
    _impl->indexName = indexName;
    _impl->fieldConfig = fieldConfig;

    const auto& supportMetas = GetSupportMetas(_impl->fieldConfig);
    for (const auto& meta : supportMetas) {
        _impl->metas.emplace_back(meta);
    }
    if (NeedFieldSource()) {
        _impl->metaSourceType = FieldMetaConfig::MetaSourceType::MST_FIELD;
    } else {
        assert(NeedTokenCount());
        _impl->metaSourceType = FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT;
    }
    return Status::OK();
}

const std::string& FieldMetaConfig::GetIndexType() const { return FIELD_META_INDEX_TYPE_STR; }
const std::string& FieldMetaConfig::GetIndexName() const { return _impl->indexName; }
const std::string& FieldMetaConfig::GetIndexCommonPath() const { return FIELD_META_INDEX_PATH; }
std::vector<std::string> FieldMetaConfig::GetIndexPath() const { return {GetIndexCommonPath() + "/" + GetIndexName()}; }
const std::vector<FieldMetaConfig::FieldMetaInfo>& FieldMetaConfig::GetFieldMetaInfos() const { return _impl->metas; }

std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> FieldMetaConfig::GetFieldConfigs() const
{
    return {_impl->fieldConfig};
}

std::shared_ptr<indexlibv2::config::FieldConfig> FieldMetaConfig::GetFieldConfig() const
{
    auto fieldConfigs = GetFieldConfigs();
    assert(fieldConfigs.size() == 1u);
    return fieldConfigs[0];
}

bool FieldMetaConfig::NeedFieldSource() const
{
    for (const auto& fieldMetaInfo : _impl->metas) {
        if (fieldMetaInfo.metaName == MIN_MAX_META_STR || fieldMetaInfo.metaName == HISTOGRAM_META_STR ||
            fieldMetaInfo.metaName == DATA_STATISTICS_META_STR) {
            return true;
        }
    }
    return false;
}
bool FieldMetaConfig::NeedTokenCount() const
{
    for (const auto& fieldMetaInfo : _impl->metas) {
        if (fieldMetaInfo.metaName == FIELD_TOKEN_COUNT_META_STR) {
            return true;
        }
    }
    return false;
}

void FieldMetaConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                  const indexlibv2::config::IndexConfigDeserializeResource& resource)
{
    auto json = autil::legacy::Jsonizable::JsonWrapper(any);
    std::string fieldName;
    bool needMetaSource = true;
    json.Jsonize(indexlibv2::config::FieldConfig::FIELD_NAME, fieldName, fieldName);
    json.Jsonize("index_name", _impl->indexName, _impl->indexName);
    json.Jsonize("metas", _impl->metas, _impl->metas);
    json.Jsonize("store_meta_source", needMetaSource, needMetaSource);
    _impl->fieldConfig = resource.GetFieldConfig(fieldName);
    if (!_impl->fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "init field meta [%s] for field [%s] failed", _impl->indexName.c_str(),
                             fieldName.c_str());
    }
    if (_impl->indexName.empty()) {
        _impl->indexName = fieldName;
    }
    if (_impl->metas.empty()) {
        const auto& supportMetas = GetSupportMetas(_impl->fieldConfig);
        for (const auto& meta : supportMetas) {
            _impl->metas.emplace_back(meta);
        }
    }
    if (needMetaSource) {
        if (NeedFieldSource()) {
            _impl->metaSourceType = FieldMetaConfig::MetaSourceType::MST_FIELD;
        } else {
            assert(NeedTokenCount());
            _impl->metaSourceType = FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT;
        }
    }
}
void FieldMetaConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    json.Jsonize(indexlibv2::config::FieldConfig::FIELD_NAME, _impl->fieldConfig->GetFieldName());
    json.Jsonize("index_name", _impl->indexName);
    json.Jsonize("metas", _impl->metas);
    json.Jsonize("store_meta_source", _impl->metaSourceType != MetaSourceType::MST_NONE);
}

void FieldMetaConfig::Check() const
{
    auto supportMetas = GetSupportMetas(_impl->fieldConfig);
    std::unordered_set<std::string> metas;
    for (const auto& fieldMeta : _impl->metas) {
        if (std::find(supportMetas.begin(), supportMetas.end(), fieldMeta.metaName) == supportMetas.end()) {
            INDEXLIB_FATAL_ERROR(Schema, "field type [%s], isMultiValue[%d] not support meta [%s]",
                                 indexlibv2::config::FieldConfig::FieldTypeToStr(_impl->fieldConfig->GetFieldType()),
                                 _impl->fieldConfig->IsMultiValue(), fieldMeta.metaName.c_str());
        }
        if (metas.find(fieldMeta.metaName) != metas.end()) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] same field meta [%s] can only config once", GetIndexName().c_str(),
                                 fieldMeta.metaName.c_str());
        }
        metas.insert(fieldMeta.metaName);

        CheckFieldMetaParam(fieldMeta);
    }
}

template <typename T>
void FieldMetaConfig::CheckHistogramMinMaxConfig(const FieldMetaConfig::FieldMetaInfo& info) const
{
    const auto& jsonMap = info.metaParams;
    auto maxValueIter = jsonMap.find(MAX_VALUE);
    auto minValueIter = jsonMap.find(MIN_VALUE);
    T maxValue, minValue;
    if (maxValueIter != jsonMap.end() && minValueIter != jsonMap.end()) {
        try {
            autil::legacy::FromJson(maxValue, maxValueIter->second);
            autil::legacy::FromJson(minValue, minValueIter->second);
        } catch (...) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] field meta [%s] param for max min value invalid",
                                 GetIndexName().c_str(), HISTOGRAM_META_STR.c_str());
        }
        if (maxValue < minValue) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] field meta [%s] param for max [%ld ] min [%ld] value invalid",
                                 GetIndexName().c_str(), HISTOGRAM_META_STR.c_str(), maxValue, minValue);
        }
    }
}

void FieldMetaConfig::CheckFieldMetaParam(const FieldMetaConfig::FieldMetaInfo& info) const
{
    if (info.metaName == HISTOGRAM_META_STR) {
        auto binsValue = util::GetTypeValueFromJsonMap<uint64_t>(info.metaParams, BINS, 10000u);
        if (!binsValue.has_value() || binsValue.value() == 0) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] field meta [%s] param for bins is invalid", GetIndexName().c_str(),
                                 HISTOGRAM_META_STR.c_str());
        }
        auto fieldType = GetFieldConfig()->GetFieldType();
        if (fieldType == ft_uint64) {
            CheckHistogramMinMaxConfig<uint64_t>(info);
        } else {
            CheckHistogramMinMaxConfig<int64_t>(info);
        }
    } else if (info.metaName == DATA_STATISTICS_META_STR) {
        auto value = util::GetTypeValueFromJsonMap<uint64_t>(info.metaParams, TOP_NUM, 1000);
        if (!value.has_value() || value.value() == 0) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] field meta [%s] param for [%s] is invalid", GetIndexName().c_str(),
                                 HISTOGRAM_META_STR.c_str(), TOP_NUM);
        }
    }
}

Status FieldMetaConfig::CheckCompatible(const indexlibv2::config::IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const FieldMetaConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to field meta config failed");
    }
    auto toJsonString = [](const indexlibv2::config::IIndexConfig* config) {
        autil::legacy::Jsonizable::JsonWrapper json;
        config->Serialize(json);
        return ToJsonString(json.GetMap());
    };
    const auto& jsonStr = toJsonString(this);
    const auto& jsonStrOther = toJsonString(typedOther);
    if (jsonStr != jsonStrOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "original config [%s] is not compatible with [%s]",
                               jsonStr.c_str(), jsonStrOther.c_str());
    }
    return Status::OK();
}

std::vector<std::string>
FieldMetaConfig::GetSupportMetas(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig) const
{
    auto fieldType = fieldConfig->GetFieldType();

    if (fieldConfig->IsMultiValue()) {
        return {};
    }
    switch (fieldType) {
    case ft_int32:
    case ft_int64:
    case ft_uint32:
    case ft_uint64:
    case ft_int8:
    case ft_uint8:
    case ft_int16:
    case ft_uint16:
        return {MIN_MAX_META_STR, HISTOGRAM_META_STR, DATA_STATISTICS_META_STR};
    case ft_double:
    case ft_float:
        return {MIN_MAX_META_STR};
    case ft_text:
        return {FIELD_TOKEN_COUNT_META_STR};
    case ft_string:
        return {DATA_STATISTICS_META_STR};
    default:
        return {};
    }

    return {};
}

std::string FieldMetaConfig::GetSourceFieldName() const { return GetFieldConfig()->GetFieldName(); }

FieldMetaConfig::MetaSourceType FieldMetaConfig::GetStoreMetaSourceType() const { return _impl->metaSourceType; }

std::vector<FieldMetaConfig::FieldMetaInfo>& FieldMetaConfig::TEST_GetFieldMetaInfos() { return _impl->metas; }
void FieldMetaConfig::TEST_SetStoreMetaSourceType(MetaSourceType type) { _impl->metaSourceType = type; }

bool FieldMetaConfig::IsDisabled() const { return _impl->status == indexlib::is_disable; }

bool FieldMetaConfig::SupportTokenCount() const
{
    if (_impl->metaSourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return false;
    }
    for (const auto& fieldMetaInfo : _impl->metas) {
        if (fieldMetaInfo.metaName == FIELD_TOKEN_COUNT_META_STR) {
            return true;
        }
    }
    return false;
}

} // namespace indexlib::index
