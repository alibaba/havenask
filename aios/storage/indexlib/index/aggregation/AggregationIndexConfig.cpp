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
#include "indexlib/index/aggregation/AggregationIndexConfig.h"

#include <algorithm>

#include "autil/StringTokenizer.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, AggregationIndexConfig);

static const std::string DELETED_DATA_DIR_SUFFIX = "_deleted";
static constexpr size_t DEFAULT_RECORD_COUNT_PER_BLOCK = 4096;

struct KeyDescription final : public autil::legacy::Jsonizable {
    static constexpr size_t DEFAULT_INITIAL_KEY_COUNT = 1024;

    std::vector<std::string> fieldNames;
    size_t initialKeyCount = DEFAULT_INITIAL_KEY_COUNT;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("fields", fieldNames);
        json.Jsonize("initial_key_count", initialKeyCount, initialKeyCount);
    }
};

struct ValueDescription final : public autil::legacy::Jsonizable {
    std::vector<std::string> fieldNames;
    SortDescriptions sortDescriptions;
    bool dedup = false;
    size_t recordCountPerBlock = DEFAULT_RECORD_COUNT_PER_BLOCK;
    // TODO: add options for compression, encoding...

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("fields", fieldNames);
        json.Jsonize("sort_descriptions", sortDescriptions, sortDescriptions);
        json.Jsonize("dedup", dedup, dedup);
        json.Jsonize("record_count_per_block", recordCountPerBlock, recordCountPerBlock);
    }
};

struct AggregationIndexConfig::Impl {
    std::string indexName;
    KeyDescription keyDescription;
    ValueDescription valueDescription;

    std::vector<std::shared_ptr<FieldConfig>> keyFields;
    std::shared_ptr<ValueConfig> valueConfig;

    std::shared_ptr<FieldConfig> uniqueField;
    std::shared_ptr<FieldConfig> versionField;
    std::shared_ptr<AggregationIndexConfig> deleteConfig;
};

AggregationIndexConfig::AggregationIndexConfig() { _impl = std::make_unique<Impl>(); }

AggregationIndexConfig::~AggregationIndexConfig() {}

const std::string& AggregationIndexConfig::GetIndexType() const { return index::AGGREGATION_INDEX_TYPE_STR; }

const std::string& AggregationIndexConfig::GetIndexName() const { return _impl->indexName; }

std::vector<std::string> AggregationIndexConfig::GetIndexPath() const
{
    return {index::AGGREGATION_INDEX_PATH + "/" + GetIndexName()};
}

std::vector<std::shared_ptr<FieldConfig>> AggregationIndexConfig::GetFieldConfigs() const { return GetKeyFields(); }

void AggregationIndexConfig::Check() const
{
    assert(_impl);
    if (_impl->keyFields.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "%s: not key fields specified", GetIndexName().c_str());
    }
    if (!_impl->valueConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "%s: value config is undefined", GetIndexName().c_str());
    }
    if (_impl->valueConfig->GetFixedLength() < 0) {
        INDEXLIB_FATAL_ERROR(UnImplement, "[%s:%s]: only support fixed-size value now", GetIndexType().c_str(),
                             GetIndexName().c_str());
    }
}

void AggregationIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                         const IndexConfigDeserializeResource& resource)
{
    autil::legacy::Jsonizable::JsonWrapper wrapper(any);
    Jsonize(wrapper);
    ComplementWithResource(resource);
}

void AggregationIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    AggregationIndexConfig* mu = const_cast<AggregationIndexConfig*>(this);
    mu->Jsonize(json);
}

void AggregationIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("index_name", _impl->indexName);
    json.Jsonize("key", _impl->keyDescription);
    json.Jsonize("value", _impl->valueDescription);
    json.Jsonize("unique_field", _impl->uniqueField, _impl->uniqueField);
}

const std::vector<std::shared_ptr<FieldConfig>>& AggregationIndexConfig::GetKeyFields() const
{
    return _impl->keyFields;
}

const std::shared_ptr<ValueConfig>& AggregationIndexConfig::GetValueConfig() const { return _impl->valueConfig; }

const SortDescriptions& AggregationIndexConfig::GetSortDescriptions() const
{
    return _impl->valueDescription.sortDescriptions;
}

bool AggregationIndexConfig::NeedDedup() const { return _impl->valueDescription.dedup; }

const std::vector<std::string>& AggregationIndexConfig::GetValueFields() const
{
    return _impl->valueDescription.fieldNames;
}

size_t AggregationIndexConfig::GetInitialKeyCount() const { return _impl->keyDescription.initialKeyCount; }

size_t AggregationIndexConfig::GetRecordCountPerBlock() const { return _impl->valueDescription.recordCountPerBlock; }

void AggregationIndexConfig::ComplementWithResource(const IndexConfigDeserializeResource& resource)
{
    for (const auto& fieldName : _impl->keyDescription.fieldNames) {
        auto fieldConfig = resource.GetFieldConfig(fieldName);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "key %s does not exist in field_config", fieldName.c_str());
        }
        _impl->keyFields.emplace_back(std::move(fieldConfig));
    }

    auto valueConfig = std::make_shared<ValueConfig>();
    for (const auto& fieldName : _impl->valueDescription.fieldNames) {
        auto fieldConfig = resource.GetFieldConfig(fieldName);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "value %s does not exist in field_config", fieldName.c_str());
        }
        auto s = valueConfig->AppendField(fieldConfig);
        if (!s.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "append field %s to ValueConfig failed, error: %s", fieldName.c_str(),
                                 s.ToString().c_str());
        }
    }
    valueConfig->EnableCompactFormat(true);
    valueConfig->EnablePlainFormat(true);
    _impl->valueConfig = std::move(valueConfig);
}

Status AggregationIndexConfig::SetUniqueField(const std::shared_ptr<config::FieldConfig>& uniqueField,
                                              const std::shared_ptr<config::FieldConfig>& versionField,
                                              bool sortDeleteData)
{
    _impl->valueDescription.dedup = false;
    if (_impl->deleteConfig) {
        return Status::OK();
    }

    _impl->uniqueField = uniqueField;
    _impl->versionField = versionField;
    RETURN_STATUS_DIRECTLY_IF_ERROR(AddFieldToValue(_impl->uniqueField));
    RETURN_STATUS_DIRECTLY_IF_ERROR(AddFieldToValue(_impl->versionField));
    return SetDeleteConfig(sortDeleteData);
}

Status AggregationIndexConfig::AddFieldToValue(const std::shared_ptr<config::FieldConfig>& field)
{
    bool found = false;
    for (size_t i = 0; i < _impl->valueConfig->GetAttributeCount(); ++i) {
        const auto& attrConfig = _impl->valueConfig->GetAttributeConfig(i);
        const auto& fieldConfig = attrConfig->GetFieldConfig();
        if (fieldConfig->GetFieldName() == field->GetFieldName()) {
            auto s = fieldConfig->CheckEqual(*field);
            if (!s.IsOK()) {
                return s;
            }
            found = true;
            break;
        }
    }
    if (!found) {
        auto s = _impl->valueConfig->AppendField(field);
        if (!s.IsOK()) {
            return s;
        }
        _impl->valueDescription.fieldNames.push_back(field->GetFieldName());
    }
    return Status::OK();
}

const std::shared_ptr<config::FieldConfig>& AggregationIndexConfig::GetUniqueField() const
{
    return _impl->uniqueField;
}

const std::shared_ptr<config::FieldConfig>& AggregationIndexConfig::GetVersionField() const
{
    return _impl->versionField;
}

const std::shared_ptr<AggregationIndexConfig>& AggregationIndexConfig::GetDeleteConfig() const
{
    return _impl->deleteConfig;
}

std::shared_ptr<AggregationIndexConfig> AggregationIndexConfig::MakeDeleteIndexConfig(bool sort) const
{
    auto valueConfig = MakeDeleteValueConfig();
    if (!valueConfig) {
        return nullptr;
    }
    auto deleteIndexConfig = std::make_shared<AggregationIndexConfig>();
    deleteIndexConfig->_impl->indexName = _impl->indexName + DELETED_DATA_DIR_SUFFIX;
    deleteIndexConfig->_impl->keyDescription = _impl->keyDescription;
    deleteIndexConfig->_impl->keyFields = _impl->keyFields;
    deleteIndexConfig->_impl->valueDescription.fieldNames.push_back(_impl->uniqueField->GetFieldName());
    deleteIndexConfig->_impl->valueDescription.fieldNames.push_back(_impl->versionField->GetFieldName());
    if (sort) {
        deleteIndexConfig->_impl->valueDescription.sortDescriptions.push_back(
            SortDescription(_impl->uniqueField->GetFieldName(), sp_desc));
        deleteIndexConfig->_impl->valueDescription.sortDescriptions.push_back(
            SortDescription(_impl->versionField->GetFieldName(), sp_desc));
        deleteIndexConfig->_impl->valueDescription.dedup = true;
    }
    valueConfig->EnableCompactFormat(true);
    valueConfig->EnablePlainFormat(true);
    deleteIndexConfig->_impl->valueConfig = valueConfig;
    return deleteIndexConfig;
}

std::shared_ptr<ValueConfig> AggregationIndexConfig::MakeDeleteValueConfig() const
{
    auto delValueConfig = std::make_shared<ValueConfig>();
    auto s = delValueConfig->AppendField(_impl->uniqueField);
    if (!s.IsOK()) {
        return nullptr;
    }
    s = delValueConfig->AppendField(_impl->versionField);
    if (!s.IsOK()) {
        return nullptr;
    }
    return delValueConfig;
}

std::shared_ptr<AggregationIndexConfig>
AggregationIndexConfig::TEST_Create(const std::string& indexName, const std::string& fieldDescriptions,
                                    const std::string& keyFields, const std::string& valueFields,
                                    const std::string& sortFields, bool dedup, const std::string& uniqueField,
                                    const std::string& versionField)
{
    auto fieldVector = FieldConfig::TEST_CreateFields(fieldDescriptions);
    if (fieldVector.empty()) {
        return nullptr;
    }

    auto config = std::make_shared<AggregationIndexConfig>();
    auto* impl = config->_impl.get();

    impl->indexName = indexName;

    static constexpr char DELIM = ';';
    impl->keyDescription.fieldNames = autil::StringTokenizer::tokenize(autil::StringView(keyFields), DELIM);
    impl->valueDescription.fieldNames = autil::StringTokenizer::tokenize(autil::StringView(valueFields), DELIM);
    impl->valueDescription.sortDescriptions = SortDescription::TEST_Create(sortFields);
    impl->valueDescription.dedup = dedup;

    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldVector, autil::legacy::json::JsonMap(), runtimeSettings);
    try {
        config->ComplementWithResource(resource);
        config->Check();
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "create config failed, error: %s", e.what());
        return nullptr;
    }

    if (!uniqueField.empty()) {
        auto it = std::find_if(fieldVector.begin(), fieldVector.end(),
                               [&uniqueField](const auto& field) { return field->GetFieldName() == uniqueField; });
        if (it == fieldVector.end()) {
            AUTIL_LOG(ERROR, "uniqueField %s does not exist", uniqueField.c_str());
            return nullptr;
        }
        auto uniqFieldPtr = *it;
        it = std::find_if(fieldVector.begin(), fieldVector.end(),
                          [&versionField](const auto& field) { return field->GetFieldName() == versionField; });
        if (it == fieldVector.end()) {
            AUTIL_LOG(ERROR, "versionField %s does not exist", versionField.c_str());
            return nullptr;
        }
        auto versionFieldPtr = *it;
        auto s = config->SetUniqueField(uniqFieldPtr, versionFieldPtr);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "SetUniqueField failed, error:%s", s.ToString().c_str());
            return nullptr;
        }
    }

    return config;
}

const std::string& AggregationIndexConfig::GetValueUniqueField() const { return _impl->uniqueField->GetFieldName(); }

void AggregationIndexConfig::TEST_SetRecordCountPerBlock(uint32_t blockSize)
{
    _impl->valueDescription.recordCountPerBlock = blockSize;
}

Status AggregationIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const AggregationIndexConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to AggregationIndexConfig failed");
    }
    auto toJsonString = [](const config::IIndexConfig* config) {
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

bool AggregationIndexConfig::SupportDelete() const { return _impl->deleteConfig != nullptr; }

Status AggregationIndexConfig::SetDeleteConfig(bool sort)
{
    _impl->deleteConfig = MakeDeleteIndexConfig(sort);
    if (!_impl->deleteConfig) {
        return Status::ConfigError("MakeDeleteIndexConfig failed");
    }
    return Status::OK();
}

bool AggregationIndexConfig::IsDataSorted() const { return !GetSortDescriptions().empty(); }

bool AggregationIndexConfig::IsDeleteDataSorted() const
{
    const auto& deleteConfig = GetDeleteConfig();
    return deleteConfig && deleteConfig->IsDataSorted();
}

bool AggregationIndexConfig::IsDataAndDeleteDataSameSort() const
{
    const auto& deleteConfig = GetDeleteConfig();
    if (!deleteConfig) {
        return false;
    }
    const auto& dataSortDescs = GetSortDescriptions();
    const auto& delSortDescs = deleteConfig->GetSortDescriptions();
    if (dataSortDescs.empty() || delSortDescs.empty()) {
        return false;
    }
    const auto& uniqueField = GetUniqueField();
    return dataSortDescs[0].GetSortFieldName() == uniqueField->GetFieldName() && dataSortDescs[0] == delSortDescs[0];
}

bool AggregationIndexConfig::IsDataSortedBy(const std::string& fieldName) const
{
    const auto& sortDescs = GetSortDescriptions();
    return !sortDescs.empty() && fieldName == sortDescs[0].GetSortFieldName();
}

} // namespace indexlibv2::config
