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
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SingleFieldIndexConfig);

struct SingleFieldIndexConfig::Impl {
    shared_ptr<FieldConfig> fieldConfig;
    bool hasPrimaryKeyAttribute = false;
};

SingleFieldIndexConfig::SingleFieldIndexConfig(const string& indexName, InvertedIndexType indexType)
    : InvertedIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
}

SingleFieldIndexConfig::SingleFieldIndexConfig(const SingleFieldIndexConfig& other)
    : InvertedIndexConfig(other)
    , _impl(std::make_unique<Impl>(*(other._impl)))
{
}

SingleFieldIndexConfig::~SingleFieldIndexConfig() {}

SingleFieldIndexConfig& SingleFieldIndexConfig::operator=(const SingleFieldIndexConfig& other)
{
    if (this != &other) {
        InvertedIndexConfig::operator=(other);
        _impl = std::make_unique<Impl>(*(other._impl));
    }
    return *this;
}

uint32_t SingleFieldIndexConfig::GetFieldCount() const { return 1; }
void SingleFieldIndexConfig::Check() const
{
    InvertedIndexConfig::Check();
    CheckWhetherIsVirtualField();
}

void SingleFieldIndexConfig::CheckWhetherIsVirtualField() const
{
    bool valid = (IsVirtual() && _impl->fieldConfig->IsVirtual()) || (!IsVirtual() && !_impl->fieldConfig->IsVirtual());
    if (!valid) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "Index is virtual but field is not virtual or"
                             "Index is not virtual but field is virtual, index name [%s]",
                             GetIndexName().c_str());
    }
}

bool SingleFieldIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return (_impl->fieldConfig->GetFieldId() == fieldId);
}

void SingleFieldIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    if (IsVirtual()) {
        return;
    }
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    InvertedIndexConfig::Serialize(json);
    if (_impl->fieldConfig.get() != NULL) {
        string fieldName = _impl->fieldConfig->GetFieldName();
        json.Jsonize(INDEX_FIELDS, fieldName);
    }
}

Status SingleFieldIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = InvertedIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "inverted index config check equal fail");
    const SingleFieldIndexConfig& other2 = (const SingleFieldIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->hasPrimaryKeyAttribute, other2._impl->hasPrimaryKeyAttribute,
                       "hasPrimaryKeyAttribute not equal");
    if (_impl->fieldConfig) {
        status = _impl->fieldConfig->CheckEqual(*(other2._impl->fieldConfig));
        RETURN_IF_STATUS_ERROR(status, "field config check equal fail");
    }
    return Status::OK();
}

InvertedIndexConfig* SingleFieldIndexConfig::Clone() const { return new SingleFieldIndexConfig(*this); }

InvertedIndexConfig::Iterator SingleFieldIndexConfig::DoCreateIterator() const
{
    return InvertedIndexConfig::Iterator(_impl->fieldConfig);
}
int32_t SingleFieldIndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    fieldid_t fieldId = _impl->fieldConfig->GetFieldId();
    return fieldId == id ? 0 : -1;
}
bool SingleFieldIndexConfig::CheckFieldType(FieldType ft) const
{
#define DO_CHECK(flag)                                                                                                 \
    if (flag)                                                                                                          \
        return true;

    InvertedIndexType it = GetInvertedIndexType();
    DO_CHECK(it == it_trie);

    DO_CHECK(ft == ft_text && it == it_text);
    DO_CHECK(ft == ft_uint64 && it == it_datetime);
    DO_CHECK(ft == ft_date && it == it_datetime);
    DO_CHECK(ft == ft_time && it == it_datetime);
    DO_CHECK(ft == ft_timestamp && it == it_datetime);
    DO_CHECK(ft == ft_string && (it == it_string));
    DO_CHECK((it == it_number_int8 && ft == ft_int8) || (it == it_number_uint8 && ft == ft_uint8) ||
             (it == it_number_int16 && ft == ft_int16) || (it == it_number_uint16 && ft == ft_uint16) ||
             (it == it_number_int32 && ft == ft_int32) || (it == it_number_uint32 && ft == ft_uint32) ||
             (it == it_number_int64 && ft == ft_int64) || (it == it_number_uint64 && ft == ft_uint64));
    DO_CHECK((it == it_number || it == it_range) &&
             (ft == ft_int8 || ft == ft_uint8 || ft == ft_int16 || ft == ft_uint16 || ft == ft_integer ||
              ft == ft_uint32 || ft == ft_long || ft == ft_uint64));
    return false;
}
Status SingleFieldIndexConfig::SetFieldConfig(const shared_ptr<FieldConfig>& fieldConfig)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType)) {
        stringstream ss;
        ss << "InvertedIndexType " << InvertedIndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType())
           << " and FieldType " << FieldConfig::FieldTypeToStr(fieldType) << " Mismatch.";
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", ss.str().c_str());
    }

    if (fieldConfig->IsEnableNullField() &&
        (GetInvertedIndexType() == it_range || GetInvertedIndexType() == it_spatial)) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "InvertedIndexType [%s] not support enable null field [%s]",
                               InvertedIndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()),
                               fieldConfig->GetFieldName().c_str());
    }

    _impl->fieldConfig = fieldConfig;
    SetAnalyzer(_impl->fieldConfig->GetAnalyzerName());
    return Status::OK();
}
shared_ptr<FieldConfig> SingleFieldIndexConfig::GetFieldConfig() const { return _impl->fieldConfig; }

std::map<std::string, std::string> SingleFieldIndexConfig::GetDictHashParams() const
{
    assert(_impl->fieldConfig);
    return indexlib::util::ConvertFromJsonMap(_impl->fieldConfig->GetUserDefinedParam());
}
vector<shared_ptr<indexlibv2::config::FieldConfig>> SingleFieldIndexConfig::GetFieldConfigs() const
{
    return {_impl->fieldConfig};
}

void SingleFieldIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                           const config::IndexConfigDeserializeResource& resource)
{
    InvertedIndexConfig::DoDeserialize(any, resource);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    std::string fieldName;
    json.Jsonize(INDEX_FIELDS, fieldName);
    auto fieldConfig = resource.GetFieldConfig(fieldName);
    if (!fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "get field config [%s] failed, index name [%s]", fieldName.c_str(),
                             GetIndexName().c_str());
    }
    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType != ft_text && !GetAnalyzer().empty()) {
        std::stringstream ss;
        ss << "index field :" << fieldName << " can not set index analyzer, index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    if (fieldConfig->IsBinary()) {
        stringstream ss;
        ss << "index field :" << fieldName << " can not set isBinary = true, index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    auto status = SetFieldConfig(fieldConfig);
    THROW_IF_STATUS_ERROR(status);
    auto invertedIndexType = GetInvertedIndexType();
    if (invertedIndexType == it_number) {
        auto type = InvertedIndexConfig::FieldTypeToInvertedIndexType(fieldConfig->GetFieldType());
        SetInvertedIndexType(type);
    }
}
} // namespace indexlibv2::config
