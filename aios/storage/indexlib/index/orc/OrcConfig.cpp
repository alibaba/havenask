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
#include "indexlib/index/orc/OrcConfig.h"

#include <assert.h>
#include <numeric>
#include <sstream>

#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcGeneralOptions.h"
#include "indexlib/index/orc/OrcReaderOptions.h"
#include "indexlib/index/orc/OrcRowReaderOptions.h"
#include "indexlib/index/orc/OrcWriterOptions.h"
#include "indexlib/util/Exception.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, OrcConfig);

const std::string OrcConfig::ORC_FILE_NAME("orc.data");

const std::set<FieldType> OrcConfig::SUPPORTED_FIELD_TYPES = {
    ft_int8, ft_int16, ft_int32, ft_int64, ft_uint8, ft_uint16, ft_uint32, ft_uint64, ft_float, ft_double, ft_string};

OrcConfig::OrcConfig() {}

OrcConfig::OrcConfig(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs)
    : OrcConfig(index::DEFAULT_ORC_INDEX_NAME, fieldConfigs, OrcWriterOptions(), OrcReaderOptions(),
                OrcRowReaderOptions(), OrcGeneralOptions())
{
}

OrcConfig::OrcConfig(const std::string& indexName, const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs)
    : OrcConfig(indexName, fieldConfigs, OrcWriterOptions(), OrcReaderOptions(), OrcRowReaderOptions(),
                OrcGeneralOptions())
{
}

OrcConfig::OrcConfig(const std::string& indexName, const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs,
                     const OrcWriterOptions& writerOptions, const OrcReaderOptions& readerOptions,
                     const OrcRowReaderOptions& rowReaderOptions, const OrcGeneralOptions& generalOptions)
    : _indexName(indexName)
    , _fieldConfigs(fieldConfigs)
    , _writerOptions(writerOptions)
    , _readerOptions(readerOptions)
    , _rowReaderOptions(rowReaderOptions)
    , _generalOptions(generalOptions)
{
    for (size_t i = 0; i < fieldConfigs.size(); i++) {
        _fieldName2Id[fieldConfigs[i]->GetFieldName()] = i;
    }
}

std::vector<std::string> OrcConfig::GetIndexPath() const { return {std::string("orc/") + _indexName}; }
size_t OrcConfig::GetFieldId(const std::string& name) const
{
    const auto& iter = _fieldName2Id.find(name);
    if (iter == _fieldName2Id.end()) {
        return -1;
    }
    return iter->second;
}

FieldConfig* OrcConfig::GetFieldConfig(const std::string& name) const
{
    for (const auto& fieldConfig : _fieldConfigs) {
        if (fieldConfig->GetFieldName() == name) {
            return fieldConfig.get();
        }
    }
    return nullptr;
}

std::string OrcConfig::DebugString() const
{
    std::stringstream ss;
    ss << _indexName << "<";
    for (size_t i = 0; i < _fieldConfigs.size(); ++i) {
        ss << _fieldConfigs[i]->GetFieldName(); // TODO: Add FieldType
        if (i + 1 != _fieldConfigs.size()) {
            ss << ",";
        }
    }
    ss << ">";
    return ss.str();
}

namespace {

template <typename T>
void ParseOptions(const autil::legacy::json::JsonMap& jsonMap, const char* key, T& options)
{
    auto iter = jsonMap.find(key);
    if (iter != jsonMap.end()) {
        FromJson(options, iter->second);
    }
}

} // namespace

void OrcConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                            const config::IndexConfigDeserializeResource& resource)
{
    JsonMap jsonMap = AnyCast<JsonMap>(any);
    JsonMap::iterator iter = jsonMap.find("index_name");
    std::string indexName = index::DEFAULT_ORC_INDEX_NAME;
    if (iter != jsonMap.end()) {
        indexName = AnyCast<std::string>(iter->second);
    } else {
        AUTIL_LOG(WARN, "[index_name] not found, use default name[%s]", indexName.c_str());
    }
    iter = jsonMap.find("index_fields");
    if (iter == jsonMap.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "[index_fields] not found");
    }
    std::vector<std::shared_ptr<FieldConfig>> fields;
    JsonArray fieldArray = AnyCast<JsonArray>(iter->second);
    for (const auto& field : fieldArray) {
        auto fieldName = AnyCast<std::string>(field);
        auto fieldConfig = resource.GetFieldConfig(fieldName);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "try to get field config [%s] failed", fieldName.c_str());
        }
        fields.push_back(fieldConfig);
    }
    if (fields.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "field config is empty");
    }
    iter = jsonMap.find("index_options");
    OrcWriterOptions writerOptions;
    OrcReaderOptions readerOptions;
    OrcRowReaderOptions rowReaderOptions;
    OrcGeneralOptions generalOptions;
    if (iter != jsonMap.end()) {
        auto indexOptions = AnyCast<JsonMap>(iter->second);
        ParseOptions(indexOptions, "writer", writerOptions);
        ParseOptions(indexOptions, "reader", readerOptions);
        ParseOptions(indexOptions, "row_reader", rowReaderOptions);
        ParseOptions(indexOptions, "general", generalOptions);
    }

    *this = OrcConfig(indexName, fields, writerOptions, readerOptions, rowReaderOptions, generalOptions);
}

void OrcConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    json.Jsonize("index_name", _indexName);
    json.Jsonize("index_type", GetIndexType());
    JsonMap indexOptions;
    if (_writerOptions.NeedToJson()) {
        indexOptions["writer"] = ToJson(_writerOptions);
    }
    if (_readerOptions.NeedToJson()) {
        indexOptions["reader"] = ToJson(_readerOptions);
    }
    if (_rowReaderOptions.NeedToJson()) {
        indexOptions["row_reader"] = ToJson(_rowReaderOptions);
    }
    if (_generalOptions.NeedToJson()) {
        indexOptions["general"] = ToJson(_generalOptions);
    }
    if (!indexOptions.empty()) {
        json.Jsonize("index_options", indexOptions);
    }
    std::vector<std::string> fieldNames;
    for (const auto& field : _fieldConfigs) {
        fieldNames.push_back(field->GetFieldName());
    }
    json.Jsonize("index_fields", ToJson(fieldNames));
}

void OrcConfig::Check() const
{
    for (const auto& fieldConfig : _fieldConfigs) {
        if (SUPPORTED_FIELD_TYPES.count(fieldConfig->GetFieldType()) == 0) {
            INDEXLIB_FATAL_ERROR(Schema, "orc config check supported field type failed.");
        }
    }

    if (_generalOptions.GetRowGroupSize() <= 0) {
        INDEXLIB_FATAL_ERROR(Schema, "orc config check row group size failed.");
    }

    if (_generalOptions.GetBuildBufferSize() <= 0) {
        INDEXLIB_FATAL_ERROR(Schema, "orc config check build buffer size failed.");
    }
}

void OrcConfig::SetLegacyWriterOptions()
{
    _writerOptions.setCompression(orc::CompressionKind_ZSTD);
    _writerOptions.setIntegerEncodingVersion(orc::FastPFor256);
    _writerOptions.setRowIndexStride(0); // disable row index
    _writerOptions.setEnableStats(false);
}

const std::string& OrcConfig::GetOrcFileName() { return ORC_FILE_NAME; }

Status OrcConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const OrcConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to OrcConfig failed");
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

} // namespace indexlibv2::config
