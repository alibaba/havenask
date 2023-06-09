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
#include "indexlib/config/LegacySchemaConvertor.h"

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_partition_schema.h"

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.config, LegacySchemaConvertor);

namespace indexlibv2::config {

Status LegacySchemaConvertor::FromJson(autil::legacy::Jsonizable::JsonWrapper& json, UnresolvedSchema* schemaImpl)
{
    auto legacySchema = std::make_shared<indexlib::config::IndexPartitionSchema>();
    try {
        legacySchema->SetLoadFromIndex(false);
        legacySchema->Jsonize(json);
        legacySchema->Check();
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "parse legacy schame failed: %s", e.what());
        return Status::ConfigError("parse legacy schema failed");
    }

    assert(schemaImpl);
    schemaImpl->_legacySchema = legacySchema;

    schemaImpl->_tableType = indexlib::config::IndexPartitionSchema::TableType2Str(legacySchema->GetTableType());
    schemaImpl->_tableName = legacySchema->GetSchemaName();
    schemaImpl->_schemaId = legacySchema->GetSchemaVersionId();

    const auto& userDefineParam = legacySchema->GetUserDefinedParam();
    for (const auto& [key, jsonValue] : userDefineParam) {
        if (schemaImpl->_settings.count(key) == 0) {
            schemaImpl->_settings[key] = jsonValue;
        }
    }

    auto fieldSchema = legacySchema->GetFieldSchema();
    if (fieldSchema) {
        ConvertFieldConfigs(*fieldSchema, schemaImpl);
    }

    return Status::OK();
}

autil::legacy::json::JsonMap LegacySchemaConvertor::ToJson(UnresolvedSchema* schemaImpl)
{
    auto legacySchema = schemaImpl->_legacySchema;
    if (!legacySchema) {
        return autil::legacy::json::JsonMap();
    }
    legacySchema->SetSchemaName(schemaImpl->_tableName);
    legacySchema->SetSchemaVersionId(schemaImpl->GetSchemaId());
    return autil::legacy::AnyCast<autil::legacy::json::JsonMap>(autil::legacy::ToJson(legacySchema.get()));
}

void LegacySchemaConvertor::ConvertFieldConfigs(const indexlib::config::FieldSchema& fieldSchema,
                                                UnresolvedSchema* schemaImpl)
{
    schemaImpl->_fields.clear();
    schemaImpl->_fieldNameToIdMap.clear();
    for (auto iter = fieldSchema.Begin(); iter != fieldSchema.End(); ++iter) {
        [[maybe_unused]] auto status = schemaImpl->AddFieldConfig(*iter);
        assert(status.IsOK());
    }
}

} // namespace indexlibv2::config
