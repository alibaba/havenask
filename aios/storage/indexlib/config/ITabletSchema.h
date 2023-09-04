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

#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/json.h"
#include "indexlib/base/Types.h"
#include "indexlib/util/JsonMap.h"

namespace indexlib::config {
class IndexPartitionSchema;
}

namespace indexlibv2::config {
class IIndexConfig;
class FieldConfig;
class MutableJson;

// DEFINITION: disabled indexes will be filtered or return nullptr by default
class ITabletSchema
{
public:
    virtual ~ITabletSchema() = default;

public:
    // base
    virtual const std::string& GetTableType() const = 0;
    virtual const std::string& GetTableName() const = 0;
    virtual schemaid_t GetSchemaId() const = 0;
    virtual std::string GetSchemaFileName() const = 0;
    // fields
    virtual size_t GetFieldCount() const = 0;
    virtual fieldid_t GetFieldId(const std::string& fieldName) const = 0;
    virtual std::shared_ptr<FieldConfig> GetFieldConfig(fieldid_t fieldId) const = 0;
    virtual std::shared_ptr<FieldConfig> GetFieldConfig(const std::string& fieldName) const = 0;
    virtual std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const = 0;
    virtual std::vector<std::shared_ptr<FieldConfig>> GetIndexFieldConfigs(const std::string& indexType) const = 0;
    // indexes
    virtual std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& indexType,
                                                         const std::string& indexName) const = 0;
    virtual std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs(const std::string& indexType) const = 0;
    virtual std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs() const = 0;
    virtual std::vector<std::shared_ptr<IIndexConfig>>
    GetIndexConfigsByFieldName(const std::string& fieldName) const = 0;
    virtual std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const = 0;
    // runtime settings
    virtual const MutableJson& GetRuntimeSettings() const = 0;
    // user defined param, the payload no affect on index, we just serialize and deserialize it
    virtual const indexlib::util::JsonMap& GetUserDefinedParam() const = 0;
    // serialization
    virtual bool Serialize(bool isCompact, std::string* output) const = 0;
    // legacy
    virtual const std::shared_ptr<indexlib::config::IndexPartitionSchema>& GetLegacySchema() const = 0;
};

} // namespace indexlibv2::config
