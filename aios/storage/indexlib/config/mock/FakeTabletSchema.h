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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/util/JsonMap.h"

namespace indexlib::config {
class IndexPartitionSchema;
} // namespace indexlib::config

namespace indexlibv2::config {
class IIndexConfig;
class FieldConfig;

class FakeTabletSchema : public ITabletSchema
{
public:
    FakeTabletSchema();
    ~FakeTabletSchema() override;

public:
    // base
    const std::string& GetTableType() const override;
    const std::string& GetTableName() const override;
    schemaid_t GetSchemaId() const override;
    std::string GetSchemaFileName() const override;
    // fields
    size_t GetFieldCount() const override;
    fieldid_t GetFieldId(const std::string& fieldName) const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(fieldid_t fieldId) const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(const std::string& fieldName) const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetIndexFieldConfigs(const std::string& indexType) const override;
    // indexes
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& indexType,
                                                 const std::string& indexName) const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs(const std::string& indexType) const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs() const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigsByFieldName(const std::string& fieldName) const override;
    std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const override;
    // settings
    const MutableJson& GetRuntimeSettings() const override;
    // user defined param, the payload no affect on index, we just serialize and deserialize it
    const indexlib::util::JsonMap& GetUserDefinedParam() const override;
    // serialization
    bool Serialize(bool isCompact, std::string* output) const override;
    // legacy
    const std::shared_ptr<indexlib::config::IndexPartitionSchema>& GetLegacySchema() const override;

public:
    std::vector<std::shared_ptr<FieldConfig>>& MutableFieldConfigs();
    std::vector<std::shared_ptr<IIndexConfig>>& MutableIndexConfigs();
    indexlib::util::JsonMap& MutableSettings();
    indexlib::util::JsonMap& MutableUserDefinedParam();
    MutableJson& MutableRuntimeSettings();

private:
    std::vector<std::shared_ptr<FieldConfig>> _fieldConfigs;
    std::vector<std::shared_ptr<IIndexConfig>> _indexConfigs;
    indexlib::util::JsonMap _settings;
    indexlib::util::JsonMap _userDefinedParam;
    MutableJson _runtimeSettings;
    std::string _tableType = "mock";
    std::string _tableName = "mock";
    schemaid_t _schemaId = DEFAULT_SCHEMAID;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////

} // namespace indexlibv2::config
