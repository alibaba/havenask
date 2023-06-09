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

#include <unordered_map>

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/misc/log.h"

namespace indexlib::config {
class IndexPartitionSchema;

class LegacySchemaAdapter : public indexlibv2::config::ITabletSchema
{
private:
    using IIndexConfig = indexlibv2::config::IIndexConfig;
    using IIndexConfigVector = std::vector<std::shared_ptr<IIndexConfig>>;

public:
    LegacySchemaAdapter(const std::shared_ptr<IndexPartitionSchema>& legacySchema);
    ~LegacySchemaAdapter() = default;

    // ITabletSchema
    const std::string& GetTableType() const override;
    const std::string& GetTableName() const override;
    std::shared_ptr<indexlibv2::config::FieldConfig> GetFieldConfig(const std::string& fieldName) const override;
    fieldid_t GetFieldId(const std::string& fieldName) const override;
    std::shared_ptr<indexlibv2::config::FieldConfig> GetFieldConfig(fieldid_t fieldId) const override;
    size_t GetFieldCount() const override;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>
    GetIndexFieldConfigs(const std::string& indexType) const override;
    IIndexConfigVector GetIndexConfigs(const std::string& indexType) const override;
    std::shared_ptr<indexlibv2::config::IIndexConfig> GetIndexConfig(const std::string& indexType,
                                                                     const std::string& indexName) const override;
    std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const override;
    bool GetValueFromUserDefinedParam(const std::string& key, std::string& value) const override;
    void SetUserDefinedParam(const std::string& key, const std::string& value) override;
    autil::legacy::json::JsonMap GetUserDefinedParam() const override;
    bool Serialize(bool isCompact, std::string* output) const override;
    const std::shared_ptr<IndexPartitionSchema>& GetLegacySchema() const override { return _legacySchema; }

public:
    static std::shared_ptr<indexlibv2::config::ITabletSchema> LoadLegacySchema(const std::string& jsonStr);

    static const std::shared_ptr<indexlib::config::IndexPartitionSchema>&
    GetLegacySchema(const indexlibv2::config::ITabletSchema& schema);

private:
    void Init();
    void AddIndexConfigToMap(const std::string& type, const std::shared_ptr<IIndexConfig>& indexConfig);

private:
    std::shared_ptr<IndexPartitionSchema> _legacySchema;

    IIndexConfigVector _primaryKeyIndexConfigs;
    IIndexConfigVector _invertedIndexConfigs;
    IIndexConfigVector _attributeConfigs;
    IIndexConfigVector _virtualAttributeConfigs;
    IIndexConfigVector _summaryIndexConfigs;
    IIndexConfigVector _kvIndexConfigs;
    IIndexConfigVector _kkvIndexConfigs;

    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> _generalizedValueIndexFieldConfigs;

    std::map<std::pair</*type*/ std::string, /*name*/ std::string>, std::shared_ptr<IIndexConfig>> _indexConfigMap;
    std::shared_ptr<indexlibv2::config::IIndexConfig> _primaryKeyIndexConfig;

private:
    IE_LOG_DECLARE();
};

} // namespace indexlib::config
