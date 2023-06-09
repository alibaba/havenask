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

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/SortDescription.h"

namespace indexlibv2::config {
class FieldConfig;
class ValueConfig;

class AggregationIndexConfig : public IIndexConfig, public autil::legacy::Jsonizable
{
public:
    AggregationIndexConfig();
    ~AggregationIndexConfig();

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    void Check() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status CheckCompatible(const IIndexConfig* other) const override;

public:
    const std::vector<std::shared_ptr<FieldConfig>>& GetKeyFields() const;
    const std::shared_ptr<ValueConfig>& GetValueConfig() const;
    const SortDescriptions& GetSortDescriptions() const;
    size_t GetInitialKeyCount() const;
    bool NeedDedup() const;
    size_t GetRecordCountPerBlock() const;
    const std::vector<std::string>& GetValueFields() const;

    const std::string& GetValueUniqueField() const;
    Status SetUniqueField(const std::shared_ptr<config::FieldConfig>& uniqueField,
                          const std::shared_ptr<config::FieldConfig>& versionField, bool sortDeleteData = true);
    const std::shared_ptr<config::FieldConfig>& GetUniqueField() const;
    const std::shared_ptr<config::FieldConfig>& GetVersionField() const;

    bool SupportDelete() const;
    const std::shared_ptr<AggregationIndexConfig>& GetDeleteConfig() const;

    bool IsDataSorted() const;
    bool IsDeleteDataSorted() const;
    bool IsDataAndDeleteDataSameSort() const;
    bool IsDataSortedBy(const std::string& fieldName) const;

private:
    void ComplementWithResource(const IndexConfigDeserializeResource& resource);
    Status SetDeleteConfig(bool sort = true);
    std::shared_ptr<AggregationIndexConfig> MakeDeleteIndexConfig(bool sort) const;
    std::shared_ptr<ValueConfig> MakeDeleteValueConfig() const;
    Status AddFieldToValue(const std::shared_ptr<config::FieldConfig>& field);

public:
    // fieldDescriptions: fieldName:filedType:isMulti
    // keyFields: field1;field2
    // valueFields: field1;field2
    // sortFields: field1
    static std::shared_ptr<AggregationIndexConfig>
    TEST_Create(const std::string& indexName, const std::string& fieldDescriptions, const std::string& keyFields,
                const std::string& valueFields, const std::string& sortFields, bool dedup = false,
                const std::string& uniqueField = "", const std::string& versionField = "");
    void TEST_SetRecordCountPerBlock(uint32_t blockSize);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
