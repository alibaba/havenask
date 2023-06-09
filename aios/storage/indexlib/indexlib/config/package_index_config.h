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
#ifndef __INDEXLIB_PACKAGE_INDEX_CONFIG_H
#define __INDEXLIB_PACKAGE_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class PackageIndexConfig : public IndexConfig
{
public:
    static constexpr uint32_t PACK_MAX_FIELD_NUM = 32;
    static constexpr uint32_t EXPACK_MAX_FIELD_NUM = 8;
    static constexpr uint32_t CUSTOMIZED_MAX_FIELD_NUM = 32;

public:
    PackageIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    PackageIndexConfig(const PackageIndexConfig& other);
    ~PackageIndexConfig();

public:
    uint32_t GetFieldCount() const override;
    IndexConfig::Iterator CreateIterator() const override;
    int32_t GetFieldIdxInPack(fieldid_t id) const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;
    util::KeyValueMap GetDictHashParams() const override;

    int32_t GetFieldBoost(fieldid_t id) const;
    void SetFieldBoost(fieldid_t id, int32_t boost);
    int32_t GetFieldIdxInPack(const std::string& fieldName) const;
    fieldid_t GetFieldId(int32_t fieldIdxInPack) const;
    int32_t GetTotalFieldBoost() const;
    void AddFieldConfig(const FieldConfigPtr& fieldConfig, int32_t boost = 0);
    void AddFieldConfig(fieldid_t id, int32_t boost = 0);
    void AddFieldConfig(const std::string& fieldName, int32_t boost = 0);
    bool HasSectionAttribute() const;
    const SectionAttributeConfigPtr& GetSectionAttributeConfig() const;
    void SetHasSectionAttributeFlag(bool flag);
    const FieldConfigVector& GetFieldConfigVector() const;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;
    void SetFieldSchema(const FieldSchemaPtr& fieldSchema);
    void SetDefaultAnalyzer();
    void SetMaxFirstOccInDoc(pos_t pos);
    pos_t GetMaxFirstOccInDoc() const;
    std::vector<std::string> GetIndexPath() const override;
    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig) override;
    // only for test
    void SetSectionAttributeConfig(SectionAttributeConfigPtr sectionAttributeConfig);

    void Check() const override;

protected:
    bool CheckFieldType(FieldType ft) const override;
    bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const override;

private:
    void CheckDictHashMustBeDefault() const;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    friend class PackageIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_PACKAGE_INDEX_CONFIG_H
