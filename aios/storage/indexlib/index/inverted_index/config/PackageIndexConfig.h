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

#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::config {
class SectionAttributeConfig;

class PackageIndexConfig : public InvertedIndexConfig
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
    int32_t GetFieldIdxInPack(fieldid_t id) const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckEqual(const InvertedIndexConfig& other) const override;
    InvertedIndexConfig* Clone() const override;
    indexlib::util::KeyValueMap GetDictHashParams() const override;

    int32_t GetFieldBoost(fieldid_t id) const;
    void SetFieldBoost(fieldid_t id, int32_t boost);
    int32_t GetFieldIdxInPack(const std::string& fieldName) const;
    fieldid_t GetFieldId(int32_t fieldIdxInPack) const;
    int32_t GetTotalFieldBoost() const;
    Status AddFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig, int32_t boost);
    bool HasSectionAttribute() const;
    const std::shared_ptr<SectionAttributeConfig>& GetSectionAttributeConfig() const;
    void SetHasSectionAttributeFlag(bool flag);
    const std::vector<std::shared_ptr<FieldConfig>>& GetFieldConfigVector() const;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const override;
    Status SetDefaultAnalyzer();
    void SetMaxFirstOccInDoc(pos_t pos);
    pos_t GetMaxFirstOccInDoc() const;
    std::vector<std::string> GetIndexPath() const override;
    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig) override;
    void SetSectionAttributeConfig(std::shared_ptr<SectionAttributeConfig> sectionAttributeConfig);

    void Check() const override;

protected:
    bool CheckFieldType(FieldType ft) const override;
    InvertedIndexConfig::Iterator DoCreateIterator() const override;
    void DoDeserialize(const autil::legacy::Any& any, const config::IndexConfigDeserializeResource& resource) override;

private:
    void CheckDictHashMustBeDefault() const;

private:
    inline static const std::string FIELD_BOOST = "boost";
    inline static const std::string SECTION_ATTRIBUTE_CONFIG = "section_attribute_config";
    inline static const std::string HAS_SECTION_ATTRIBUTE = "has_section_attribute";
    inline static const std::string MAX_FIRSTOCC_IN_DOC = "max_firstocc_in_doc";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    friend class PackageIndexConfigTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
