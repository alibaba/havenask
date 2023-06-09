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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {
class FileCompressConfig;
class FileCompressConfig;
class CompressTypeOption;
} // namespace indexlib::config

namespace indexlibv2::config {
class AttributeConfig;

class SectionAttributeConfig : public autil::legacy::Jsonizable
{
public:
    SectionAttributeConfig();
    // for test only
    SectionAttributeConfig(const std::string& compressString, bool hasSectionWeight, bool hasFieldId);
    ~SectionAttributeConfig();

public:
    bool IsUniqEncode() const;
    bool HasEquivalentCompress() const;

    bool HasSectionWeight() const;
    bool HasFieldId() const;
    Status CheckEqual(const SectionAttributeConfig& other) const;
    bool operator==(const SectionAttributeConfig& other) const;
    bool operator!=(const SectionAttributeConfig& other) const;

    std::shared_ptr<AttributeConfig> CreateAttributeConfig(const std::string& indexName) const;

    static std::string IndexNameToSectionAttributeName(const std::string& indexName);
    static std::string SectionAttributeNameToIndexName(const std::string& attrName);

    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig);

    std::shared_ptr<indexlib::config::FileCompressConfig> GetFileCompressConfig() const;
    indexlib::config::CompressTypeOption GetCompressType() const;

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    inline static const std::string HAS_SECTION_WEIGHT = "has_section_weight";
    inline static const std::string HAS_SECTION_FIELD_ID = "has_field_id";
    inline static const std::string SECTION_DIR_NAME_SUFFIX = "_section";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
