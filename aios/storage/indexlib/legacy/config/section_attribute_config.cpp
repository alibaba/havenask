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
#include "indexlib/config/section_attribute_config.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SectionAttributeConfig);

SectionAttributeConfig::SectionAttributeConfig() {}

SectionAttributeConfig::SectionAttributeConfig(const string& compressString, bool hasSectionWeight, bool hasFieldId)
    : indexlibv2::config::SectionAttributeConfig(compressString, hasSectionWeight, hasFieldId)
{
}

SectionAttributeConfig::~SectionAttributeConfig() {}

void SectionAttributeConfig::AssertEqual(const SectionAttributeConfig& other) const
{
    auto status = indexlibv2::config::SectionAttributeConfig::CheckEqual(other);
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(AssertEqual, "section attribute config not equal");
    }
}

bool SectionAttributeConfig::operator==(const SectionAttributeConfig& other) const
{
    return indexlibv2::config::SectionAttributeConfig::operator==(other);
}

bool SectionAttributeConfig::operator!=(const SectionAttributeConfig& other) const
{
    return indexlibv2::config::SectionAttributeConfig::operator!=(other);
}

AttributeConfigPtr SectionAttributeConfig::CreateAttributeConfig(const string& indexName) const
{
    string fieldName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    // single string attribute
    FieldConfigPtr fieldConfig(new FieldConfig(fieldName, ft_string, false, true, false));
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_section));

    attrConfig->Init(fieldConfig);
    attrConfig->SetFileCompressConfig(indexlibv2::config::SectionAttributeConfig::GetFileCompressConfig());
    auto status =
        attrConfig->SetCompressType(indexlibv2::config::SectionAttributeConfig::GetCompressType().GetCompressStr());
    THROW_IF_STATUS_ERROR(status);
    return attrConfig;
}

}} // namespace indexlib::config
