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
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/Types.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlib::document {
class AttributeDocument;
}
namespace indexlibv2::config {
class ITabletSchema;
} // namespace indexlibv2::config
namespace indexlibv2::index {
class PackAttributeFormatter;
class AttributeConfig;
} // namespace indexlibv2::index

namespace indexlibv2::document {

class AttributeDocumentFieldExtractor
{
public:
    Status Init(const std::shared_ptr<config::ITabletSchema>& schema);
    autil::StringView GetField(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDoc, fieldid_t fieldId,
                               autil::mem_pool::Pool* pool);

private:
    void AddPackAttributeFormatter(std::unique_ptr<index::PackAttributeFormatter> formatter, packattrid_t packId);
    void AddAttributeConfig(const std::shared_ptr<index::AttributeConfig>& attrConfig);
    const std::shared_ptr<index::AttributeConfig>& GetAttributeConfig(fieldid_t fieldId) const;

private:
    std::vector<std::unique_ptr<index::PackAttributeFormatter>> _packFormatters; // index by packId
    std::shared_ptr<config::ITabletSchema> _schema;
    std::vector<std::shared_ptr<index::AttributeConfig>> _attrConfigs; // index by fieldId

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
