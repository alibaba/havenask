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
#include "indexlib/document/orc/OrcSingleDocumentParser.h"

#include "indexlib/document/orc/OrcExtendDocFieldsConvertor.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/orc/OrcConfig.h"

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, OrcSingleDocumentParser);

OrcSingleDocumentParser::OrcSingleDocumentParser() {}

OrcSingleDocumentParser::~OrcSingleDocumentParser() {}

bool OrcSingleDocumentParser::prepareIndexConfigMap()
{
    assert(_schema);
    const auto& configs = _schema->GetIndexConfigs(indexlibv2::index::ORC_INDEX_TYPE_STR);
    if (configs.empty()) {
        AUTIL_LOG(ERROR, "schema[%s] has no orc index", _schema->GetTableName().c_str());
        return false;
    }
    if (configs.size() != 1u) {
        AUTIL_LOG(ERROR, "schema[%s] orc configs size[%lu] error", _schema->GetTableName().c_str(), configs.size());
        return false;
    }
    const auto& orcConfig = std::dynamic_pointer_cast<indexlibv2::config::OrcConfig>(configs[0]);
    assert(orcConfig);
    size_t fieldCount = _schema->GetFieldCount();
    _fieldIdToAttrConfigs.clear();
    _fieldIdToAttrConfigs.resize(fieldCount);
    const auto& fieldConfigs = orcConfig->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        assert(fieldId >= 0 && static_cast<size_t>(fieldId) < fieldCount);
        auto attrConfig = std::make_shared<indexlibv2::config::AttributeConfig>();
        attrConfig->Init(fieldConfig);
        _fieldIdToAttrConfigs[fieldId] = std::move(attrConfig);
    }
    return true;
}

std::unique_ptr<indexlibv2::document::ExtendDocFieldsConvertor>
OrcSingleDocumentParser::CreateExtendDocFieldsConvertor() const
{
    return std::make_unique<OrcExtendDocFieldsConvertor>(_schema);
}

} // namespace indexlib::document
