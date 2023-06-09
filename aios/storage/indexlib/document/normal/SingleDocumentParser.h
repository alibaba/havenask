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
#include <set>

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/document/normal/NullFieldAppender.h"

namespace indexlibv2::config {
class TabletSchema;
class AttributeConfig;
class SummaryIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2 { namespace document {
class ExtendDocFieldsConvertor;
class SectionAttributeAppender;
class PackAttributeAppender;

class SingleDocumentParser
{
public:
    SingleDocumentParser();
    virtual ~SingleDocumentParser();

public:
    bool Init(const std::shared_ptr<config::TabletSchema>& schema,
              std::shared_ptr<indexlib::util::AccumulativeCounter>& attrConvertErrorCounter);
    std::shared_ptr<NormalDocument> Parse(NormalExtendDocument* extendDoc);

public:
    bool IsSummaryIndexField(fieldid_t fieldId) const
    {
        return _summaryFieldIds.find(fieldId) != _summaryFieldIds.end();
    }
    bool IsAttributeIndexField(fieldid_t fieldId) const
    {
        return _attributeFieldIds.find(fieldId) != _attributeFieldIds.end();
    }
    bool IsInvertedIndexField(fieldid_t fieldId) const
    {
        return _invertedFieldIds.find(fieldId) != _invertedFieldIds.end();
    }

protected:
    virtual std::unique_ptr<ExtendDocFieldsConvertor> CreateExtendDocFieldsConvertor() const;
    virtual bool prepareIndexConfigMap();

private:
    const std::shared_ptr<indexlibv2::config::AttributeConfig>& GetAttributeConfig(fieldid_t fieldId) const;

    void SetPrimaryKeyField(NormalExtendDocument* document);

    void AddModifiedFields(const NormalExtendDocument* document, const std::shared_ptr<NormalDocument>& indexDoc);

    std::shared_ptr<NormalDocument> CreateDocument(const NormalExtendDocument* document);

    bool Validate(const NormalExtendDocument* document);

protected:
    std::shared_ptr<config::TabletSchema> _schema;
    std::vector<std::shared_ptr<config::AttributeConfig>> _fieldIdToAttrConfigs;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;

private:
    std::shared_ptr<ExtendDocFieldsConvertor> _fieldConvertPtr;
    std::shared_ptr<SectionAttributeAppender> _sectionAttrAppender;
    std::shared_ptr<PackAttributeAppender> _packAttrAppender;
    std::shared_ptr<NullFieldAppender> _nullFieldAppender;
    bool _hasPrimaryKey = false;
    fieldid_t _primaryKeyFieldId = INVALID_FIELDID;
    std::shared_ptr<indexlib::util::AccumulativeCounter> _attributeConvertErrorCounter;
    std::set<fieldid_t> _summaryFieldIds;
    std::set<fieldid_t> _attributeFieldIds;
    std::set<fieldid_t> _invertedFieldIds;
    std::string _orderPreservingField;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
