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
#include "indexlib/test/ExtendDocField2IndexDocField.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/test/document_parser.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, ExtendDocField2IndexDocField);

ExtendDocField2IndexDocField::ExtendDocField2IndexDocField(const IndexPartitionSchemaPtr& schema, regionid_t regionId)
    : mSchema(schema)
    , mRegionId(regionId)
    , mIndexFieldConvertor(schema, regionId)
{
    init();
}

ExtendDocField2IndexDocField::~ExtendDocField2IndexDocField() {}

void ExtendDocField2IndexDocField::init() { initAttrConvert(); }

void ExtendDocField2IndexDocField::convertAttributeField(const NormalDocumentPtr& document,
                                                         const FieldConfigPtr& fieldConfig,
                                                         const RawDocumentPtr& rawDoc)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);

    fieldid_t fieldId = fieldConfig->GetFieldId();
    if ((size_t)fieldId >= mAttrConvertVec.size()) {
        IE_LOG(ERROR, "field config error: fieldName[%s], fieldId[%d]", fieldConfig->GetFieldName().c_str(), fieldId);
        return;
    }
    const auto& convertor = mAttrConvertVec[fieldId];
    assert(convertor);
    const string& fieldValue = rawDoc->GetField(fieldConfig->GetFieldName());
    if (fieldValue.empty() && doc->GetDocOperateType() == UPDATE_FIELD) {
        return;
    }

    string parsedFieldValue = fieldValue;
    if (fieldConfig->IsMultiValue()) {
        bool replaceSep = (fieldConfig->GetFieldType() != ft_location && fieldConfig->GetFieldType() != ft_line &&
                           fieldConfig->GetFieldType() != ft_polygon);

        parsedFieldValue = DocumentParser::ParseMultiValueField(fieldValue, replaceSep);
    }

    AttributeDocumentPtr attributeDoc = doc->GetAttributeDocument();
    if (fieldConfig->IsEnableNullField() && parsedFieldValue == fieldConfig->GetNullFieldLiteralString()) {
        attributeDoc->SetNullField(fieldId);
        return;
    }
    StringView convertedValue = convertor->Encode(StringView(parsedFieldValue), doc->GetPool());
    attributeDoc->SetField(fieldId, convertedValue);
}

void ExtendDocField2IndexDocField::convertIndexField(const NormalDocumentPtr& document,
                                                     const FieldConfigPtr& fieldConfig, const RawDocumentPtr& rawDoc)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    string fieldValue;
    if (!rawDoc->Exist(fieldConfig->GetFieldName())) {
        if (fieldConfig->IsEnableNullField()) {
            fieldValue = fieldConfig->GetNullFieldLiteralString();
        } else {
            return;
        }
    } else {
        fieldValue = rawDoc->GetField(fieldConfig->GetFieldName());
    }

    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    // TODO: use fieldConfig->GetFieldTag()
    convertIndexField(indexDoc, fieldConfig, fieldValue, DocumentParser::DP_TOKEN_SEPARATOR, doc->GetPool());
}

void ExtendDocField2IndexDocField::convertIndexField(const IndexDocumentPtr& indexDoc,
                                                     const FieldConfigPtr& fieldConfig, const string& fieldValue,
                                                     const string& fieldSep, Pool* pool)
{
    mIndexFieldConvertor.convertIndexField(indexDoc, fieldConfig, fieldValue, fieldSep, pool);
}

void ExtendDocField2IndexDocField::convertModifyFields(const NormalDocumentPtr& document, const string& modifyFields)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    vector<string> fieldNames;
    StringUtil::fromString(modifyFields, fieldNames, MODIFY_FIELDS_SEP);
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    FieldSchemaPtr subFieldSchema;
    if (subSchema) {
        subFieldSchema = subSchema->GetFieldSchema();
    }
    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    for (size_t i = 0; i < fieldNames.size(); i++) {
        const string& fieldName = fieldNames[i];
        if (fieldSchema->IsFieldNameInSchema(fieldName)) {
            fieldid_t fid = fieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            doc->AddModifiedField(fid);
        } else if (subFieldSchema) {
            fieldid_t fid = subFieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            doc->AddSubModifiedField(fid);
        }
    }
}

void ExtendDocField2IndexDocField::convertSummaryField(const SummaryDocumentPtr& doc, const FieldConfigPtr& fieldConfig,
                                                       const RawDocumentPtr& rawDoc)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const string& fieldValue = rawDoc->GetField(fieldConfig->GetFieldName());
    string parsedFieldValue = fieldValue;
    if (fieldConfig->IsMultiValue()) {
        bool replaceSep = (fieldConfig->GetFieldType() != ft_location && fieldConfig->GetFieldType() != ft_line &&
                           fieldConfig->GetFieldType() != ft_line);
        parsedFieldValue = DocumentParser::ParseMultiValueField(fieldValue, replaceSep);
    }

    if (rawDoc->Exist(fieldConfig->GetFieldName())) {
        rawDoc->SetField(fieldConfig->GetFieldName(), parsedFieldValue);
    }
    const string& newFieldValue = rawDoc->GetField(fieldConfig->GetFieldName());
    doc->SetField(fieldId, StringView(newFieldValue));
}

void ExtendDocField2IndexDocField::initAttrConvert()
{
    const FieldSchemaPtr& fieldSchemaPtr = mSchema->GetFieldSchema(mRegionId);
    if (!fieldSchemaPtr) {
        return;
    }
    mAttrConvertVec.resize(fieldSchemaPtr->GetFieldCount());
    const AttributeSchemaPtr& attrSchemaPtr = mSchema->GetAttributeSchema(mRegionId);
    if (!attrSchemaPtr) {
        return;
    }
    for (AttributeSchema::Iterator it = attrSchemaPtr->Begin(); it != attrSchemaPtr->End(); ++it) {
        const auto& attrConfig = *it;
        mAttrConvertVec[attrConfig->GetFieldId()].reset(
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    }
}

// string ExtendDocField2IndexDocField::transIndexTokenizeFieldToSummaryStr(
//         const IndexTokenizeFieldPtr &tokenizeField)
// {
//     string summaryStr;
//     if (!tokenizeField.get()) {
//         return summaryStr;
//     }

//     IndexTokenizeField::Iterator it = tokenizeField->createIterator();
//     while (!it.isEnd()) {
//         if ((*it) == NULL) {
//             it.next();
//             continue;
//         }
//         TokenizeSection::Iterator tokenIter = (*it)->createIterator();
//         while (tokenIter) {
//             if (!summaryStr.empty()) {
//                 summaryStr.append("\t");
//             }
//             const ShortString &text = (*tokenIter)->getText();
//             summaryStr.append(text.begin(), text.end());
//             tokenIter.nextBasic();
//         }
//         it.next();
//     }
//     return summaryStr;
// }
}} // namespace indexlib::test
