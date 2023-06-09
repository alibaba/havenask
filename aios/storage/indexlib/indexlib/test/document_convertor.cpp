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
#include "indexlib/test/document_convertor.h"

#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocumentConvertor);

namespace {
string LAST_VALUE_PREFIX = "__last_value__";
}

IndexlibExtendDocumentPtr DocumentConvertor::CreateExtendDocFromRawDoc(const IndexPartitionSchemaPtr& schema,
                                                                       const RawDocumentPtr& rawDoc)
{
    document::IndexlibExtendDocumentPtr extDoc(new document::IndexlibExtendDocument);
    document::RawDocumentPtr newRawDoc(new document::DefaultRawDocument);
    newRawDoc->setDocOperateType(rawDoc->GetDocOperateType());
    newRawDoc->setDocTimestamp(rawDoc->GetTimestamp());
    indexlibv2::framework::Locator locator;
    locator.Deserialize(rawDoc->GetLocator().ToString());
    newRawDoc->SetLocator(locator);

    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        FieldConfigPtr fieldConfig = (*it);
        const string& fieldName = fieldConfig->GetFieldName();
        string value;
        if (rawDoc->GetDocOperateType() == ADD_DOC && !rawDoc->Exist(fieldName) && fieldConfig->IsEnableNullField()) {
            value = fieldConfig->GetNullFieldLiteralString();
        } else {
            value = rawDoc->GetField(fieldName);
        }

        if (value.empty()) {
            continue;
        }
        if (fieldConfig->IsMultiValue()) {
            FieldType ft = fieldConfig->GetFieldType();
            bool replaceSep = (ft != ft_location && ft != ft_line && ft != ft_polygon);
            string parsedFieldValue = DocumentParser::ParseMultiValueField(value, replaceSep);
            newRawDoc->setField(fieldName, parsedFieldValue);
        } else {
            newRawDoc->setField(fieldName, value);
        }
    }

    RawDocument::Iterator iter = rawDoc->Begin();
    for (; iter != rawDoc->End(); iter++) {
        string fieldName = iter->first;
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldName);
        if (!fieldConfig) {
            newRawDoc->setField(fieldName, iter->second);
            continue;
        }
    }

    ConvertModifyFields(schema, extDoc, rawDoc);
    extDoc->setRawDocument(newRawDoc);
    // prepare tokenized doc
    PrepareTokenizedDoc(schema, extDoc, rawDoc);
    return extDoc;
}

void DocumentConvertor::ConvertModifyFields(const IndexPartitionSchemaPtr& schema,
                                            const IndexlibExtendDocumentPtr& extDoc, const test::RawDocumentPtr& rawDoc)
{
    string modifyFields = rawDoc->GetField(RESERVED_MODIFY_FIELDS);
    if (modifyFields.empty()) {
        return;
    }
    vector<string> fieldNames;
    StringUtil::fromString(modifyFields, fieldNames, MODIFY_FIELDS_SEP);

    vector<string> fieldValues;
    string modifyValues = rawDoc->GetField(RESERVED_MODIFY_VALUES);
    StringUtil::fromString(modifyValues, fieldValues, MODIFY_FIELDS_SEP);

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    FieldSchemaPtr subFieldSchema;
    if (subSchema) {
        subFieldSchema = subSchema->GetFieldSchema();
    }
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    for (size_t i = 0; i < fieldNames.size(); i++) {
        const string& fieldName = fieldNames[i];
        if (fieldSchema->IsFieldNameInSchema(fieldName)) {
            fieldid_t fid = fieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            extDoc->addModifiedField(fid);
            if (!fieldValues.empty()) {
                rawDoc->SetField(LAST_VALUE_PREFIX + fieldName, fieldValues[i]);
            }
        }
        if (subFieldSchema and subFieldSchema->IsFieldNameInSchema(fieldName)) {
            fieldid_t fid = subFieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            extDoc->addSubModifiedField(fid);
        }
    }
}

void DocumentConvertor::PrepareTokenizedDoc(const IndexPartitionSchemaPtr& schema,
                                            const IndexlibExtendDocumentPtr& extDoc, const RawDocumentPtr& rawDoc)
{
    TokenizeDocumentPtr tokenDoc = extDoc->getTokenizeDocument();
    TokenizeDocumentPtr lastTokenDoc = extDoc->getLastTokenizeDocument();
    const FieldSchemaPtr& fieldSchema = schema->GetFieldSchema();
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        FieldConfigPtr fieldConfig = *it;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        FieldType fieldType = fieldConfig->GetFieldType();
        if (fieldType == ft_raw) {
            continue;
        }
        bool canSkipFieldInInvertedIndex = ((indexSchema == nullptr) || !indexSchema->IsInIndex(fieldId));
        bool canSkipFieldInSummary = (summarySchema == nullptr || !summarySchema->IsInSummary(fieldId));
        if (canSkipFieldInInvertedIndex && canSkipFieldInSummary) {
            continue;
        }
        string fieldName = fieldConfig->GetFieldName();
        PrepareTokenizedField(fieldName, fieldConfig, indexSchema, rawDoc, tokenDoc);
        PrepareTokenizedField(LAST_VALUE_PREFIX + fieldName, fieldConfig, indexSchema, rawDoc, lastTokenDoc);
    }
}

void DocumentConvertor::PrepareTokenizedField(const std::string& fieldName, const FieldConfigPtr& fieldConfig,
                                              const IndexSchemaPtr& indexSchema, const RawDocumentPtr& rawDoc,
                                              const TokenizeDocumentPtr& tokenDoc)
{
    string fieldValue;
    if (!rawDoc->Exist(fieldName) && fieldConfig->IsEnableNullField()) {
        fieldValue = fieldConfig->GetNullFieldLiteralString();
    } else {
        fieldValue = rawDoc->GetField(fieldName);
    }
    fieldid_t fieldId = fieldConfig->GetFieldId();
    auto fieldType = fieldConfig->GetFieldType();
    if (fieldConfig->IsEnableNullField() && fieldConfig->GetNullFieldLiteralString() == fieldValue) {
        const document::TokenizeFieldPtr& field = tokenDoc->createField(fieldId);
        field->setNull(true);
        return;
    }

    if (fieldValue.empty()) {
        return;
    }
    const document::TokenizeFieldPtr& field = tokenDoc->createField(fieldId);
    bool ret = false;
    if (ft_text == fieldType) {
        ret = TokenizeValue(field, fieldValue);
    } else if (ft_location == fieldType || ft_line == fieldType || ft_polygon == fieldType) {
        ret = TokenizeSingleValueField(field, fieldValue);
    } else if (fieldConfig->IsMultiValue() && indexSchema->IsInIndex(fieldId)) {
        ret = TokenizeValue(field, fieldValue);
    } else {
        ret = TokenizeSingleValueField(field, fieldValue);
    }
    if (!ret) {
        string errorMsg = "Failed to Tokenize field:[" + fieldName + "]";
        IE_LOG(WARN, "%s", errorMsg.c_str());
    }
}

bool DocumentConvertor::TokenizeValue(const document::TokenizeFieldPtr& field, const string& fieldValue,
                                      const string& sep)
{
    vector<string> tokenVec = StringUtil::split(fieldValue, sep);
    document::TokenizeSection* section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    document::TokenizeSection::Iterator iterator = section->createIterator();
    for (size_t i = 0; i < tokenVec.size(); i++) {
        document::AnalyzerToken token(tokenVec[i], i, tokenVec[i]);
        token.setIsStopWord(false);
        token.setIsSpace(false);
        token.setIsBasicRetrieve(true);
        token.setIsDelimiter(false);
        token.setIsRetrieve(true);
        token.setNeedSetText(true);

        section->insertBasicToken(token, iterator);
    }
    return true;
}

bool DocumentConvertor::TokenizeSingleValueField(const document::TokenizeFieldPtr& field, const string& fieldValue)
{
    document::TokenizeSection* section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    document::TokenizeSection::Iterator iterator = section->createIterator();

    document::AnalyzerToken token;
    token.setText(fieldValue);
    token.setNormalizedText(fieldValue);
    section->insertBasicToken(token, iterator);
    return true;
}

}} // namespace indexlib::test
