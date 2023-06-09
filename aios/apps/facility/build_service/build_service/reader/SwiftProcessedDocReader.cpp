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
#include "build_service/reader/SwiftProcessedDocReader.h"

#include "build_service/config/CLIOptionNames.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SwiftProcessedDocReader);

SwiftProcessedDocReader::SwiftProcessedDocReader(const util::SwiftClientCreatorPtr& swiftClientCreator)
    : SwiftRawDocumentReader(swiftClientCreator)
    , _printToken(false)
    , _printAttribute(false)
    , _printSummary(false)
    , _printKVValue(false)
{
}

SwiftProcessedDocReader::~SwiftProcessedDocReader() {}

bool SwiftProcessedDocReader::init(const ReaderInitParam& params)
{
    if (!SwiftRawDocumentReader::init(params)) {
        return false;
    }

    string tableName;
    auto it = params.kvMap.find(RAW_DOCUMENT_SCHEMA_NAME);
    if (it != params.kvMap.end()) {
        tableName = it->second;
    }

    it = params.kvMap.find("print_token");
    if (it != params.kvMap.end()) {
        _printToken = (it->second == "true");
    }

    it = params.kvMap.find("print_attribute");
    if (it != params.kvMap.end()) {
        _printAttribute = (it->second == "true");
    }

    it = params.kvMap.find("print_summary");
    if (it != params.kvMap.end()) {
        _printSummary = (it->second == "true");
    }

    it = params.kvMap.find("print_kvvalue");
    if (it != params.kvMap.end()) {
        _printKVValue = (it->second == "true");
    }

    IndexPartitionSchemaPtr schema;
    if (!tableName.empty() && params.resourceReader) {
        schema = params.resourceReader->getSchemaBySchemaTableName(tableName);
    }
    if (!schema) {
        schema = params.schema;
    }
    if (!schema) {
        BS_LOG(ERROR, "no valid schema.");
        return false;
    }

    _schema = schema;
    initAttributeExtractResource(_schema);
    BS_LOG(INFO,
           "SwiftProcessedDocReader init DocumentFactoryWrapper "
           "by using schema_name [%s]",
           _schema->GetSchemaName().c_str());
    string pluginPath = "";
    if (params.resourceReader) {
        pluginPath = params.resourceReader->getPluginPath();
    }

    _docFactoryWrapper =
        DocumentFactoryWrapper::CreateDocumentFactoryWrapper(_schema, CUSTOMIZED_DOCUMENT_CONFIG_PARSER, pluginPath);
    if (!_docFactoryWrapper) {
        return false;
    }
    CustomizedConfigPtr docParserConfig = CustomizedConfigHelper::FindCustomizedConfig(
        _schema->GetCustomizedDocumentConfigs(), CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
    BuiltInParserInitResource resource;
    resource.counterMap = params.counterMap;
    resource.metricProvider = params.metricProvider;
    DocumentInitParamPtr initParam;
    if (docParserConfig) {
        initParam.reset(new BuiltInParserInitParam(docParserConfig->GetParameters(), resource));
    } else {
        DocumentInitParam::KeyValueMap kvMap;
        initParam.reset(new BuiltInParserInitParam(kvMap, resource));
    }
    _docParser.reset(_docFactoryWrapper->CreateDocumentParser(initParam));
    return _docParser.get() != nullptr;
}

void SwiftProcessedDocReader::initAttributeExtractResource(const indexlib::config::IndexPartitionSchemaPtr& schema)
{
    _schema = schema;
    auto tableType = schema->GetTableType();
    if (tableType == indexlib::tt_kv || tableType == indexlib::tt_kkv) {
        KVIndexConfigPtr indexConf =
            DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        assert(indexConf);
        PackAttributeConfigPtr packConfig = indexConf->GetValueConfig()->CreatePackAttributeConfig();
        PackAttributeFormatterPtr packFormatter(new PackAttributeFormatter);
        packFormatter->Init(packConfig);
        _packFormatters.push_back(packFormatter);
        return;
    }

    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); i++) {
            PackAttributeConfigPtr packConfig = attrSchema->GetPackAttributeConfig(i);
            PackAttributeFormatterPtr packFormatter(new PackAttributeFormatter);
            packFormatter->Init(packConfig);
            _packFormatters.push_back(packFormatter);
        }

        _attrConvertors.resize(schema->GetFieldCount());
        for (AttributeSchema::Iterator it = attrSchema->Begin(); it != attrSchema->End(); ++it) {
            const auto& attrConfig = *it;
            _attrConvertors[attrConfig->GetFieldId()].reset(
                AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, tableType));
        }
    }
}

void SwiftProcessedDocReader::ExtractDocMeta(document::RawDocument& rawDoc, const indexlib::document::DocumentPtr& doc)
{
    rawDoc.setField("__META__:timestamp", autil::StringUtil::toString(doc->GetTimestamp()));
    rawDoc.setField("__META__:CMD", DefaultRawDocument::getCmdString(doc->GetDocOperateType()));
    rawDoc.setField("__META__:locator", autil::StringUtil::strToHexStr(doc->GetLocator().ToString()));
    rawDoc.setField("__META__:ttl", autil::StringUtil::toString(doc->GetTTL()));
    rawDoc.setField("__META__:trace", autil::StringUtil::toString(doc->NeedTrace()));
    rawDoc.setField("__META__:serializeVersion", autil::StringUtil::toString(doc->GetSerializedVersion()));

    const auto& tagMap = doc->GetTagInfoMap();
    for (const auto& [key, value] : tagMap) {
        rawDoc.setField(string("__TAG__:") + key, value);
    }
}

void SwiftProcessedDocReader::ExtractRawDocument(document::RawDocument& rawDoc,
                                                 const indexlib::document::RawDocumentPtr& rDoc)
{
    RawDocFieldIteratorPtr iter(rDoc->CreateIterator());
    while (iter && iter->IsValid()) {
        rawDoc.setField(iter->GetFieldName(), iter->GetFieldValue());
        iter->MoveToNext();
    }
}

void SwiftProcessedDocReader::ExtractIndexDocument(document::RawDocument& rawDoc,
                                                   const indexlib::document::IndexDocumentPtr& indexDoc)
{
    rawDoc.setField("__primaryKey__", indexDoc->GetPrimaryKey());
    auto iter = indexDoc->CreateIterator();
    uint32_t fieldCount = 0;
    while (iter.HasNext()) {
        Field* field = iter.Next();
        if (!field) {
            continue;
        }
        string value;
        switch (field->GetFieldTag()) {
        case Field::FieldTag::TOKEN_FIELD: {
            value = "tokenize";
            IndexTokenizeField* tokenField = dynamic_cast<IndexTokenizeField*>(field);
            for (auto secIter = tokenField->Begin(); secIter != tokenField->End(); secIter++) {
                Section* section = *secIter;
                if (section == NULL) {
                    continue;
                }
                if (_printToken) {
                    value += " section[";
                    for (size_t i = 0; i < section->GetTokenCount(); i++) {
                        Token* token = section->GetToken(i);
                        value += autil::StringUtil::toString(token->GetHashKey());
                        if (i != section->GetTokenCount() - 1) {
                            value += ",";
                        }
                    }
                    value += "]";
                } else {
                    value += " section(";
                    value += autil::StringUtil::toString(section->GetTokenCount());
                    value += ")";
                }
            }
            break;
        }
        case Field::FieldTag::RAW_FIELD: {
            value = "raw";
            break;
        }
        case Field::FieldTag::NULL_FIELD: {
            value = "null";
            break;
        }
        default:
            value = "unknown";
        }
        fieldCount++;
        fieldid_t fid = field->GetFieldId();
        FieldConfigPtr fieldConfig = _schema->GetFieldConfig(fid);
        if (fieldConfig) {
            rawDoc.setField(string("__indexDoc:") + fieldConfig->GetFieldName(), value);
        } else {
            rawDoc.setField(string("__indexDoc:fid_") + autil::StringUtil::toString(fid), value);
        }
    }
    rawDoc.setField(string("__META__:indexFieldCount"), autil::StringUtil::toString(fieldCount));
}

void SwiftProcessedDocReader::ExtractAttributeDocument(document::RawDocument& rawDoc,
                                                       const indexlib::document::AttributeDocumentPtr& attrDoc,
                                                       autil::mem_pool::Pool* pool)
{
    rawDoc.setField(string("__META__:attrFieldCount"), autil::StringUtil::toString(attrDoc->GetNotEmptyFieldCount()));
    rawDoc.setField(string("__META__:packAttrFieldCount"), autil::StringUtil::toString(attrDoc->GetPackFieldCount()));

    AttributeSchemaPtr attrSchema = _schema->GetAttributeSchema();
    if (!attrSchema || !_printAttribute) {
        return;
    }

    auto iter = attrDoc->CreateIterator();
    while (iter.HasNext()) {
        fieldid_t fieldId;
        autil::StringView value = iter.Next(fieldId);
        if ((size_t)fieldId >= _attrConvertors.size()) {
            rawDoc.setField(string("__attrDoc__:fid_") + autil::StringUtil::toString(fieldId) + "_not_exist",
                            autil::StringUtil::strToHexStr(string(value.data(), value.size())));
            continue;
        }
        auto attrConvertor = _attrConvertors[fieldId];
        if (!attrConvertor) {
            rawDoc.setField(string("__attrDoc__:fid_") + autil::StringUtil::toString(fieldId) + "_not_exist",
                            autil::StringUtil::strToHexStr(string(value.data(), value.size())));
            continue;
        }
        string literalValue;
        if (!attrConvertor->DecodeLiteralField(value, literalValue)) {
            rawDoc.setField(string("__attrDoc__:fid_") + autil::StringUtil::toString(fieldId) + "_extract_fail",
                            autil::StringUtil::strToHexStr(string(value.data(), value.size())));
            continue;
        }
        rawDoc.setField(string("__attrDoc:") + attrConvertor->GetFieldName(), literalValue);
    }
    for (size_t i = 0; i < attrDoc->GetPackFieldCount(); i++) {
        autil::StringView packField = attrDoc->GetPackField(i);
        if (packField.empty()) {
            continue;
        }
        if (i >= _packFormatters.size()) {
            continue;
        }
        PackAttributeFormatterPtr formatter = _packFormatters[i];
        PackAttributeConfigPtr packConfig = formatter->GetPackAttributeConfig();
        vector<AttributeConfigPtr> attrConfigs = packConfig->GetAttributeConfigVec();
        for (size_t i = 0; i < attrConfigs.size(); i++) {
            string key = "__packAttrDoc:" + attrConfigs[i]->GetAttrName();
            string value = formatter->GetFieldStringValueFromPackedValue(packField, attrConfigs[i]->GetAttrId(), pool);
            rawDoc.setField(key, value);
        }
    }
}

void SwiftProcessedDocReader::ExtractSummaryDocument(document::RawDocument& rawDoc,
                                                     const indexlib::document::SerializedSummaryDocumentPtr& doc,
                                                     autil::mem_pool::Pool* pool)
{
    SummarySchemaPtr summarySchema = _schema->GetSummarySchema();
    if (!summarySchema || !_printSummary) {
        return;
    }

    SearchSummaryDocument searchDoc(pool, summarySchema->GetSummaryCount());
    SummaryFormatter formatter(summarySchema);
    formatter.DeserializeSummaryDoc(doc, &searchDoc);
    auto fields = searchDoc.GetFields();
    for (auto iter = summarySchema->Begin(); iter != summarySchema->End(); iter++) {
        auto summaryConfig = *iter;
        if (!summaryConfig) {
            continue;
        }

        auto id = summarySchema->GetSummaryFieldId(summaryConfig->GetFieldId());
        if (id == INVALID_SUMMARYFIELDID) {
            continue;
        }

        if (fields[id] == nullptr) {
            continue;
        }
        rawDoc.setField(string("__summaryDoc:") + summaryConfig->GetSummaryName(),
                        string(fields[id]->data(), fields[id]->size()));
    }
}

void SwiftProcessedDocReader::ExtractSourceDocument(document::RawDocument& rawDoc,
                                                    const indexlib::document::SerializedSourceDocumentPtr& sourceDoc,
                                                    autil::mem_pool::Pool* pool)
{
    SourceDocumentFormatter formatter;
    formatter.Init(_schema->GetSourceSchema());
    SourceDocument srcDoc(pool);
    formatter.DeserializeSourceDocument(sourceDoc, &srcDoc);
    srcDoc.ToRawDocument(rawDoc);
}

void SwiftProcessedDocReader::ExtractIndexNormalDocument(document::RawDocument& rawDoc,
                                                         const indexlib::document::NormalDocumentPtr& normalDoc)
{
    rawDoc.setField("__META__:schemaId", autil::StringUtil::toString(normalDoc->GetSchemaVersionId()));
    rawDoc.setField("__META__:regionId", autil::StringUtil::toString(normalDoc->GetRegionId()));
    rawDoc.setField("__META__:userTimestamp", autil::StringUtil::toString(normalDoc->GetUserTimestamp()));

    auto indexDoc = normalDoc->GetIndexDocument();
    if (indexDoc) {
        ExtractIndexDocument(rawDoc, indexDoc);
    }

    auto sourceDoc = normalDoc->GetSourceDocument();
    if (sourceDoc) {
        ExtractSourceDocument(rawDoc, sourceDoc, normalDoc->GetPool());
    }

    auto attrDoc = normalDoc->GetAttributeDocument();
    if (attrDoc) {
        ExtractAttributeDocument(rawDoc, attrDoc, normalDoc->GetPool());
    }

    auto summaryDoc = normalDoc->GetSummaryDocument();
    if (summaryDoc) {
        ExtractSummaryDocument(rawDoc, summaryDoc, normalDoc->GetPool());
    }
}

void SwiftProcessedDocReader::ExtractIndexKVDocument(document::RawDocument& rawDoc,
                                                     const indexlib::document::KVDocumentPtr& kvDoc)
{
    assert(kvDoc->GetDocumentCount() == 1u);
    const auto& kvIndexDoc = kvDoc->back();
    rawDoc.setField("__META__:regionId", autil::StringUtil::toString(kvIndexDoc.GetRegionId()));
    rawDoc.setField("__META__:userTimestamp", autil::StringUtil::toString(kvIndexDoc.GetUserTimestamp()));
    rawDoc.setField("__primaryKey__", autil::StringUtil::toString(kvIndexDoc.GetPkFieldValue()));

    rawDoc.setField("__pkeyHash__", autil::StringUtil::toString(kvIndexDoc.GetPKeyHash()));
    if (_printKVValue && !kvIndexDoc.GetValue().empty()) {
        assert(_packFormatters.size() == 1);
        PackAttributeFormatterPtr formatter = _packFormatters[0];
        PackAttributeConfigPtr packConfig = formatter->GetPackAttributeConfig();
        vector<AttributeConfigPtr> attrConfigs = packConfig->GetAttributeConfigVec();
        for (size_t i = 0; i < attrConfigs.size(); i++) {
            string key = "__packAttrDoc:" + attrConfigs[i]->GetAttrName();
            string value = formatter->GetFieldStringValueFromPackedValue(kvIndexDoc.GetValue(),
                                                                         attrConfigs[i]->GetAttrId(), kvDoc->getPool());
            rawDoc.setField(key, value);
        }
    } else {
        rawDoc.setField(autil::StringView("__value__"), kvIndexDoc.GetValue());
    }
    if (kvIndexDoc.HasSKey()) {
        rawDoc.setField("__skeyHash__", autil::StringUtil::toString(kvIndexDoc.GetSKeyHash()));
    }
}

RawDocumentReader::ErrorCode SwiftProcessedDocReader::getNextRawDoc(document::RawDocument& rawDoc,
                                                                    Checkpoint* checkpoint, DocInfo& docInfo)
{
    string docStr;
    ErrorCode ec = readDocStr(docStr, checkpoint, docInfo);
    if (ec != ERROR_NONE) {
        return ec;
    }

    assert(_docParser);
    DocumentPtr doc = _docParser->Parse(StringView(docStr));
    if (!doc) {
        rawDoc.setField("__ERROR__", "parse processed document fail!");
        BS_LOG(ERROR, "parse processed document fail!");
        return ec;
    }

    ExtractFieldInfos(rawDoc, doc);
    return ec;
}

void SwiftProcessedDocReader::ExtractFieldInfos(document::RawDocument& rawDoc,
                                                const indexlib::document::DocumentPtr& doc)
{
    ExtractDocMeta(rawDoc, doc);
    RawDocumentPtr rDoc = DYNAMIC_POINTER_CAST(RawDocument, doc);
    if (rDoc) {
        ExtractRawDocument(rawDoc, rDoc);
    }

    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    if (normalDoc) {
        ExtractIndexNormalDocument(rawDoc, normalDoc);
    }

    KVDocumentPtr kvDoc = DYNAMIC_POINTER_CAST(KVDocument, doc);
    if (kvDoc) {
        ExtractIndexKVDocument(rawDoc, kvDoc);
    }
}

}} // namespace build_service::reader
