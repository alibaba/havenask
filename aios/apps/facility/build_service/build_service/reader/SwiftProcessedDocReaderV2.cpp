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
#include "build_service/reader/SwiftProcessedDocReaderV2.h"

#include "build_service/config/CLIOptionNames.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SwiftProcessedDocReaderV2);

SwiftProcessedDocReaderV2::SwiftProcessedDocReaderV2(const util::SwiftClientCreatorPtr& swiftClientCreator)
    : SwiftRawDocumentReader(swiftClientCreator)
    , _printToken(false)
    , _printAttribute(false)
    , _printSummary(false)
    , _printKVValue(false)
{
}

SwiftProcessedDocReaderV2::~SwiftProcessedDocReaderV2() {}

bool SwiftProcessedDocReaderV2::init(const ReaderInitParam& params)
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

    std::shared_ptr<indexlibv2::config::TabletSchema> schema;
    if (!tableName.empty() && params.resourceReader) {
        schema = params.resourceReader->getTabletSchemaBySchemaTableName(tableName);
    }
    if (!schema) {
        schema = params.schemaV2;
    }
    if (!schema) {
        BS_LOG(ERROR, "no valid schema.");
        return false;
    }
    _schema = schema;

    if (!initAttributeExtractResource(_schema)) {
        BS_LOG(ERROR, "init attribute extract resource failed");
        return false;
    }
    BS_LOG(INFO,
           "SwiftProcessedDocReaderV2 init DocumentFactory "
           "by using table_name [%s]",
           _schema->GetTableName().c_str());

    auto documentFactory = createDocumentFactory(_schema);
    if (!documentFactory) {
        BS_LOG(ERROR, "create document factory failed");
        return false;
    }

    indexlibv2::document::BuiltInParserInitResource resource;
    resource.counterMap = params.counterMap;
    resource.metricProvider = params.metricProvider;
    std::shared_ptr<indexlibv2::document::DocumentInitParam> initParam;
    indexlibv2::document::DocumentInitParam::KeyValueMap kvMap;
    initParam.reset(new indexlibv2::document::BuiltInParserInitParam(kvMap, resource));
    _docParser = documentFactory->CreateDocumentParser(_schema, initParam);
    if (!_docParser) {
        BS_LOG(ERROR, "create document parser failed");
        return false;
    }
    return true;
}

std::unique_ptr<indexlibv2::document::IDocumentFactory>
SwiftProcessedDocReaderV2::createDocumentFactory(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema) const
{
    if (!schema) {
        BS_LOG(ERROR, "tablet schema is nullptr");
        return nullptr;
    }
    const auto& tableType = schema->GetTableType();
    auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        BS_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
               autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType()).c_str());
        return nullptr;
    }
    auto options = std::make_shared<indexlibv2::config::TabletOptions>();
    if (!tabletFactory->Init(options, nullptr)) {
        BS_LOG(ERROR, "init tablet factory with type [%s] failed", tableType.c_str());
        return nullptr;
    }
    return tabletFactory->CreateDocumentFactory(schema);
}

bool SwiftProcessedDocReaderV2::initAttributeExtractResource(
    const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
{
    _schema = schema;
    auto tableType = schema->GetTableType();
    if (tableType == indexlib::table::TABLE_TYPE_KV || tableType == indexlib::table::TABLE_TYPE_KKV) {
        const auto& indexConf =
            std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(schema->GetPrimaryKeyIndexConfig());
        assert(indexConf);
        auto [status, packConfig] = indexConf->GetValueConfig()->CreatePackAttributeConfig();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "create pack attribute config failed");
            return false;
        }
        auto packFormatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
        packFormatter->Init(packConfig);
        _packFormatters.push_back(packFormatter);
        return true;
    }

    // TODO: uncomment when indexlibv2 support pack attribute
    // if (schema->GetLegacySchema() && schema->GetLegacySchema()->GetAttributeSchema()) {
    //     auto attrSchema = schema->GetLegacySchema()->GetAttributeSchema();
    //     for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); i++) {
    //         auto packConfig = attrSchema->GetPackAttributeConfig(i);
    //         PackAttributeFormatterPtr packFormatter(new PackAttributeFormatter);
    //         packFormatter->Init(packConfig);
    //         _packFormatters.push_back(packFormatter);
    //     }
    // }
    const auto& configs = schema->GetIndexConfigs(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR);
    if (!configs.empty()) {
        _attrConvertors.resize(schema->GetFieldCount());
        bool kvOrKKVTable =
            (tableType == indexlib::table::TABLE_TYPE_KV || tableType == indexlib::table::TABLE_TYPE_KKV);
        for (const auto& config : configs) {
            const auto& attrConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(config);
            assert(attrConfig);
            _attrConvertors[attrConfig->GetFieldId()].reset(
                indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig,
                                                                                                 kvOrKKVTable));
        }
    }
    return true;
}

void SwiftProcessedDocReaderV2::ExtractDocMeta(document::RawDocument& rawDoc,
                                               const std::shared_ptr<indexlibv2::document::IDocument>& doc)
{
    rawDoc.setField("__META__:timestamp", autil::StringUtil::toString(doc->GetDocInfo().timestamp));
    rawDoc.setField("__META__:CMD", indexlibv2::document::DefaultRawDocument::getCmdString(doc->GetDocOperateType()));
    rawDoc.setField("__META__:locator", autil::StringUtil::strToHexStr(doc->GetLocatorV2().Serialize()));
    rawDoc.setField("__META__:ttl", autil::StringUtil::toString(doc->GetTTL()));
    bool needTrace = !doc->GetTraceId().empty();
    rawDoc.setField("__META__:trace", autil::StringUtil::toString(needTrace));
}

void SwiftProcessedDocReaderV2::ExtractRawDocument(document::RawDocument& rawDoc,
                                                   const std::shared_ptr<indexlibv2::document::RawDocument>& rDoc)
{
    ExtractTagInfoMap(rawDoc, rDoc->GetTagInfoMap());

    std::shared_ptr<indexlibv2::document::RawDocFieldIterator> iter(rDoc->CreateIterator());
    while (iter && iter->IsValid()) {
        rawDoc.setField(iter->GetFieldName(), iter->GetFieldValue());
        iter->MoveToNext();
    }
}

void SwiftProcessedDocReaderV2::ExtractIndexDocument(document::RawDocument& rawDoc,
                                                     const std::shared_ptr<indexlib::document::IndexDocument>& indexDoc)
{
    rawDoc.setField("__primaryKey__", indexDoc->GetPrimaryKey());
    auto iter = indexDoc->CreateIterator();
    uint32_t fieldCount = 0;
    while (iter.HasNext()) {
        auto field = iter.Next();
        if (!field) {
            continue;
        }
        string value;
        switch (field->GetFieldTag()) {
        case indexlib::document::Field::FieldTag::TOKEN_FIELD: {
            value = "tokenize";
            auto tokenField = dynamic_cast<indexlib::document::IndexTokenizeField*>(field);
            for (auto secIter = tokenField->Begin(); secIter != tokenField->End(); secIter++) {
                auto section = *secIter;
                if (section == NULL) {
                    continue;
                }
                if (_printToken) {
                    value += " section[";
                    for (size_t i = 0; i < section->GetTokenCount(); i++) {
                        auto token = section->GetToken(i);
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
        case indexlib::document::Field::FieldTag::RAW_FIELD: {
            value = "raw";
            break;
        }
        case indexlib::document::Field::FieldTag::NULL_FIELD: {
            value = "null";
            break;
        }
        default:
            value = "unknown";
        }
        fieldCount++;
        fieldid_t fid = field->GetFieldId();
        auto fieldConfig = _schema->GetFieldConfig(fid);
        if (fieldConfig) {
            rawDoc.setField(string("__indexDoc:") + fieldConfig->GetFieldName(), value);
        } else {
            rawDoc.setField(string("__indexDoc:fid_") + autil::StringUtil::toString(fid), value);
        }
    }
    rawDoc.setField(string("__META__:indexFieldCount"), autil::StringUtil::toString(fieldCount));
}

void SwiftProcessedDocReaderV2::ExtractAttributeDocument(
    document::RawDocument& rawDoc, const std::shared_ptr<indexlib::document::AttributeDocument>& attrDoc,
    autil::mem_pool::Pool* pool)
{
    rawDoc.setField(string("__META__:attrFieldCount"), autil::StringUtil::toString(attrDoc->GetNotEmptyFieldCount()));
    rawDoc.setField(string("__META__:packAttrFieldCount"), autil::StringUtil::toString(attrDoc->GetPackFieldCount()));

    if (!_printAttribute) {
        return;
    }

    auto iter = attrDoc->CreateIterator();
    while (!_attrConvertors.empty() && iter.HasNext()) {
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
        auto formatter = _packFormatters[i];
        auto packConfig = formatter->GetPackAttributeConfig();
        auto attrConfigs = packConfig->GetAttributeConfigVec();
        for (size_t i = 0; i < attrConfigs.size(); i++) {
            string key = "__packAttrDoc:" + attrConfigs[i]->GetAttrName();
            string value = formatter->GetFieldStringValueFromPackedValue(packField, attrConfigs[i]->GetAttrId(), pool);
            rawDoc.setField(key, value);
        }
    }
}

void SwiftProcessedDocReaderV2::ExtractSummaryDocument(
    document::RawDocument& rawDoc, const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& doc,
    autil::mem_pool::Pool* pool)
{
    if (!_printSummary) {
        return;
    }
    const auto& config =
        _schema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return;
    }
    const auto& summaryIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(config);
    assert(summaryIndexConfig);

    indexlib::document::SearchSummaryDocument searchDoc(pool, summaryIndexConfig->GetSummaryCount());
    indexlibv2::document::SummaryFormatter formatter(summaryIndexConfig);
    formatter.DeserializeSummaryDoc(doc, &searchDoc);
    auto fields = searchDoc.GetFields();
    for (auto iter = summaryIndexConfig->begin(); iter != summaryIndexConfig->end(); iter++) {
        auto summaryConfig = *iter;
        if (!summaryConfig) {
            continue;
        }

        auto id = summaryIndexConfig->GetSummaryFieldId(summaryConfig->GetFieldId());
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

void SwiftProcessedDocReaderV2::ExtractSourceDocument(document::RawDocument& rawDoc,
                                                      const indexlib::document::SerializedSourceDocumentPtr& sourceDoc,
                                                      autil::mem_pool::Pool* pool)
{
    // indexlibv2 not support source index yet
    if (!_schema->GetLegacySchema()) {
        return;
    }
    indexlib::document::SourceDocumentFormatter formatter;
    formatter.Init(_schema->GetLegacySchema()->GetSourceSchema());
    indexlib::document::SourceDocument srcDoc(pool);
    formatter.DeserializeSourceDocument(sourceDoc, &srcDoc);
    srcDoc.ToRawDocument(rawDoc);
}

void SwiftProcessedDocReaderV2::ExtractIndexNormalDocument(
    document::RawDocument& rawDoc, const std::shared_ptr<indexlibv2::document::NormalDocument>& normalDoc)
{
    rawDoc.setField("__META__:serializeVersion", autil::StringUtil::toString(normalDoc->GetSerializedVersion()));
    rawDoc.setField("__META__:userTimestamp", autil::StringUtil::toString(normalDoc->GetUserTimestamp()));
    ExtractTagInfoMap(rawDoc, normalDoc->GetTagInfoMap());

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

void SwiftProcessedDocReaderV2::ExtractTagInfoMap(document::RawDocument& rawDoc,
                                                  const std::map<std::string, std::string>& tagInfoMap)
{
    for (const auto& [key, value] : tagInfoMap) {
        rawDoc.setField(string("__TAG__:") + key, value);
    }
}

void SwiftProcessedDocReaderV2::ExtractIndexKVDocument(
    document::RawDocument& rawDoc, const std::shared_ptr<indexlibv2::document::KVDocument>& kvIndexDoc)
{
    rawDoc.setField("__META__:userTimestamp", autil::StringUtil::toString(kvIndexDoc->GetUserTimestamp()));
    rawDoc.setField("__primaryKey__", autil::StringUtil::toString(kvIndexDoc->GetPkFieldValue()));

    rawDoc.setField("__pkeyHash__", autil::StringUtil::toString(kvIndexDoc->GetPKeyHash()));
    if (_printKVValue && !kvIndexDoc->GetValue().empty()) {
        assert(_packFormatters.size() == 1);
        auto formatter = _packFormatters[0];
        auto packConfig = formatter->GetPackAttributeConfig();
        auto attrConfigs = packConfig->GetAttributeConfigVec();
        for (size_t i = 0; i < attrConfigs.size(); i++) {
            string key = "__packAttrDoc:" + attrConfigs[i]->GetAttrName();
            string value = formatter->GetFieldStringValueFromPackedValue(
                kvIndexDoc->GetValue(), attrConfigs[i]->GetAttrId(), kvIndexDoc->GetPool());
            rawDoc.setField(key, value);
        }
    } else {
        rawDoc.setField(autil::StringView("__value__"), kvIndexDoc->GetValue());
    }
    if (kvIndexDoc->HasSKey()) {
        rawDoc.setField("__skeyHash__", autil::StringUtil::toString(kvIndexDoc->GetSKeyHash()));
    }
}

RawDocumentReader::ErrorCode SwiftProcessedDocReaderV2::getNextRawDoc(document::RawDocument& rawDoc,
                                                                      Checkpoint* checkpoint, DocInfo& docInfo)
{
    string docStr;
    ErrorCode ec = readDocStr(docStr, checkpoint, docInfo);
    if (ec != ERROR_NONE) {
        return ec;
    }

    assert(_docParser);
    auto [status, docBatch] = _docParser->Parse(StringView(docStr));
    if (!status.IsOK() || !docBatch) {
        rawDoc.setField("__ERROR__", "parse processed document fail!");
        BS_LOG(ERROR, "parse processed document fail!");
        return ec;
    }

    assert(docBatch->GetBatchSize() == 1);
    auto doc = (*docBatch)[0];
    ExtractFieldInfos(rawDoc, doc);
    return ec;
}

void SwiftProcessedDocReaderV2::ExtractFieldInfos(document::RawDocument& rawDoc,
                                                  const std::shared_ptr<indexlibv2::document::IDocument>& doc)
{
    ExtractDocMeta(rawDoc, doc);
    auto rDoc = std::dynamic_pointer_cast<indexlibv2::document::RawDocument>(doc);
    if (rDoc) {
        ExtractRawDocument(rawDoc, rDoc);
    }

    auto normalDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(doc);
    if (normalDoc) {
        ExtractIndexNormalDocument(rawDoc, normalDoc);
    }

    auto kvDoc = std::dynamic_pointer_cast<indexlibv2::document::KVDocument>(doc);
    if (kvDoc) {
        ExtractIndexKVDocument(rawDoc, kvDoc);
    }
}

}} // namespace build_service::reader
