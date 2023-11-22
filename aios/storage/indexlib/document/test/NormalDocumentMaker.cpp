#include "indexlib/document/test/NormalDocumentMaker.h"

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/NormalDocumentFactory.h"
#include "indexlib/document/normal/NormalDocumentParser.h"
#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/document/normal/TokenizeDocumentConvertor.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

using namespace std;

namespace indexlibv2::document {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.document, NormalDocumentMaker);

std::vector<std::string> NormalDocumentMaker::_udfFieldNames = {};

shared_ptr<NormalDocument> NormalDocumentMaker::Make(const shared_ptr<config::ITabletSchema>& schema,
                                                     const string& docStr)
{
    shared_ptr<RawDocument> rawDoc = RawDocumentMaker::Make(docStr);
    if (!rawDoc) {
        AUTIL_LOG(ERROR, "make raw doc failed, [%s]", docStr.c_str());
        return nullptr;
    }
    rawDoc->setIgnoreEmptyField(true);
    return Make(schema, rawDoc);
}

shared_ptr<NormalDocument> NormalDocumentMaker::Make(const shared_ptr<config::ITabletSchema>& schema,
                                                     const shared_ptr<RawDocument>& rawDoc)
{
    auto factory = CreateDocumentFactory(schema->GetTableType());
    if (!factory) {
        AUTIL_LOG(ERROR, "create document factory failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    auto parser = CreateDocumentParser(schema);
    if (!parser) {
        AUTIL_LOG(ERROR, "create document parser failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    vector<string> multiFields = GetMultiField(schema);
    if (!multiFields.empty()) {
        ConvertMultiField(multiFields, rawDoc);
    }
    auto extendDoc = factory->CreateExtendDocument();
    assert(extendDoc);
    extendDoc->SetRawDocument(rawDoc);
    if (!InitExtendDoc(schema, extendDoc.get()).IsOK()) {
        AUTIL_LOG(ERROR, "init extend doc failed");
        return nullptr;
    }
    InitSourceDocument(schema, extendDoc.get());
    auto [s, docBatch] = parser->Parse(*extendDoc);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "parse failed, error: %s, raw doc: %s", s.ToString().c_str(), rawDoc->toString().c_str());
        return {};
    }
    if (!docBatch) {
        return nullptr;
    }
    if (docBatch->GetBatchSize() != 1) {
        AUTIL_LOG(ERROR, "expect batch size [1], real size [%lu]", docBatch->GetBatchSize());
        return nullptr;
    }
    auto doc = dynamic_pointer_cast<NormalDocument>((*docBatch)[0]);
    assert(doc);
    doc->SetLocator(rawDoc->getLocator());
    return doc;
}

shared_ptr<IDocumentBatch> NormalDocumentMaker::MakeBatch(const shared_ptr<config::ITabletSchema>& schema,
                                                          const string& docBatchStr)
{
    auto rawDocs = RawDocumentMaker::MakeBatch(docBatchStr);
    for (const auto& rawDoc : rawDocs) {
        assert(rawDoc);
        rawDoc->setIgnoreEmptyField(true);
    }
    return MakeBatch(schema, rawDocs);
}

shared_ptr<IDocumentBatch> NormalDocumentMaker::MakeBatch(const shared_ptr<config::ITabletSchema>& schema,
                                                          const vector<shared_ptr<RawDocument>>& rawDocs)
{
    if (rawDocs.empty()) {
        return {};
    }
    auto factory = CreateDocumentFactory(schema->GetTableType());
    if (!factory) {
        AUTIL_LOG(ERROR, "create document factory failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    auto parser = CreateDocumentParser(schema);
    if (!parser) {
        AUTIL_LOG(ERROR, "create document parser failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    vector<string> multiFields = GetMultiField(schema);
    docid_t docid = 0;
    auto batch = make_shared<DocumentBatch>();
    for (auto rawDoc : rawDocs) {
        if (!multiFields.empty()) {
            ConvertMultiField(multiFields, rawDoc);
        }
        auto extendDoc = factory->CreateExtendDocument();
        assert(extendDoc);
        extendDoc->SetRawDocument(rawDoc);
        if (!InitExtendDoc(schema, extendDoc.get()).IsOK()) {
            AUTIL_LOG(ERROR, "init extend doc failed");
            return {};
        }
        InitSourceDocument(schema, extendDoc.get());
        auto [s, docBatch] = parser->Parse(*extendDoc);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "parse failed, error: %s, raw doc: %s", s.ToString().c_str(), rawDoc->toString().c_str());
            return {};
        }
        auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
        while (iter->HasNext()) {
            auto doc = dynamic_pointer_cast<NormalDocument>(iter->Next());
            assert(doc);
            doc->SetLocator(rawDoc->getLocator());
            doc->SetDocId(docid++);
            batch->AddDocument(doc);
        }
    }
    return batch;
}

unique_ptr<IDocumentFactory> NormalDocumentMaker::CreateDocumentFactory(const string& tableType)
{
    return make_unique<NormalDocumentFactory>();
}

unique_ptr<NormalDocumentParser>
NormalDocumentMaker::CreateDocumentParser(const shared_ptr<config::ITabletSchema>& schema)
{
    // not resolved
    if (!schema->GetPrimaryKeyIndexConfig()) {
        auto status = framework::TabletSchemaLoader::ResolveSchema(nullptr, "", schema.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "resolve schema failed");
            return nullptr;
        }
    }
    for (const auto& fieldConfig : schema->GetFieldConfigs()) {
        if (fieldConfig->GetFieldType() == ft_text) {
            fieldConfig->SetAnalyzerName(analyzer::SIMPLE_ANALYZER);
        }
    }
    auto tableType = schema->GetTableType();
    auto parser = make_unique<NormalDocumentParser>(nullptr, true);
    if (tableType == "orc") {
        AUTIL_LOG(ERROR, "not support orc table");
        return nullptr;
    }
    auto status = parser->Init(schema, nullptr);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "normal document parser init failed, table[%s] [%s]", schema->GetTableName().c_str(),
                  status.ToString().c_str());
        return nullptr;
    }
    return parser;
}

Status NormalDocumentMaker::InitExtendDoc(const shared_ptr<config::ITabletSchema>& schema, ExtendDocument* extendDoc)
{
    auto normalExtendDoc = dynamic_cast<NormalExtendDocument*>(extendDoc);
    assert(normalExtendDoc);
    ConvertModifyFields(schema, normalExtendDoc);
    auto tokenizeDoc = normalExtendDoc->getTokenizeDocument();
    auto lastTokenizeDoc = normalExtendDoc->getLastTokenizeDocument();

    map<fieldid_t, string> fieldAnalyzerNameMap;
    TokenizeDocumentConvertor convertor;
    RETURN_IF_STATUS_ERROR(convertor.Init(schema, nullptr), "tokenize document convertor init failed");
    auto rawDoc = normalExtendDoc->GetRawDocument();
    RETURN_IF_STATUS_ERROR(convertor.Convert(rawDoc.get(), fieldAnalyzerNameMap, tokenizeDoc, lastTokenizeDoc),
                           "tokenize document convertor convert failed");
    return Status::OK();
}

vector<string> NormalDocumentMaker::GetMultiField(const shared_ptr<config::ITabletSchema>& schema)
{
    vector<string> multiFields;
    for (const auto& field : schema->GetFieldConfigs()) {
        auto fieldType = field->GetFieldType();
        if (fieldType == ft_location || fieldType == ft_line || fieldType == ft_polygon) {
            continue;
        }
        if (field->IsMultiValue() || fieldType == ft_text) {
            multiFields.push_back(field->GetFieldName());
        }
    }
    return multiFields;
}
void NormalDocumentMaker::ConvertMultiField(const vector<string>& multiFields, const shared_ptr<RawDocument>& rawDoc)
{
    for (const auto& fieldName : multiFields) {
        auto value = rawDoc->getField(autil::StringView(fieldName));
        auto iter = value.find(' ');
        if (iter == autil::StringView::npos) {
            continue;
        }
        auto valueStr = value.to_string();
        autil::StringUtil::replace(valueStr, ' ', autil::MULTI_VALUE_DELIMITER);
        rawDoc->setField(fieldName, valueStr);
    }
}

void NormalDocumentMaker::ConvertModifyFields(const shared_ptr<config::ITabletSchema>& schema,
                                              NormalExtendDocument* normalExtendDoc)
{
    const auto& rawDoc = normalExtendDoc->GetRawDocument();
    assert(rawDoc);
    string modifyFields = rawDoc->getField(RESERVED_MODIFY_FIELDS);
    if (modifyFields.empty()) {
        return;
    }
    vector<string> fieldNames;
    autil::StringUtil::fromString(modifyFields, fieldNames, MODIFY_FIELDS_SEP);

    vector<string> fieldValues;
    string modifyValues = rawDoc->getField(RESERVED_MODIFY_VALUES);
    autil::StringUtil::fromString(modifyValues, fieldValues, MODIFY_FIELDS_SEP);

    for (size_t i = 0; i < fieldNames.size(); i++) {
        const string& fieldName = fieldNames[i];
        auto fieldId = schema->GetFieldId(fieldName);
        if (fieldId == INVALID_FIELDID) {
            continue;
        }
        normalExtendDoc->addModifiedField(fieldId);
        if (!fieldValues.empty()) {
            rawDoc->setField(LAST_VALUE_PREFIX + fieldName, fieldValues[i]);
        }
    }
}
void NormalDocumentMaker::SetUDFSourceFields(const std::vector<std::string>& udfFieldNames)
{
    _udfFieldNames = udfFieldNames;
}

void NormalDocumentMaker::InitSourceDocument(const std::shared_ptr<config::ITabletSchema>& schema,
                                             ExtendDocument* extendDoc)
{
    auto extDoc = dynamic_cast<NormalExtendDocument*>(extendDoc);
    assert(extDoc);
    auto sourceConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(
        schema->GetIndexConfig(index::SOURCE_INDEX_TYPE_STR, index::SOURCE_INDEX_NAME));

    if (!sourceConfig) {
        return;
    }

    const auto& classifiedDoc = extDoc->getClassifiedDocument();
    classifiedDoc->setOriginalSnapshot(std::shared_ptr<RawDocument::Snapshot>(extDoc->GetRawDocument()->GetSnapshot()));
    for (const auto& groupConfig : sourceConfig->GetGroupConfigs()) {
        if (groupConfig->GetFieldMode() == indexlib::config::SourceGroupConfig::USER_DEFINE) {
            if (!_udfFieldNames.empty()) {
                extDoc->GetRawDocument()->AddTag("udf_source_fields", autil::StringUtil::toString(_udfFieldNames, ";"));
            }
        }
    }
}

} // namespace indexlibv2::document
