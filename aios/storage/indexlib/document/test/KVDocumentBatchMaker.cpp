#include "indexlib/document/test/KVDocumentBatchMaker.h"

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/kkv/KKVDocumentFactory.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/kv/KVDocumentFactory.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"

namespace indexlibv2::document {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.document, DocumentBatchMaker);

std::shared_ptr<IDocumentBatch> KVDocumentBatchMaker::Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                           const std::string& docBatchStr)
{
    auto rawDocs = RawDocumentMaker::MakeBatch(docBatchStr);
    return Make(schema, rawDocs);
}

std::shared_ptr<IDocumentBatch> KVDocumentBatchMaker::Make(const std::shared_ptr<config::ITabletSchema>& schema,
                                                           const std::vector<std::shared_ptr<RawDocument>>& rawDocs)
{
    auto batchVec = MakeBatchVec(schema, rawDocs);
    auto batch = std::make_shared<KVDocumentBatch>();
    if (batchVec.empty()) {
        return batch;
    }
    for (const auto& docBatch : batchVec) {
        auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
        while (iter->HasNext()) {
            auto doc = std::dynamic_pointer_cast<KVDocument>(iter->Next());
            batch->AddDocument(doc->Clone(batch->GetPool()));
        }
    }
    return batch;
}

std::vector<std::shared_ptr<IDocumentBatch>>
KVDocumentBatchMaker::MakeBatchVec(const std::shared_ptr<config::ITabletSchema>& schema, const std::string& docBatchStr)
{
    auto rawDocs = RawDocumentMaker::MakeBatch(docBatchStr);
    return MakeBatchVec(schema, rawDocs);
}

std::vector<std::shared_ptr<IDocumentBatch>>
KVDocumentBatchMaker::MakeBatchVec(const std::shared_ptr<config::ITabletSchema>& schema,
                                   const std::vector<std::shared_ptr<RawDocument>>& rawDocs)
{
    if (rawDocs.empty()) {
        return {};
    }
    auto factory = CreateDocumentFactory(schema->GetTableType());
    if (!factory) {
        AUTIL_LOG(ERROR, "create document factory failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    auto parser = factory->CreateDocumentParser(schema, nullptr);
    if (!parser) {
        AUTIL_LOG(ERROR, "create document parser failed for table type %s", schema->GetTableType().c_str());
        return {};
    }
    auto multiFields = GetMultiField(schema);
    std::vector<std::shared_ptr<IDocumentBatch>> batchVec;
    for (const auto& rawDoc : rawDocs) {
        if (!multiFields.empty()) {
            ConvertMultiField(multiFields, rawDoc);
        }
        auto extendDoc = factory->CreateExtendDocument();
        extendDoc->SetRawDocument(rawDoc);
        auto [s, docBatch] = parser->Parse(*extendDoc);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "parse failed, error: %s, raw doc: %s", s.ToString().c_str(), rawDoc->toString().c_str());
            return {};
        }
        auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
        while (iter->HasNext()) {
            auto doc = std::dynamic_pointer_cast<KVDocument>(iter->Next());
            assert(doc);
            doc->SetLocator(rawDoc->getLocator());
        }
        batchVec.emplace_back(std::move(docBatch));
    }
    return batchVec;
}

std::unique_ptr<IDocumentFactory> KVDocumentBatchMaker::CreateDocumentFactory(const std::string& tableType)
{
    if (tableType == "kv") {
        return std::make_unique<KVDocumentFactory>();
    } else if (tableType == "kkv") {
        return std::make_unique<KKVDocumentFactory>();
    } else {
        AUTIL_LOG(ERROR, "unsupported table type: %s", tableType.c_str());
        return nullptr;
    }
}

std::vector<std::string> KVDocumentBatchMaker::GetMultiField(const std::shared_ptr<config::ITabletSchema>& schema)
{
    std::vector<std::string> multiFields;
    for (const auto& field : schema->GetFieldConfigs()) {
        if (field->IsMultiValue()) {
            multiFields.push_back(field->GetFieldName());
        }
    }
    return multiFields;
}
void KVDocumentBatchMaker::ConvertMultiField(const std::vector<std::string>& multiFields,
                                             const std::shared_ptr<RawDocument>& rawDoc)
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

} // namespace indexlibv2::document
