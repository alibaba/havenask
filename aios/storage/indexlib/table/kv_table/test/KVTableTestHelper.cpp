#include "indexlib/table/kv_table/test/KVTableTestHelper.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/kv_table/KVSchemaResolver.h"
#include "indexlib/table/kv_table/test/KVTableTestSearcher.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/ResultChecker.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableTestHelper);

std::shared_ptr<TableTestHelper> KVTableTestHelper::CreateMergeHelper()
{
    std::shared_ptr<TableTestHelper> ptr(new KVTableTestHelper);
    return ptr;
}

bool KVTableTestHelper::DoQuery(const std::shared_ptr<framework::ITabletReader>& reader, std::string indexType,
                                std::string indexName, std::string queryStr, std::string expectValue)
{
    auto expectResult = DocumentParser::ParseResult(expectValue);
    auto kvReader =
        std::dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(reader->GetIndexReader(indexType, indexName));
    assert(kvReader != nullptr);
    const auto& pkConfig = reader->GetSchema()->GetPrimaryKeyIndexConfig();
    assert(pkConfig);
    auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(pkConfig);
    KVTableTestSearcher searcher;
    searcher.Init(kvReader, kvConfig);
    auto result = searcher.Search(queryStr, 0, indexlib::tsc_no_cache);
    return ResultChecker::Check(result, /*"expectedError"*/ false, expectResult);
}

bool KVTableTestHelper::DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                                std::string expectValue)
{
    auto reader = GetTabletReader();
    assert(_schema->GetTableType() == "kv");
    return DoQuery(reader, indexType, indexName, queryStr, expectValue);
}

Status KVTableTestHelper::DoBuild(std::string docs, bool oneBatch)
{
    assert(_schema->GetTableType() == "kv");

    auto docBatch = document::KVDocumentBatchMaker::Make(_schema, docs);
    if (!docBatch) {
        return Status::InternalError("make document batch from %s failed", docs.c_str());
    }
    if (oneBatch) {
        auto status = GetITablet()->Build(docBatch);
        if (status.IsUninitialize()) {
            status = GetITablet()->Build(docBatch);
        }
        RETURN_IF_STATUS_ERROR(status, "build docs failed");
    } else {
        auto kvBatch = dynamic_cast<document::KVDocumentBatch*>(docBatch.get());
        if (!kvBatch) {
            return Status::InternalError("not an KVDocumentBatch");
        }
        auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(kvBatch);
        while (iter->HasNext()) {
            auto singleDocBatch = std::make_shared<document::KVDocumentBatch>();
            singleDocBatch->AddDocument(iter->Next());
            auto status = GetITablet()->Build(singleDocBatch);
            if (status.IsUninitialize()) {
                status = GetITablet()->Build(singleDocBatch);
            }
            RETURN_IF_STATUS_ERROR(status, "build doc [%ld] failed", iter->GetDocIdx());
        }
    }
    return Status::OK();
}

Status KVTableTestHelper::DoBuild(const std::string& docStr, const framework::Locator& locator)
{
    assert(_schema->GetTableType() == "kv");
    auto docBatch = document::KVDocumentBatchMaker::Make(_schema, docStr);
    if (!docBatch) {
        return Status::InternalError("make document batch from %s failed", docStr.c_str());
    }
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
    while (iter->HasNext()) {
        iter->Next()->SetLocator(locator);
    }
    auto status = GetITablet()->Build(docBatch);
    if (status.IsUninitialize()) {
        status = GetITablet()->Build(docBatch);
    }
    return status;
}

std::shared_ptr<config::ITabletSchema> KVTableTestHelper::MakeSchema(const std::string& fieldNames,
                                                                     const std::string& keyName,
                                                                     const std::string& valueNames, int64_t ttl,
                                                                     const std::string& valueFormat)
{
    return table::KVTabletSchemaMaker::Make(fieldNames, keyName, valueNames, ttl, valueFormat);
}

std::shared_ptr<indexlibv2::config::ITabletSchema> KVTableTestHelper::LoadSchema(const std::string& dir,
                                                                                 const std::string& schemaFileName)
{
    return framework::TabletSchemaLoader::LoadSchema(dir, schemaFileName);
}

} // namespace indexlibv2::table
