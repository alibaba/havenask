#include "indexlib/index/summary/test/SummaryMaker.h"

#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryMaker);

std::shared_ptr<indexlib::document::SummaryDocument>
SummaryMaker::MakeOneDocument(docid_t docId, const std::shared_ptr<config::ITabletSchema>& schema,
                              autil::mem_pool::Pool* pool)
{
    std::shared_ptr<indexlib::document::SummaryDocument> document(new indexlib::document::SummaryDocument());
    document->SetDocId(docId);

    const auto& fieldConfigs = schema->GetFieldConfigs();
    auto summaryConf = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME));
    assert(summaryConf);
    for (uint32_t i = 0; i < fieldConfigs.size(); ++i) {
        const auto& fieldConfig = fieldConfigs[i];
        std::stringstream ss;
        ss << "content_" << docId << "_" << i;

        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (summaryConf->IsInSummary((fieldid_t)i)) {
            document->SetField(fieldId, autil::MakeCString(ss.str(), pool));
        }
    }
    return document;
}

std::shared_ptr<SummaryMemIndexer>
SummaryMaker::BuildOneSegmentWithoutDump(uint32_t docCount, const std::shared_ptr<config::ITabletSchema>& schema,
                                         autil::mem_pool::Pool* pool, DocumentArray& answerDocArray)
{
    auto memIndexer = std::make_shared<SummaryMemIndexer>();
    auto docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
    auto config = schema->GetIndexConfig(SUMMARY_INDEX_TYPE_STR, SUMMARY_INDEX_NAME);
    assert(config);
    auto summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
    assert(summaryIndexConfig);
    auto status = memIndexer->Init(summaryIndexConfig, docInfoExtractorFactory.get());
    if (!status.IsOK()) {
        return nullptr;
    }
    for (size_t j = 0; j < docCount; ++j) {
        auto doc = MakeOneDocument(j, schema, pool);
        std::shared_ptr<indexlib::document::SerializedSummaryDocument> serDoc;
        indexlibv2::document::SummaryFormatter formatter(summaryIndexConfig);
        auto status = formatter.SerializeSummaryDoc(doc, serDoc);
        assert(status.IsOK());
        auto normalDoc = std::make_shared<indexlibv2::document::NormalDocument>();
        normalDoc->SetSummaryDocument(serDoc);
        if (!memIndexer->AddDocument(normalDoc.get()).IsOK()) {
            return nullptr;
        }
        doc->SetDocId(answerDocArray.size());
        answerDocArray.push_back(doc);
    }

    return memIndexer;
}

Status SummaryMaker::BuildOneSegment(const std::shared_ptr<indexlib::file_system::IDirectory>& segDirectory,
                                     segmentid_t segId, std::shared_ptr<config::ITabletSchema>& schema,
                                     uint32_t docCount, autil::mem_pool::Pool* pool, DocumentArray& answerDocArray)
{
    std::shared_ptr<SummaryMemIndexer> memIndexer = BuildOneSegmentWithoutDump(docCount, schema, pool, answerDocArray);

    auto [status, summaryDirectory] =
        segDirectory->MakeDirectory(SUMMARY_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();
    assert(status.IsOK());
    if (status.IsOK()) {
        indexlib::util::SimplePool simplePool;
        return memIndexer->Dump(&simplePool, indexlib::file_system::IDirectory::ToLegacyDirectory(summaryDirectory),
                                nullptr);
    }
    return status;
}

IIndexFactory* SummaryMaker::GetIndexFactory(const std::shared_ptr<indexlibv2::config::IIndexConfig>& config)
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto [st, indexFactory] = indexFactoryCreator->Create(config->GetIndexType());
    if (!st.IsOK()) {
        return nullptr;
    }
    return indexFactory;
}

std::shared_ptr<IIndexReader> SummaryMaker::CreateIndexReader(std::shared_ptr<framework::TabletData>& tabletData,
                                                              const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                              const IndexReaderParameter& indexReaderParam)
{
    auto indexFactory = GetIndexFactory(indexConfig);
    auto indexReader = indexFactory->CreateIndexReader(indexConfig, indexReaderParam);
    auto status = indexReader->Open(indexConfig, tabletData.get());
    if (!status.IsOK()) {
        return nullptr;
    }
    std::shared_ptr<index::IIndexReader> reader = std::move(indexReader);
    return reader;
}

} // namespace indexlibv2::index
