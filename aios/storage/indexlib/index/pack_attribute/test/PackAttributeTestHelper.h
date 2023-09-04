#pragma once

#include "autil/Log.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/test/IndexTestHelper.h"

namespace indexlibv2::document {
class RawDocument;
class NormalDocument;
class IDocumentBatch;
class IIndexFields;
class PackAttributeAppender;
} // namespace indexlibv2::document
namespace indexlibv2::document::extractor {
class IDocumentInfoExtractorFactory;
}

namespace indexlibv2::index {
class PackAttributeMemIndexer;
class PackAttributeReader;

class PackAttributeTestHelper : public indexlib::index::IndexTestHelper
{
public:
    PackAttributeTestHelper();
    ~PackAttributeTestHelper();

public:
    // FORMAT: packName1:fieldName1,fieldName2,...:compressStr
    StatusOr<std::shared_ptr<PackAttributeConfig>> MakePackAttributeConfig(const std::string& packAttributeDesc,
                                                                           size_t indexIdx);
    const std::shared_ptr<PackAttributeConfig>& GetPackAttributeConfig() const { return _packAttributeConfig; }
    const std::shared_ptr<PackAttributeReader>& GetPackAttributeReader() const { return _packAttrReader; }
    // FORMAT: subAttrName1:value1,subAttrName2:value2...
    bool Query(docid_t docid, const std::string& expectValueStr);

protected:
    void DoInit() override;
    Status DoOpen() override;
    StatusOr<std::shared_ptr<config::IIndexConfig>> DoMakeIndexConfig(const std::string& indexDesc,
                                                                      size_t indexIdx) override;
    std::shared_ptr<document::IDocumentBatch> MakeDocumentBatch(const std::string& docs) override;
    StatusOr<std::shared_ptr<IMemIndexer>> CreateMemIndexer() override;
    StatusOr<std::shared_ptr<IIndexMerger>> CreateIndexMerger() override;
    StatusOr<std::shared_ptr<framework::IndexTaskResourceManager>>
    CreateIndexTaskResourceManager(const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                                   const std::string& docMapStr) override;
    // queryStr:    "DOCID"
    // expectValue: "field1=value1,field2=value2"
    bool DoQuery(const std::string& queryStr, const std::string& expectValueStr) override;

private:
    std::shared_ptr<document::IIndexFields> MakePackAttributeIndexFields(const std::string& docs);
    std::shared_ptr<document::IDocumentBatch> MakeNormalDocumentBatch(const std::string& docs);
    void CompleteRawDocument(const std::shared_ptr<document::RawDocument>& rawDoc);
    void ConvertAttributeField(const std::shared_ptr<document::RawDocument>& rawDoc,
                               const std::shared_ptr<document::NormalDocument>& normalDoc);
    bool Query(docid_t docid, const std::string& subAttrName, const std::string& expectValue);

private:
    std::shared_ptr<PackAttributeConfig> _packAttributeConfig;
    std::unique_ptr<document::PackAttributeAppender> _packAttrAppender;
    std::unique_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::shared_ptr<PackAttributeReader> _packAttrReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
