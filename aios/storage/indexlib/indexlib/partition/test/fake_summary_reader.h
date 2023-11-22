#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class FakeSummaryReader : public SummaryReader
{
public:
    const static versionid_t BAD_VERSION_OF_SUMMARY = -10;

public:
    FakeSummaryReader(const config::SummarySchemaPtr& summarySchema) : SummaryReader(summarySchema) {}
    FakeSummaryReader(const FakeSummaryReader& reader) : SummaryReader(reader) {}
    ~FakeSummaryReader() {}

    DECLARE_SUMMARY_READER_IDENTIFIER(fake);

public:
    bool ReOpen(const SegmentDirectoryPtr& segDir, const InvertedIndexReaderPtr& pkReader)
    {
        if (segDir->GetVersion().GetVersionId() == BAD_VERSION_OF_SUMMARY) {
            return false;
        }
        return true;
    }

    document::SummaryDocumentPtr GetDocument(docid_t docId) const { return document::SummaryDocumentPtr(); }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const { return true; }

    bool GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc) const
    {
        return true;
    }

    std::string GetField(docid_t docId, fieldid_t fid) const { return ""; }

    SummaryReader* Clone() const { return new FakeSummaryReader(*this); }

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) {}

    AttributeReaderPtr GetAttrReader(fieldid_t fid) const { return AttributeReaderPtr(); }
};

DEFINE_SHARED_PTR(FakeSummaryReader);
}} // namespace indexlib::partition
