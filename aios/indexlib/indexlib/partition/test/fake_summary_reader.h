#ifndef __INDEXLIB_FAKE_SUMMARY_READER_H
#define __INDEXLIB_FAKE_SUMMARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"

IE_NAMESPACE_BEGIN(partition);

class FakeSummaryReader : public SummaryReader
{
public:
    const static versionid_t BAD_VERSION_OF_SUMMARY = -10;

public:
    FakeSummaryReader(const config::SummarySchemaPtr& summarySchema) 
        : SummaryReader(summarySchema)
    {
    }
    FakeSummaryReader(const FakeSummaryReader& reader) 
        : SummaryReader(reader)
    {
    }
    ~FakeSummaryReader() {}

    DECLARE_SUMMARY_READER_IDENTIFIER(fake);
public:
    bool ReOpen(const SegmentDirectoryPtr& segDir,
                const IndexReaderPtr& pkReader) 
    {
        if (segDir->GetVersion().GetVersionId() == BAD_VERSION_OF_SUMMARY)
        {
            return false;
        }
        return true;
    }

    document::SummaryDocumentPtr GetDocument(docid_t docId) const
    {
        return document::SummaryDocumentPtr();
    }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const {
        return true;
    }

    bool GetDocumentByPkStr(const std::string &pkStr, document::SearchSummaryDocument *summaryDoc) const 
    {
        return true;
    }

    std::string GetField(docid_t docId, fieldid_t fid) const
    {
        return "";
    }

    SummaryReader* Clone() const
    {
        return new FakeSummaryReader(*this);
    }    
    
    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) {}

    AttributeReaderPtr GetAttrReader(fieldid_t fid) const { return AttributeReaderPtr(); }

};


DEFINE_SHARED_PTR(FakeSummaryReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_SUMMARY_READER_H
