#ifndef __INDEXLIB_FAKE_SUMMARY_READER_H
#define __INDEXLIB_FAKE_SUMMARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeSummaryReader : public index::SummaryReader
{
private:
    using index::SummaryReader::Open;
    using index::SummaryReader::GetDocument;
public:
    FakeSummaryReader()
        : SummaryReader(config::SummarySchemaPtr())
    {
    }
    ~FakeSummaryReader() {
        for (std::vector<document::SearchSummaryDocument*>::iterator it = mSummaryDocuments.begin();
             it != mSummaryDocuments.end(); ++it)
        {
            delete *it;
        }
    }
public:
    void AddSummaryDocument(document::SearchSummaryDocument* doc) {
        mSummaryDocuments.push_back(doc);
    }

    document::SummaryDocumentPtr GetDocument(docid_t docId) const 
    { return document::SummaryDocumentPtr(); }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const override
    {
        if (docId < (docid_t)mSummaryDocuments.size()) {
            for (size_t i = 0; i < mSummaryDocuments[docId]->GetFieldCount(); ++i) {
                const autil::ConstString *str = mSummaryDocuments[docId]->GetFieldValue(i);
                if (str) {
                    summaryDoc->SetFieldValue(i, str->data(), str->size());
                }
            }
            return true;
        }

        return false;
    }

    bool GetDocumentByPkStr(const std::string &pkStr, document::SearchSummaryDocument *summaryDoc) const {
        return true;
    }

    void AddAttrReader(fieldid_t fieldId, const index::AttributeReaderPtr& attrReader) override{}

    index::AttributeReaderPtr GetAttrReader(fieldid_t fid) const
    { return index::AttributeReaderPtr(); }

    std::string GetIdentifier() const override {assert(false);return "";}

private:
    std::vector<document::SearchSummaryDocument*> mSummaryDocuments;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeSummaryReader);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_SUMMARY_READER_H
