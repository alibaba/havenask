#ifndef __INDEXLIB_FAKE_SUMMARY_READER_H
#define __INDEXLIB_FAKE_SUMMARY_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace testlib {

class FakeSummaryReader : public index::SummaryReader
{
private:
    using index::SummaryReader::GetDocument;
    using index::SummaryReader::Open;

public:
    FakeSummaryReader() : SummaryReader(config::SummarySchemaPtr()) {}
    ~FakeSummaryReader()
    {
        for (std::vector<document::SearchSummaryDocument*>::iterator it = mSummaryDocuments.begin();
             it != mSummaryDocuments.end(); ++it) {
            delete *it;
        }
    }

public:
    void AddSummaryDocument(document::SearchSummaryDocument* doc) { mSummaryDocuments.push_back(doc); }

    std::shared_ptr<document::SummaryDocument> GetDocument(docid_t docId) const { return nullptr; }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const override
    {
        if (docId < (docid_t)mSummaryDocuments.size()) {
            for (size_t i = 0; i < mSummaryDocuments[docId]->GetFieldCount(); ++i) {
                const autil::StringView* str = mSummaryDocuments[docId]->GetFieldValue(i);
                if (str) {
                    summaryDoc->SetFieldValue(i, str->data(), str->size());
                }
            }
            return true;
        }

        return false;
    }

    bool GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc) const override
    {
        return true;
    }

    void AddAttrReader(fieldid_t fieldId, const index::AttributeReaderPtr& attrReader) override {}

    index::AttributeReaderPtr GetAttrReader(fieldid_t fid) const { return index::AttributeReaderPtr(); }

    std::string GetIdentifier() const override
    {
        assert(false);
        return "";
    }

private:
    std::vector<document::SearchSummaryDocument*> mSummaryDocuments;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeSummaryReader);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_SUMMARY_READER_H
