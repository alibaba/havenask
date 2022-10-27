#ifndef ISEARCH_INDEX_FAKESUMMARYREADER_H
#define ISEARCH_INDEX_FAKESUMMARYREADER_H

#include <ha3/index/index.h>
#include <vector>
#include <indexlib/common_define.h>
#include <indexlib/index/normal/summary/summary_reader.h>
#include <indexlib/document/index_document/normal_document/search_summary_document.h>
#include <ha3/isearch.h>
#include <autil/legacy/exception.h>

IE_NAMESPACE_BEGIN(index);

class FakeSummaryReader : public SummaryReader
{
public:
    FakeSummaryReader()
        : SummaryReader(config::SummarySchemaPtr()) {
    }

    FakeSummaryReader(const config::SummarySchemaPtr& summarySchema)
        : SummaryReader(summarySchema) {
    }

    ~FakeSummaryReader() {
        for (std::vector<document::SearchSummaryDocument*>::iterator it = _summaryDocuments.begin();
             it != _summaryDocuments.end(); ++it)
        {
            delete *it;
        }
    }

    void AddSummaryDocument(document::SearchSummaryDocument* doc) {
        _summaryDocuments.push_back(doc);
    }

    virtual uint32_t GetDocCount() const 
    {
        assert(false);
        return 0;
    }
public:
    virtual bool ReOpen(const index_base::SegmentDirectoryPtr& segDir,
                        const IndexReaderPtr &pkIndexReader) 
    { assert(false); return false; }

    virtual bool WarmUpDocument(docid_t docId) { assert(false); return false; }

    virtual bool IsInCache(docid_t docId) const { assert(false); return false; }

    /*override*/
    virtual document::SummaryDocumentPtr GetDocument(docid_t docId) const {
        return document::SummaryDocumentPtr();
    }

    virtual bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const {
        if (docId < (docid_t)_summaryDocuments.size()) {
            for (size_t i = 0; i < _summaryDocuments[docId]->GetFieldCount(); ++i) {
                const autil::ConstString *str = _summaryDocuments[docId]->GetFieldValue(i);
                if (str) {
                    summaryDoc->SetFieldValue(i, str->data(), str->size());
                }
            }
            return true;
        }

        return false;
    }

    virtual bool GetDocument(docid_t docId,
                             document::SearchSummaryDocument *summaryDoc,
                             const SummaryGroupIdVec& groupVec) const {
        if (docId < (docid_t)_summaryDocuments.size()) {
            for (size_t i = 0; i < groupVec.size(); ++i) {
                const config::SummaryGroupConfigPtr& summaryGroupConfig =
                    mSummarySchema->GetSummaryGroupConfig(groupVec[i]);
                summaryfieldid_t fieldId = summaryGroupConfig->GetSummaryFieldIdBase();
                for (size_t j = 0; j < summaryGroupConfig->GetSummaryFieldsCount()  ; j++) {
                    const autil::ConstString *str = _summaryDocuments[docId]->GetFieldValue(fieldId);
                    if (str) {
                        summaryDoc->SetFieldValue(fieldId, str->data(), str->size());
                    }
                    fieldId++;
                }
            }
            return true;
        }
        return false;
    }

    bool GetDocumentByPkStr(const std::string &pkStr, document::SearchSummaryDocument *summaryDoc) const {
        return true;
    }

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) {}

    AttributeReaderPtr GetAttrReader(fieldid_t fid) const { return AttributeReaderPtr(); }

    virtual std::string GetField(docid_t docId, fieldid_t fid) const { assert(false);return ""; }

    virtual SummaryReader* Clone() const {assert(false);return NULL;}

    virtual std::string GetIdentifier() const {assert(false);return "";}
    virtual void WarmUp() { assert(false); }

private:
    std::vector<document::SearchSummaryDocument*> _summaryDocuments;
};

HA3_TYPEDEF_PTR(FakeSummaryReader);

class ExceptionSummaryReader : public FakeSummaryReader
{
public:
    /*override*/
    virtual document::SummaryDocumentPtr GetDocument(docid_t docId) const {
        AUTIL_LEGACY_THROW(misc::FileIOException, "fake exception");
    }

    virtual bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const {
        AUTIL_LEGACY_THROW(misc::FileIOException, "fake exception");
    }
};

HA3_TYPEDEF_PTR(ExceptionSummaryReader);

IE_NAMESPACE_END(index);
#endif
