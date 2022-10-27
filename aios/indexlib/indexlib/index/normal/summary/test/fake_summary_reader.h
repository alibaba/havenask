#ifndef __INDEXLIB_FAKE_SUMMARY_READER_H
#define __INDEXLIB_FAKE_SUMMARY_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/summary/summary_reader.h"
#include <set>

IE_NAMESPACE_BEGIN(index);

class FakeSummaryReader : public SummaryReader
{
public:
    FakeSummaryReader() : mDocCount(0) {}
    FakeSummaryReader(uint32_t docCount) : mDocCount(docCount) {}

public:
    void Open(const storage::StoragePtr& partition, 
                      const SegmentInfos& segmentInfos) {}
    bool WarmUpDocument(docid_t docId) 
    {
        if (docId < (docid_t)mDocCount)
        {
            mWarmedDocIds.insert(docId);
            return true;
        }
        return false;
    }

    bool IsInCache(docid_t docId) const 
    {
        return (mWarmedDocIds.find(docId) != mWarmedDocIds.end());
    }
    document::SummaryDocumentPtr GetDocument(docid_t docId) const
    { return document::SummaryDocumentPtr(); }

    std::string GetField(docid_t docId, fieldid_t fid) const { return "";}

    // SummaryReader* Clone() const { return NULL; }

    std::string GetIdentifier() const {return "fake";}

    void ReOpen(const SegmentInfos& segmentInfos) {}

    storage::StoragePtr GetStorage() const
    {
        return storage::StoragePtr(); 
    }

    uint32_t GetDocCount() const { return mDocCount; }

    void SetDocCount(uint32_t docCount) { mDocCount = docCount; }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const {}


    bool GetDocumentByPkStr(const std::string &pkStr, document::SearchSummaryDocument *summaryDoc) const {}

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) {}

    AttributeReaderPtr GetAttrReader(fieldid_t fid) const {}

private:
    typedef std::set<docid_t> DocIdSet;
    DocIdSet mWarmedDocIds;
    uint32_t mDocCount;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_SUMMARY_READER_H
