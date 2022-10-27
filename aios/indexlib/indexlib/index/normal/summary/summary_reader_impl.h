#ifndef __INDEXLIB_SUMMARY_READER_IMPL_H
#define __INDEXLIB_SUMMARY_READER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_reader.h"

DECLARE_REFERENCE_CLASS(index, LocalDiskSummaryReader);
        
IE_NAMESPACE_BEGIN(index);

// Group manager
class SummaryReaderImpl final : public SummaryReader
{
public:
    SummaryReaderImpl(const config::SummarySchemaPtr& summarySchema);
    ~SummaryReaderImpl();
    DECLARE_SUMMARY_READER_IDENTIFIER(group);
public:
    bool Open(const index_base::PartitionDataPtr& partitionData,
              const PrimaryKeyIndexReaderPtr& pkIndexReader) override final;

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) override final;
    void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader) override final;    

    bool GetDocument(docid_t docId,
                     document::SearchSummaryDocument *summaryDoc) const override final
    { return GetDocument(docId, summaryDoc, mAllGroupIds); }

    bool GetDocument(docid_t docId,
                     document::SearchSummaryDocument *summaryDoc,
                     const SummaryGroupIdVec& groupVec) const override final;

private:
    bool DoGetDocument(docid_t docId,
                       document::SearchSummaryDocument *summaryDoc,
                       const SummaryGroupIdVec& groupVec) const;

private:
    typedef std::vector<LocalDiskSummaryReaderPtr> SummaryGroupVec;
    SummaryGroupVec mSummaryGroups;
    SummaryGroupIdVec mAllGroupIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryReaderImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_READER_IMPL_H
