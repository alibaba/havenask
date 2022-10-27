#ifndef __INDEXLIB_LOCAL_DISK_SUMMARY_READER_H
#define __INDEXLIB_LOCAL_DISK_SUMMARY_READER_H

#include <string>
#include <tr1/memory>
#include "fslib/fs/FileSystem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"
#include "indexlib/index/normal/summary/building_summary_reader.h"
#include "indexlib/index_base/segment/segment_data.h"

IE_NAMESPACE_BEGIN(index);

class LocalDiskSummaryReader : public SummaryReader
{
public:
    typedef std::vector<std::pair<fieldid_t, AttributeReaderPtr> > AttributeReaders;
    typedef std::vector<std::pair<fieldid_t, PackAttributeReaderPtr> > PackAttributeReaders;

public:
    LocalDiskSummaryReader(const config::SummarySchemaPtr& summarySchema,
                           summarygroupid_t summaryGroupId);
    ~LocalDiskSummaryReader();

    DECLARE_SUMMARY_READER_IDENTIFIER(local_disk);

public:
    bool Open(const index_base::PartitionDataPtr& partitionData,
              const PrimaryKeyIndexReaderPtr& pkIndexReader) override;

    bool GetDocument(docid_t docId,
                     document::SearchSummaryDocument *summaryDoc) const override;

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) override;
    void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader) override;
    
private:
    bool LoadSegment(const std::string& segPath);
    void LoadSegmentInfo(const std::string& segPath, index_base::SegmentInfo& segInfo);
    bool GetDocumentFromSummary(
            docid_t docId, document::SearchSummaryDocument *summaryDoc) const;
    bool GetDocumentFromAttributes(
            docid_t docId, document::SearchSummaryDocument *summaryDoc) const;
    bool LoadSegmentReader(const index_base::SegmentData& segmentData);

    void InitBuildingSummaryReader(const index_base::SegmentIteratorPtr& segIter);

    bool SetSummaryDocField(document::SearchSummaryDocument *summaryDoc,
                            fieldid_t fieldId, const std::string& value) const;

private:
    AttributeReaders mAttrReaders;
    PackAttributeReaders mPackAttrReaders;
    std::vector<LocalDiskSummarySegmentReaderPtr> mSegmentReaders;
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    index_base::SegmentInfos mSegmentInfos;

    BuildingSummaryReaderPtr mBuildingSummaryReader;
    docid_t mBuildingBaseDocId;

private:
    friend class LocalDiskSummaryReaderTest;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(LocalDiskSummaryReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCAL_DISK_SUMMARY_READER_H
